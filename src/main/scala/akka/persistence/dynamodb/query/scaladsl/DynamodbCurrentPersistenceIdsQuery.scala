package akka.persistence.dynamodb.query.scaladsl

import akka.NotUsed
import akka.persistence.dynamodb.query.ReadJournalSettingsProvider
import akka.persistence.dynamodb.query.scaladsl.CreatePersistenceIdsIndex.createPersistenceIdsIndexRequest
import akka.persistence.dynamodb.query.scaladsl.DynamodbCurrentPersistenceIdsQuery.{RichNumber, RichOption, SourceLazyOps}
import akka.persistence.dynamodb.query.scaladsl.PersistenceIdsResult.RichPersistenceIdsResult
import akka.persistence.dynamodb.{ActorSystemProvider, DynamoProvider, LoggingProvider}
import akka.persistence.query.scaladsl.CurrentPersistenceIdsQuery
import akka.stream.scaladsl.Source
import akka.util.ccompat.JavaConverters._
import com.amazonaws.services.dynamodbv2.model._

import java.util
import scala.concurrent.Future
import scala.util.control.NonFatal

trait DynamodbCurrentPersistenceIdsQuery extends CurrentPersistenceIdsQuery { self: ReadJournalSettingsProvider with DynamoProvider with ActorSystemProvider with LoggingProvider =>

  /**
   * Same type of query as [[akka.persistence.query.scaladsl.PersistenceIdsQuery.persistenceIds()]] but the stream
   * is completed immediately when it reaches the end of the "result set". Persistent
   * actors that are created after the query is completed are not included in the stream.
   *
   * A dynamodb <code>scan</code> will be performed. Results will be paged per 1 MB size.
   */
  override def currentPersistenceIds(): Source[String, NotUsed] = {
    log.debug("starting currentPersistenceIds")
    currentPersistenceIdsScanInternal()
      .mapConcat(seq => seq.toList)
      .log("currentPersistenceIds")
  }

  /**
   * The implementation that is used for [[currentPersistenceIds]]
   * Here the results are offered page by page.
   * A dynamodb <code>scan</code> will be performed. Results will be paged per 1 MB size.
   */
  def currentPersistenceIdsByPageScan(): Source[Seq[String], NotUsed] = {
    log.debug("starting currentPersistenceIdsByPageScan")
    currentPersistenceIdsScanInternal()
      .log("currentPersistenceIdsByPageScan")
  }

  /**
   * Persistence ids are returned page by page.
   * A dynamodb <code>query</code> will be performed against a Global Secondary Index 'persistence-ids-idx'.
   * See [[CreatePersistenceIdsIndex.createPersistenceIdsIndex]]
   */
  def currentPersistenceIdsByPageQuery(): Source[Seq[String], NotUsed] = {
    log.debug("starting currentPersistenceIdsByPageQuery")
    currentPersistenceIdsQueryInternal()
      .log("currentPersistenceIdsByPageQuery")
  }

  private def currentPersistenceIdsScanInternal(): Source[Seq[String], NotUsed] = {
    import PersistenceIdsResult.persistenceIdsScanResult
    currentPersistenceIdsByPageInternal(scanPersistenceIds)
  }

  private def currentPersistenceIdsQueryInternal(): Source[Seq[String], NotUsed] = {
    import PersistenceIdsResult.persistenceIdsQueryResult
    currentPersistenceIdsByPageInternal(queryPersistenceIds)
  }

  private def currentPersistenceIdsByPageInternal[Result: PersistenceIdsResult](getPersistenceIds: Option[java.util.Map[String, AttributeValue]] => Future[Result]): Source[Seq[String], NotUsed] = {
    import system.dispatcher
    type ResultSource = Source[Option[Result], NotUsed]

    def nextCall(maybePreviousResult: Option[Result]): Future[Option[Result]] = {
      val maybeNextResult = for {
        previousResult <- maybePreviousResult
        nextEvaluatedKey <- previousResult.nextEvaluatedKey
      } yield getPersistenceIds(Some(nextEvaluatedKey)).map(Some(_))

      maybeNextResult.getOrElse(Future.successful(None))
    }

    def lazyStream(currentResult: ResultSource): ResultSource = {
      def nextResult: ResultSource = currentResult.mapAsync(parallelism = 1)(nextCall)

      currentResult.concatLazy(lazyStream(nextResult))
    }

    val infiniteStreamOfResults: ResultSource =
      lazyStream(Source.fromFuture(getPersistenceIds(None).map(Some(_))))

    infiniteStreamOfResults
      .takeWhile(_.isDefined)
      .flatMapConcat(_.toSource)
      .map(scanResult =>
        scanResult.toPersistenceIdsPage
          .map ( rawPersistenceId => parsePersistenceId(rawPersistenceId = rawPersistenceId, journalName = readJournalSettings.JournalName) )
      )
  }

  private def queryPersistenceIds(exclusiveStartKey: Option[java.util.Map[String, AttributeValue]]) = {
    def queryRequest(exclusiveStartKey: Option[java.util.Map[String, AttributeValue]]): QueryRequest = {
      val req = new QueryRequest()
        .withTableName(readJournalSettings.Table)
        .withIndexName("persistence-ids-idx")
        .withProjectionExpression("par")
        .withKeyConditionExpression("num = :n")
        .withExpressionAttributeValues(Map(":n" -> 1.toAttribute).asJava)
      exclusiveStartKey.foreach(esk => req.withExclusiveStartKey(esk))
      req
    }
    dynamo.query(queryRequest(exclusiveStartKey))
  }

  private def scanPersistenceIds(exclusiveStartKey: Option[java.util.Map[String, AttributeValue]]) = {
    def scanRequest(exclusiveStartKey: Option[java.util.Map[String, AttributeValue]]): ScanRequest = {
      val req = new ScanRequest()
        .withTableName(readJournalSettings.Table)
        .withProjectionExpression("par")
        .withFilterExpression("num = :n")
        .withExpressionAttributeValues(Map(":n" -> 1.toAttribute).asJava)
      exclusiveStartKey.foreach(esk => req.withExclusiveStartKey(esk))
      req
    }
    dynamo.scan(scanRequest(exclusiveStartKey))
  }

  // persistence id is formatted as follows journal-P-98adb33a-a94d-4ec8-a279-4570e16a0c14-0
  // see DynamoDBJournal.messagePartitionKeyFromGroupNr
  private def parsePersistenceId(rawPersistenceId: String, journalName: String): String =
    try {
      val prefixLength = journalName.length + 3
      val startPostfix = rawPersistenceId.lastIndexOf("-")
      rawPersistenceId.substring(prefixLength, startPostfix)
    } catch {
      case NonFatal(_) =>
        log.error("Could not parse raw persistence id '{}' using journal name '{}'. Returning it unparsed.", rawPersistenceId, journalName)
        rawPersistenceId
    }
}

object DynamodbCurrentPersistenceIdsQuery {
    implicit class RichString(val s: String) extends AnyVal {
    def toAttribute: AttributeValue = new AttributeValue().withS(s)
  }

  implicit class RichNumber(val n: Int) extends AnyVal {
    def toAttribute: AttributeValue = new AttributeValue().withN(n.toString)
  }

  implicit class RichOption[+A](val option: Option[A]) extends AnyVal {
    def toSource: Source[A, NotUsed] = option match {
        case Some(value) => Source.single(value)
        case None => Source.empty
      }
  }

  implicit class SourceLazyOps[E, M](val src: Source[E, M]) extends AnyVal {

    // see https://github.com/akka/akka/issues/23044
    // when migrating to akka 2.6.x use akka's concatLazy
    def concatLazy[M1](src2: => Source[E, M1]): Source[E, NotUsed] =
      Source(List(() => src, () => src2)).flatMapConcat(_ ())
  }
}

// The commonality between QueryResult and ScanResult which don't share an interface
private [query] trait PersistenceIdsResult[A] {
  def toPersistenceIdsPage(result: A): Seq[String]

  def nextEvaluatedKey(result: A): Option[util.Map[String, AttributeValue]]
}

private [query] object PersistenceIdsResult {

  implicit val persistenceIdsQueryResult: PersistenceIdsResult[QueryResult] = new PersistenceIdsResult[QueryResult] {
    override def toPersistenceIdsPage(result: QueryResult): Seq[String] =
      result.getItems.asScala.map(item => item.get("par").getS).toSeq

    override def nextEvaluatedKey(result: QueryResult): Option[util.Map[String, AttributeValue]] =
      if (result.getLastEvaluatedKey != null && !result.getLastEvaluatedKey.isEmpty) Some(result.getLastEvaluatedKey) else None
  }

  implicit val persistenceIdsScanResult: PersistenceIdsResult[ScanResult] = new PersistenceIdsResult[ScanResult] {
    override def toPersistenceIdsPage(result: ScanResult): Seq[String] =
      result.getItems.asScala.map(item => item.get("par").getS).toSeq

    override def nextEvaluatedKey(result: ScanResult): Option[util.Map[String, AttributeValue]] =
      if (result.getLastEvaluatedKey != null && !result.getLastEvaluatedKey.isEmpty) Some(result.getLastEvaluatedKey) else None
  }

  implicit class RichPersistenceIdsResult[Result](val result: Result) extends AnyVal {
    def toPersistenceIdsPage(implicit persistenceIdsResult: PersistenceIdsResult[Result]): Seq[String] =
      persistenceIdsResult.toPersistenceIdsPage(result)

    def nextEvaluatedKey(implicit persistenceIdsResult: PersistenceIdsResult[Result]): Option[util.Map[String, AttributeValue]] =
      persistenceIdsResult.nextEvaluatedKey(result)
  }
}

trait CreatePersistenceIdsIndex { self: ReadJournalSettingsProvider with DynamoProvider with ActorSystemProvider with LoggingProvider =>

  /** Update the journal table to add the Global Secondary Index 'persistence-ids-idx' that's required by [[DynamodbCurrentPersistenceIdsQuery.currentPersistenceIdsByPageQuery]] */
  def createPersistenceIdsIndex(): Future[UpdateTableResult] =
    dynamo.updateTable(createPersistenceIdsIndexRequest(tableName = readJournalSettings.Table))
}

object CreatePersistenceIdsIndex {

  def createPersistenceIdsIndexRequest(tableName: String): UpdateTableRequest = {
    val creatIndex = new CreateGlobalSecondaryIndexAction()
      .withIndexName("persistence-ids-idx")
      .withKeySchema(new KeySchemaElement().withAttributeName("num").withKeyType(KeyType.HASH))
      .withProjection(new Projection().withProjectionType(ProjectionType.KEYS_ONLY))
    val update = new GlobalSecondaryIndexUpdate().withCreate(creatIndex)
    new UpdateTableRequest().withTableName(tableName)
      .withGlobalSecondaryIndexUpdates(update)
      .withAttributeDefinitions(new AttributeDefinition().withAttributeName("num").withAttributeType("N"))
  }
}
