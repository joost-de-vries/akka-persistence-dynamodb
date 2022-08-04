package akka.persistence.dynamodb.query.scaladsl

import akka.NotUsed
import akka.persistence.dynamodb.query.ReadJournalSettingsProvider
import akka.persistence.dynamodb.{ActorSystemProvider, DynamoProvider, LoggingProvider}
import akka.persistence.dynamodb.query.scaladsl.DynamodbCurrentPersistenceIdsQuery.{RichNumber, RichOption, RichScanResult}
import akka.persistence.query.scaladsl.CurrentPersistenceIdsQuery
import akka.stream.scaladsl.Source
import com.amazonaws.services.dynamodbv2.model.{AttributeValue, ScanRequest, ScanResult}

import akka.util.ccompat.JavaConverters._
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
    currentPersistenceIdsByPageInternal()
      .mapConcat(seq => seq.toList)
      .log("currentPersistenceIds")
  }

  /**
   * The implementation that is used for [[currentPersistenceIds()]]
   * Here the results are offered page by page.
   * A dynamodb <code>scan</code> will be performed. Results will be paged per 1 MB size.
   */
  def currentPersistenceIdsByPage(): Source[Seq[String], NotUsed] = {
    log.debug("starting currentPersistenceIdsByPage")
    currentPersistenceIdsByPageInternal()
      .log("currentPersistenceIdsByPage")
  }

  private def currentPersistenceIdsByPageInternal(): Source[Seq[String], NotUsed] = {
    import system.dispatcher

    def nextCall(previousOpt: Option[ScanResult]) =
      previousOpt match {
        case Some(previous) =>
          if (previous.getLastEvaluatedKey == null || previous.getLastEvaluatedKey.isEmpty) {
            Future.successful(None)
          } else {
            dynamo.scan(scanRequest(Some(previous.getLastEvaluatedKey))).map(Some(_))
          }
        case None => Future.successful(None)
      }

    def lazyStream(v: Source[Option[ScanResult], NotUsed]): Source[Option[ScanResult], NotUsed] =
      v.concat(Source.lazily(() => lazyStream(v.mapAsync(1)(nextCall))))

    val infiniteStreamOfResults: Source[Option[ScanResult], NotUsed] =
      lazyStream(Source.fromFuture(dynamo.scan(scanRequest(None)).map(Some(_))))

    infiniteStreamOfResults
      .takeWhile(_.isDefined)
      .flatMapConcat(_.toSource)
      .map(scanResult =>
        scanResult.toPersistenceIdsPage
          .map ( rawPersistenceId => parsePersistenceId(parValue = rawPersistenceId, journalName = readJournalSettings.JournalName) )
      )
  }

  private def scanRequest(exclusiveStartKey: Option[java.util.Map[String, AttributeValue]]): ScanRequest = {
    val req = new ScanRequest()
      .withTableName(readJournalSettings.Table)
      .withProjectionExpression("par")
      .withFilterExpression("num = :n")
      .withExpressionAttributeValues(Map(":n" -> 1.toAttribute).asJava)
    exclusiveStartKey.foreach(esk => req.withExclusiveStartKey(esk))
    req
  }

  // persistence id is formatted as follows journal-P-98adb33a-a94d-4ec8-a279-4570e16a0c14-0
  // see DynamoDBJournal.messagePartitionKeyFromGroupNr
  private def parsePersistenceId(parValue: String, journalName: String): String =
    try {
      val postfix = parValue.substring(journalName.length + 3)
      postfix.substring(0, postfix.lastIndexOf("-"))
    } catch {
      case NonFatal(exception) =>
        log.error("Could not parse raw persistence id '{}'. Returning it unparsed.", parValue)
        parValue
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
    def toSource: Source[A, NotUsed] =
      option match {
        case Some(value) => Source.single(value)
        case None => Source.empty
      }
  }

  implicit class RichScanResult(val scanResult: ScanResult) extends AnyVal {
    def toPersistenceIdsPage: Seq[String] = scanResult.getItems.asScala.map(item => item.get("par").getS).toSeq
  }

}
