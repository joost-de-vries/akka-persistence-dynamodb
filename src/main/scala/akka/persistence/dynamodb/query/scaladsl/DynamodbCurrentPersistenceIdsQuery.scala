package akka.persistence.dynamodb.query.scaladsl

import akka.NotUsed
import akka.persistence.dynamodb.query.scaladsl.DynamodbCurrentPersistenceIdsQuery.{RichNumber, RichOption, RichScanResult}
import akka.persistence.query.scaladsl.CurrentPersistenceIdsQuery
import akka.stream.scaladsl.Source
import com.amazonaws.services.dynamodbv2.model.{AttributeValue, ScanRequest, ScanResult}

import scala.concurrent.Future
import scala.jdk.CollectionConverters.{CollectionHasAsScala, MapHasAsJava}
import scala.util.Try

trait DynamodbCurrentPersistenceIdsQuery extends CurrentPersistenceIdsQuery { self: SettingsProvider with DynamoProvider with ActorSystemProvider =>

  def currentPersistenceIds(): Source[String, NotUsed] =
    currentPersistenceIdsByPage().mapConcat(identity)

  def currentPersistenceIdsByPage(): Source[Seq[String], NotUsed] = {
    // persistence id is formatted as follows journal-P-98adb33a-a94d-4ec8-a279-4570e16a0c14-0
    // see DynamoDBJournal.messagePartitionKeyFromGroupNr
    def parsePersistenceId(parValue: String, journalName: String): String =
      Try {
        val postfix = parValue.substring(journalName.length + 3)
        postfix.substring(0, postfix.lastIndexOf("-"))
      }.getOrElse(parValue)

    def scanRequest(exclusiveStartKey: Option[java.util.Map[String, AttributeValue]]): ScanRequest = {
      val req = new ScanRequest()
        .withTableName(settings.Table)
        .withProjectionExpression("par")
        .withFilterExpression("num = :n")
        .withExpressionAttributeValues(Map(":n" -> 1.toAttribute).asJava)
      exclusiveStartKey.foreach(esk => req.withExclusiveStartKey(esk))
      req
    }

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

    // infinite stream of call results
    val streamOfResults: Source[Option[ScanResult], NotUsed] =
      lazyStream(Source.fromFuture(dynamo.scan(scanRequest(None)).map(Some(_))))

    streamOfResults
      .takeWhile(_.isDefined)
      .flatMapConcat(_.toSource)
      .map(scanResult =>
        scanResult.toPersistenceIdsPage.map { rawPersistenceId =>
          parsePersistenceId(parValue = rawPersistenceId, journalName = settings.JournalName)
        })
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