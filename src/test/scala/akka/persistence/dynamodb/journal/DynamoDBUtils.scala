/**
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.dynamodb.journal

import akka.actor.ActorSystem
import akka.persistence.PersistentRepr
import akka.persistence.dynamodb._
import akka.util.Timeout
import com.amazonaws.services.dynamodbv2.model._

import java.util.UUID
import scala.collection.JavaConverters._
import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._

trait DynamoDBUtils extends JournalSettingsProvider with DynamoProvider {

  val system: ActorSystem
  import system.dispatcher

  override lazy val journalSettings = {
    val c      = system.settings.config
    val config = c.getConfig(c.getString("akka.persistence.journal.plugin"))
    new DynamoDBJournalConfig(config)
  }

  override lazy val dynamo: DynamoDBHelper = dynamoClient(system, journalSettings)

  implicit val timeout = Timeout(5.seconds)

  def ensureJournalTableExists(read: Long = 10L, write: Long = 10L): Unit = {
    val create =
      schema
        .withTableName(journalSettings.JournalTable)
        .withProvisionedThroughput(new ProvisionedThroughput(read, write))

    var names = Vector.empty[String]
    lazy val complete: ListTablesResult => Future[Vector[String]] = aws =>
      if (aws.getLastEvaluatedTableName == null) Future.successful(names ++ aws.getTableNames.asScala)
      else {
        names ++= aws.getTableNames.asScala
        dynamo
          .listTables(new ListTablesRequest().withExclusiveStartTableName(aws.getLastEvaluatedTableName))
          .flatMap(complete)
      }
    val list = dynamo.listTables(new ListTablesRequest).flatMap(complete)

    val setup = for {
      exists <- list.map(_ contains journalSettings.JournalTable)
      _ <- {
        if (exists) Future.successful(())
        else dynamo.createTable(create)
      }
    } yield ()
    Await.result(setup, 5.seconds)
  }

  private val writerUuid    = UUID.randomUUID.toString
  def persistenceId: String = ???

  var nextSeqNr = 1
  def seqNr() = {
    val ret = nextSeqNr
    nextSeqNr += 1
    ret
  }
  var generatedMessages: Vector[PersistentRepr] = Vector(null) // we start counting at 1

  def persistentRepr(msg: Any) = {
    val ret = PersistentRepr(msg, sequenceNr = seqNr(), persistenceId = persistenceId, writerUuid = writerUuid)
    generatedMessages :+= ret
    ret
  }
}
