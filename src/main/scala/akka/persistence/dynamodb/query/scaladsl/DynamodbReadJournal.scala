package akka.persistence.dynamodb.query.scaladsl

import akka.NotUsed
import akka.actor.ExtendedActorSystem
import akka.actor.TypedActor.context
import akka.event.LoggingAdapter
import akka.persistence.dynamodb.dynamoClient
import akka.persistence.dynamodb.journal.{DynamoDBHelper, DynamoDBJournalConfig, JournalKeys, JournalSettingsProvider, LoggingProvider, MaterializerProvider, SerializationProvider}
import akka.persistence.dynamodb.query.DynamoDBReadJournalConfig
import akka.persistence.query.EventEnvelope
import akka.persistence.query.scaladsl.{CurrentEventsByPersistenceIdQuery, ReadJournal}
import akka.serialization.{Serialization, SerializationExtension}
import akka.stream.{Materializer, SystemMaterializer}
import akka.stream.scaladsl.Source
import com.typesafe.config.Config

class DynamodbReadJournal(config: Config, configPath: String)(implicit val system: ExtendedActorSystem)
    extends ReadJournal
    with DynamodbCurrentPersistenceIdsQuery
    with DynamodbCurrentEventsByPersistenceIdQuery
    with SettingsProvider with JournalSettingsProvider with DynamoProvider with ActorSystemProvider with MaterializerProvider with LoggingProvider with JournalKeys with SerializationProvider with ActorSystemLoggingProvider {
  protected val settings = new DynamoDBReadJournalConfig(config)
  protected val dynamo: DynamoDBHelper = dynamoClient(system, settings)
  val serialization: Serialization = SerializationExtension(system)
  implicit val materializer :Materializer = SystemMaterializer(context.system).materializer
  val journalSettings = new DynamoDBJournalConfig(config)

}

object DynamodbReadJournal {
  val Identifier = "dynamodb-read-journal"
}
trait DynamoProvider {
  protected def dynamo: DynamoDBHelper
}
trait SettingsProvider {
  protected def settings:  DynamoDBReadJournalConfig
}
trait ActorSystemProvider {
  protected implicit val system: ExtendedActorSystem
}

trait ActorSystemLoggingProvider extends ActorSystemProvider with LoggingProvider {
  val log: LoggingAdapter = system.log
}