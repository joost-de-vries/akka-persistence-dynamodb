package akka.persistence.dynamodb.query.scaladsl

import akka.NotUsed
import akka.actor.ExtendedActorSystem
import akka.persistence.dynamodb.dynamoClient
import akka.persistence.dynamodb.journal.DynamoDBHelper
import akka.persistence.dynamodb.query.DynamoDBReadJournalConfig
import akka.persistence.query.EventEnvelope
import akka.persistence.query.scaladsl.{CurrentEventsByPersistenceIdQuery, ReadJournal}
import akka.stream.scaladsl.Source
import com.typesafe.config.Config

class DynamodbReadJournal(config: Config, configPath: String)(implicit val system: ExtendedActorSystem)
    extends ReadJournal
    with DynamodbCurrentPersistenceIdsQuery
//    with CurrentEventsByPersistenceIdQuery
    with SettingsProvider with DynamoProvider with ActorSystemProvider {
  protected val settings = new DynamoDBReadJournalConfig(config)
  protected val dynamo: DynamoDBHelper = dynamoClient(system, settings)

//  override def currentEventsByPersistenceId(
//      persistenceId: String,
//      fromSequenceNr: Long,
//      toSequenceNr: Long): Source[EventEnvelope, NotUsed] = ???
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