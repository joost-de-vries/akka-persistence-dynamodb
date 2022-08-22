package akka.persistence.dynamodb.query.scaladsl

import akka.actor.ExtendedActorSystem
import akka.actor.TypedActor.context
import akka.persistence.dynamodb.journal._
import akka.persistence.dynamodb.query.{ DynamoDBReadJournalConfig, ReadJournalSettingsProvider }
import akka.persistence.dynamodb._
import akka.persistence.query.scaladsl.ReadJournal
import akka.serialization.{ Serialization, SerializationExtension }
import akka.stream.{ Materializer, SystemMaterializer }
import com.typesafe.config.Config

/**
 * Scala API `akka.persistence.query.scaladsl.ReadJournal` implementation for Dynamodb.
 *
 * It is retrieved with:
 * {{{
 * val queries = PersistenceQuery(system).readJournalFor[DynamodbReadJournal](DynamodbReadJournal.Identifier)
 * }}}
 *
 * Corresponding Java API is in [[akka.persistence.dynamodb.query.javadsl.DynamodbReadJournal]].
 *
 * Configuration settings can be defined in the configuration section with the
 * absolute path corresponding to the identifier, which is `"dynamodb-read-journal"`
 * for the default [[DynamodbReadJournal#Identifier]]. See `reference.conf`.
 */
class DynamodbReadJournal(config: Config, configPath: String)(implicit val system: ExtendedActorSystem)
    extends ReadJournal
    with DynamodbCurrentPersistenceIdsQuery
    with DynamodbCurrentEventsByPersistenceIdQuery
    with ReadJournalSettingsProvider
    with JournalSettingsProvider
    with DynamoProvider
    with ActorSystemProvider
    with MaterializerProvider
    with LoggingProvider
    with JournalKeys
    with SerializationProvider
    with ActorSystemLoggingProvider {

  protected val readJournalSettings       = new DynamoDBReadJournalConfig(config)
  protected val dynamo: DynamoDBHelper    = dynamoClient(system, readJournalSettings)
  val serialization: Serialization        = SerializationExtension(system)
  implicit val materializer: Materializer = SystemMaterializer(context.system).materializer
  val journalSettings                     = new DynamoDBJournalConfig(config)

}

object DynamodbReadJournal {

  /**
   * The default identifier for [[DynamodbReadJournal]] to be used with
   * `akka.persistence.query.PersistenceQuery#readJournalFor`.
   *
   * The value is `"dynamodb-read-journal"` and corresponds
   * to the absolute path to the read journal configuration entry.
   */
  val Identifier = "dynamodb-read-journal"
}
