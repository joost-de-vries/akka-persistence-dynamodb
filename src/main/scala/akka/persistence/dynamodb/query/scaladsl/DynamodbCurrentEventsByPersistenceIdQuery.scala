package akka.persistence.dynamodb.query.scaladsl

import akka.NotUsed
import akka.persistence.PersistentRepr
import akka.persistence.dynamodb.journal._
import akka.persistence.dynamodb.query.scaladsl.DynamodbCurrentEventsByPersistenceIdQuery.RichPersistenceRepr
import akka.persistence.query.scaladsl.CurrentEventsByPersistenceIdQuery
import akka.persistence.query.{ EventEnvelope, Sequence }
import akka.stream.scaladsl.Source

trait DynamodbCurrentEventsByPersistenceIdQuery extends CurrentEventsByPersistenceIdQuery with DynamoDBRecovery {
  self: SettingsProvider
    with DynamoProvider
    with ActorSystemProvider
    with JournalSettingsProvider
    with ActorSystemProvider
    with MaterializerProvider
    with LoggingProvider
    with JournalKeys
    with SerializationProvider =>

  override def currentEventsByPersistenceId(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long): Source[EventEnvelope, NotUsed] =
    eventsStream(persistenceId = persistenceId, fromSequenceNr = fromSequenceNr, toSequenceNr = toSequenceNr, max = Int.MaxValue)
      .map(_.toEventEnvelope(persistenceId))
}

object DynamodbCurrentEventsByPersistenceIdQuery {
  implicit class RichPersistenceRepr(val persistenceRepr: PersistentRepr) extends AnyVal {
    def toEventEnvelope(persistenceId: String) =
      new EventEnvelope(
        offset = Sequence(persistenceRepr.sequenceNr),
        persistenceId = persistenceId,
        sequenceNr = persistenceRepr.sequenceNr,
        event = persistenceRepr.payload,
        timestamp = persistenceRepr.timestamp)
  }
}
