package akka.persistence.dynamodb.query.scaladsl

import akka.NotUsed
import akka.persistence.PersistentRepr
import akka.persistence.dynamodb.{ActorSystemProvider, DynamoProvider, LoggingProvider, MaterializerProvider}
import akka.persistence.dynamodb.journal._
import akka.persistence.dynamodb.query.ReadJournalSettingsProvider
import akka.persistence.dynamodb.query.scaladsl.DynamodbCurrentEventsByPersistenceIdQuery.RichPersistenceRepr
import akka.persistence.query.scaladsl.CurrentEventsByPersistenceIdQuery
import akka.persistence.query.{EventEnvelope, Sequence}
import akka.stream.scaladsl.Source

trait DynamodbCurrentEventsByPersistenceIdQuery extends CurrentEventsByPersistenceIdQuery with DynamoDBRecovery {
  self: ReadJournalSettingsProvider
    with DynamoProvider
    with ActorSystemProvider
    with JournalSettingsProvider
    with ActorSystemProvider
    with MaterializerProvider
    with LoggingProvider
    with JournalKeys
    with SerializationProvider =>

  /**
   * Same type of query as [[akka.persistence.query.scaladsl.EventsByPersistenceIdQuery.eventsByPersistenceId]]
   * but the event stream is completed immediately when it reaches the end of
   * the results. Events that are stored after the query is completed are
   * not included in the event stream.
   *
   * Execution plan:
   * - a dynamodb <code>query</code> to get lowest sequenceNr
   * - a <code>query</code> per partition. Doing follow calls to get more pages if necessary.
   */
  override def currentEventsByPersistenceId(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long): Source[EventEnvelope, NotUsed] = {
    log.debug("starting currentEventsByPersistenceId for {} from {} to {}", persistenceId, fromSequenceNr, toSequenceNr)
    eventsStream(persistenceId = persistenceId, fromSequenceNr = fromSequenceNr, toSequenceNr = toSequenceNr, max = Int.MaxValue)
      .map(_.toEventEnvelope)
      .log(s"currentEventsByPersistenceId for $persistenceId from $fromSequenceNr to $toSequenceNr")
  }
}

object DynamodbCurrentEventsByPersistenceIdQuery {
  implicit class RichPersistenceRepr(val persistenceRepr: PersistentRepr) extends AnyVal {
    def toEventEnvelope = new EventEnvelope(
        offset = Sequence(persistenceRepr.sequenceNr),
        persistenceId = persistenceRepr.persistenceId,
        sequenceNr = persistenceRepr.sequenceNr,
        event = persistenceRepr.payload,
        timestamp = persistenceRepr.timestamp)
  }
}
