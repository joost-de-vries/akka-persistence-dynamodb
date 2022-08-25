package akka.persistence.dynamodb.query.javadsl

import akka.NotUsed
import akka.persistence.query.EventEnvelope
import akka.stream.javadsl.Source

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

/**
 * Java API: `akka.persistence.query.javadsl.ReadJournal` implementation for Dynamodb.
 *
 * It is retrieved with:
 * {{{
 * DynamodbReadJournal queries =
 *   PersistenceQuery.get(system).getReadJournalFor(DynamodbReadJournal.class, DynamodbReadJournal.Identifier());
 * }}}
 *
 * Corresponding Scala API is in [[DynamodbReadJournal]].
 *
 * Configuration settings can be defined in the configuration section with the
 * absolute path corresponding to the identifier, which is `"dynamodb-read-journal"`
 * for the default [[DynamodbReadJournal#Identifier]]. See `reference.conf`.
 */
class DynamodbReadJournal(scaladslReadJournal: DynamodbReadJournal)
    extends akka.persistence.query.javadsl.ReadJournal
    //    with akka.persistence.query.javadsl.EventsByTagQuery
    with akka.persistence.query.javadsl.CurrentEventsByPersistenceIdQuery
    with akka.persistence.query.javadsl.CurrentPersistenceIdsQuery
    //with akka.persistence.query.javadsl.CurrentPersistenceIdsQuery
    {
  def currentPersistenceIds(): Source[String, NotUsed] =
    scaladslReadJournal.currentPersistenceIds().asJava

  def currentEventsByPersistenceId(
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long): Source[EventEnvelope, NotUsed] =
    scaladslReadJournal.currentEventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr).asJava
}
