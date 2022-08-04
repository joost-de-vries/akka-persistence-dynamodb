package akka.persistence.dynamodb.query.javadsl

import akka.NotUsed
import akka.persistence.dynamodb.query.scaladsl.{DynamodbReadJournal => ScalaDynamodbReadJournal}
import akka.persistence.query.EventEnvelope
import akka.stream.javadsl.Source

class DynamodbReadJournal(scaladslReadJournal: ScalaDynamodbReadJournal)
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
