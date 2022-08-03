package akka.persistence.dynamodb.query.javadsl

import akka.NotUsed
import akka.persistence.dynamodb.query.scaladsl.DynamodbReadJournal
import akka.persistence.query.EventEnvelope

class JavaDynamodbReadJournal(scaladslReadJournal: DynamodbReadJournal)
    extends akka.persistence.query.javadsl.ReadJournal
    //    with akka.persistence.query.javadsl.EventsByTagQuery
    with akka.persistence.query.javadsl.CurrentEventsByPersistenceIdQuery
    with akka.persistence.query.javadsl.CurrentPersistenceIdsQuery
    //with akka.persistence.query.javadsl.CurrentPersistenceIdsQuery
    {
  def currentPersistenceIds(): akka.stream.javadsl.Source[String, NotUsed] =
    scaladslReadJournal.currentPersistenceIds().asJava

  def currentEventsByPersistenceId(
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long): akka.stream.javadsl.Source[EventEnvelope, NotUsed] =
    scaladslReadJournal.currentEventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr).asJava
}
