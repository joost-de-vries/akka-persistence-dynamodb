package akka.persistence.dynamodb.query

import akka.actor.ExtendedActorSystem
import akka.persistence.dynamodb.query.javadsl.JavaDynamodbReadJournal
import akka.persistence.dynamodb.query.scaladsl.DynamodbReadJournal
import akka.persistence.query.scaladsl.ReadJournal
import akka.persistence.query.{ReadJournalProvider, javadsl}
import com.typesafe.config.Config

class DynamodbReadJournalProvider(system: ExtendedActorSystem, config: Config, configPath: String) extends ReadJournalProvider {
  private lazy val _scalaReadJournal = new DynamodbReadJournal(config, configPath)(system)
  override def scaladslReadJournal(): ReadJournal = _scalaReadJournal

  private lazy val _javadslReadJournal = new JavaDynamodbReadJournal(_scalaReadJournal)
  override def javadslReadJournal(): javadsl.ReadJournal = _javadslReadJournal
}

