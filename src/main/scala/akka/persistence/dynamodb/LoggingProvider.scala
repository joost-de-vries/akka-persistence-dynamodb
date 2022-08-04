package akka.persistence.dynamodb

import akka.actor.ExtendedActorSystem
import akka.event.LoggingAdapter
import akka.persistence.dynamodb.journal.{DynamoDBHelper, DynamoDBJournalConfig}
import akka.stream.Materializer

trait LoggingProvider {

  def log: LoggingAdapter
}

trait ActorSystemLoggingProvider extends ActorSystemProvider with LoggingProvider {
  val log: LoggingAdapter = system.log
}

trait ActorSystemProvider {
  protected implicit val system: ExtendedActorSystem
}

trait DynamoProvider {
  protected def dynamo: DynamoDBHelper
}

trait MaterializerProvider {
  protected implicit val materializer: Materializer
}