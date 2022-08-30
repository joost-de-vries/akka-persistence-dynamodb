package akka.persistence.dynamodb.query

import akka.actor.ActorSystem
import akka.persistence.JournalProtocol._
import akka.persistence._
import akka.persistence.dynamodb.journal.DynamoDBUtils
import akka.persistence.dynamodb.query.scaladsl.{ CreatePersistenceIdsIndex, DynamodbReadJournal }
import akka.persistence.dynamodb.{ DynamoProvider, IntegSpec }
import akka.persistence.query.PersistenceQuery
import akka.stream.scaladsl.Sink
import akka.stream.{ Materializer, SystemMaterializer }
import akka.testkit._
import org.scalactic.TypeCheckedTripleEquals
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

import java.util.UUID
import scala.concurrent.duration.DurationInt

class CurrentPersistenceIdsSpec
    extends TestKit(ActorSystem("CurrentPersistenceIdsSpec"))
    with ImplicitSender
    with WordSpecLike
    with BeforeAndAfterAll
    with Matchers
    with ScalaFutures
    with TypeCheckedTripleEquals
    with DynamoDBUtils
    with IntegSpec
    with CreatePersistenceIdsIndex
    with ReadJournalSettingsProvider
    with DynamoProvider {
  override protected lazy val readJournalSettings: DynamoDBReadJournalConfig = DynamoDBReadJournalConfig()
  override implicit val patienceConfig: PatienceConfig                       = PatienceConfig(15.seconds)

  override def beforeAll(): Unit = {
    super.beforeAll()
    ensureJournalTableExists()
    createPersistenceIdsIndex().futureValue
  }

  override def afterAll(): Unit = {
    dynamo.shutdown()
    system.terminate().futureValue
    super.afterAll()
  }

  val persistenceIds                      = (0 to 100).map(i => s"CurrentPersistenceIdsSpec_$i")
  val writerUuid                          = UUID.randomUUID.toString
  implicit val materializer: Materializer = SystemMaterializer(system).materializer
  lazy val journal                        = Persistence(system).journalFor("")
  lazy val queries                        = PersistenceQuery(system).readJournalFor[DynamodbReadJournal](DynamodbReadJournal.Identifier)

  "DynamoDB ReadJournal" must {

    "query current persistence ids" in {
      val writes = persistenceIds.map(
        persistenceId =>
          AtomicWrite(
            (0 to 5).map(
              i =>
                PersistentRepr(
                  payload = s"$persistenceId $i",
                  sequenceNr = i,
                  persistenceId = persistenceId,
                  writerUuid = writerUuid))))
      writes.foreach { message =>
        journal ! WriteMessages(message :: Nil, testActor, 1) 
        expectMsg(WriteMessagesSuccessful)
      }

      val currentPersistenceIds = queries.currentPersistenceIds().runWith(Sink.collection).futureValue.toSeq

      currentPersistenceIds.sorted shouldBe persistenceIds.sorted
    }
  }
}
