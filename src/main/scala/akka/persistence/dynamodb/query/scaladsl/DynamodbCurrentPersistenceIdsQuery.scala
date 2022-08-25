package akka.persistence.dynamodb.query.scaladsl

import akka.NotUsed
import akka.persistence.dynamodb.query.ReadJournalSettingsProvider
import akka.persistence.dynamodb.query.scaladsl.CreatePersistenceIdsIndex.createPersistenceIdsIndexRequest
import akka.persistence.dynamodb.{ ActorSystemProvider, DynamoProvider, LoggingProvider }
import akka.persistence.query.scaladsl.CurrentPersistenceIdsQuery
import akka.stream.scaladsl.Source
import com.amazonaws.services.dynamodbv2.model._

import scala.concurrent.Future

trait DynamodbCurrentPersistenceIdsQuery extends CurrentPersistenceIdsQuery {

  /**
   * Same type of query as [[akka.persistence.query.scaladsl.PersistenceIdsQuery.persistenceIds()]] but the stream
   * is completed immediately when it reaches the end of the "result set". Persistent
   * actors that are created after the query is completed are not included in the stream.
   *
   * A dynamodb <code>query</code> will be performed against a Global Secondary Index 'persistence-ids-idx'.
   * See [[CreatePersistenceIdsIndex.createPersistenceIdsIndexRequest]]
   */
  override def currentPersistenceIds(): Source[String, NotUsed]

  /**
   * Persistence ids are returned page by page.
   * A dynamodb <code>scan</code> will be performed. Results will be paged per 1 MB size.
   */
  def currentPersistenceIdsByPageScan(): Source[Seq[String], NotUsed]

  /**
   * Persistence ids are returned page by page.
   * A dynamodb <code>query</code> will be performed against a Global Secondary Index 'persistence-ids-idx'.
   * See [[CreatePersistenceIdsIndex.createPersistenceIdsIndexRequest]]
   */
  def currentPersistenceIdsByPageQuery(): Source[Seq[String], NotUsed]

}
trait CreatePersistenceIdsIndex {
  self: ReadJournalSettingsProvider with DynamoProvider with ActorSystemProvider with LoggingProvider =>

  /** Update the journal table to add the Global Secondary Index 'persistence-ids-idx' that's required by [[DynamodbCurrentPersistenceIdsQuery.currentPersistenceIdsByPageQuery]] */
  def createPersistenceIdsIndex(): Future[UpdateTableResult] =
    dynamo.updateTable(
      createPersistenceIdsIndexRequest(
        indexName = readJournalSettings.PersistenceIdsIndexName,
        tableName = readJournalSettings.Table))
}

object CreatePersistenceIdsIndex {

  def createPersistenceIdsIndexRequest(indexName: String, tableName: String): UpdateTableRequest = {
    val creatIndex = new CreateGlobalSecondaryIndexAction()
      .withIndexName(indexName)
      .withKeySchema(new KeySchemaElement().withAttributeName("num").withKeyType(KeyType.HASH))
      .withProjection(new Projection().withProjectionType(ProjectionType.KEYS_ONLY))
    val update = new GlobalSecondaryIndexUpdate().withCreate(creatIndex)
    new UpdateTableRequest()
      .withTableName(tableName)
      .withGlobalSecondaryIndexUpdates(update)
      .withAttributeDefinitions(new AttributeDefinition().withAttributeName("num").withAttributeType("N"))
  }
}
