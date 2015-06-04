package mesosphere.util.state.memory

import mesosphere.marathon.StoreCommandFailedException
import mesosphere.util.ThreadPoolContext
import mesosphere.util.state.{ PersistentEntity, PersistentStore }

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ ExecutionContext, Future }

/**
  * Simple in memory store implementation.
  * This is intended only for tests - do not use in production!
  * @param ec the execution context to use.
  */
class InMemoryStore(implicit val ec: ExecutionContext = ThreadPoolContext.context) extends PersistentStore {

  private[this] val entities = TrieMap.empty[ID, InMemoryEntity]

  override def load(key: ID): Future[Option[PersistentEntity]] = Future.successful{
    entities.get(key)
  }

  override def create(key: ID, content: Array[Byte]): Future[PersistentEntity] = Future {
    if (entities.contains(key)) throw new StoreCommandFailedException(s"Entity with id $key already exists!")
    val entity = InMemoryEntity(key, 0, content)
    entities.put(key, entity)
    entity
  }

  override def update(entity: PersistentEntity): Future[PersistentEntity] = Future {
    entity match {
      case e @ InMemoryEntity(id, version, _) =>
        if (entities(id).version != version) {
          throw new StoreCommandFailedException(s"Concurrent updates! Version differs")
        }
        val update = e.incrementVersion
        entities.replace(id, update)
        update
      case _ => throw new IllegalArgumentException(s"Wrong entity type: $entity")
    }
  }

  override def delete(key: ID): Future[Boolean] = {
    entities.get(key) match {
      case Some(value) =>
        entities.remove(key)
        Future.successful(true)
      case None => Future.successful(false)
    }
  }

  override def allIds(): Future[Seq[ID]] = Future.successful(entities.keySet.toSeq)
}

case class InMemoryEntity(id: String, version: Int, bytes: Array[Byte] = Array.empty) extends PersistentEntity {
  override def mutate(bytes: Array[Byte]): PersistentEntity = copy(bytes = bytes)
  def incrementVersion: InMemoryEntity = copy(version = version + 1)
}
