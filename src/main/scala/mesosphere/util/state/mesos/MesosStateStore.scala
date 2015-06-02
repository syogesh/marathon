package mesosphere.util.state.mesos

import mesosphere.marathon.StoreCommandFailedException
import mesosphere.util.BackToTheFuture.Timeout
import mesosphere.util.ThreadPoolContext
import mesosphere.util.state.{ PersistentEntity, PersistentStore }
import org.apache.mesos.state.{ Variable, State }

import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.collection.JavaConverters._

class MesosStateStore(state: State, timeout: Duration) extends PersistentStore {

  implicit val timeoutDuration = Timeout(timeout)
  implicit val ec = ThreadPoolContext.context
  import mesosphere.util.BackToTheFuture.futureToFuture

  override def load(key: ID): Future[Option[PersistentEntity]] = {
    futureToFuture(state.fetch(key)).map { variable =>
      if (variable.value().length > 0) Some(MesosStateEntity(key, variable)) else None
    }
  }

  override def create(key: ID, content: Array[Byte]): Future[PersistentEntity] = {
    futureToFuture(state.fetch(key)).flatMap { variable =>
      if (variable.value().length > 0) throw new StoreCommandFailedException(s"Entity with id $key already exists!")
      else futureToFuture(state.store(variable.mutate(content))).map(MesosStateEntity(key, _))
    }
  }

  override def save(entity: PersistentEntity): Future[PersistentEntity] = entity match {
    case MesosStateEntity(id, v) => futureToFuture(state.store(v)).map(MesosStateEntity(id, _))
    case _                       => throw new IllegalArgumentException("Can not handle this kind of entity")
  }

  override def delete(key: ID): Future[PersistentEntity] = {
    futureToFuture(state.fetch(key)).flatMap { variable =>
      futureToFuture(state.expunge(variable)).map(_ => MesosStateEntity(key, variable))
    }
  }

  override def allIds(): Future[Seq[ID]] = futureToFuture(state.names()).map(_.asScala.toSeq)
}

case class MesosStateEntity(id: String, variable: Variable) extends PersistentEntity {
  override def bytes: Array[Byte] = variable.value()
  override def mutate(bytes: Array[Byte]): PersistentEntity = copy(variable = variable.mutate(bytes))
}
