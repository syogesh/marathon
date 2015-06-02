package mesosphere.marathon.state

import com.codahale.metrics.{ MetricRegistry, Histogram }
import com.codahale.metrics.MetricRegistry._
import mesosphere.util.{ LockManager, ThreadPoolContext }
import mesosphere.util.state.PersistentStore
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.reflect.ClassTag

class MarathonStore[S <: MarathonState[_, S]](
    val store: PersistentStore,
    registry: MetricRegistry,
    newState: () => S,
    prefix: String = "app:")(implicit ct: ClassTag[S]) extends EntityStore[S] {

  import ThreadPoolContext.context
  private[this] val log = LoggerFactory.getLogger(getClass)

  private[this] lazy val locks = LockManager[String]()
  private[this] lazy val bytesRead: Histogram =
    registry.histogram(name(getClass, ct.getClass.getSimpleName, "read-data-size"))
  private[this] lazy val bytesWritten: Histogram =
    registry.histogram(name(getClass, ct.getClass.getSimpleName, "write-data-size"))

  def fetch(key: String): Future[Option[S]] = {
    store.load(prefix + key).map {
      _.map{ entity =>
        bytesRead.update(entity.bytes.length)
        stateFromBytes(entity.bytes)
      }
    }
  }

  def modify(key: String)(f: (() => S) => S): Future[S] = withLock(key) {
    val res = store.load(prefix + key).flatMap {
      case Some(entity) =>
        bytesRead.update(entity.bytes.length)
        val updated = f(() => stateFromBytes(entity.bytes))
        val updatedEntity = entity.mutate(updated.toProtoByteArray)
        bytesWritten.update(updatedEntity.bytes.length)
        store.save(updatedEntity)
      case None =>
        val created = f(() => newState()).toProtoByteArray
        bytesWritten.update(created.length)
        store.create(prefix + key, created)
    }
    res.map { entity => stateFromBytes(entity.bytes) }
  }

  def expunge(key: String): Future[Boolean] = withLock(key) {
    store.delete(prefix + key) map { _ => true }
  }

  def names(): Future[Seq[String]] = {
    store.allIds().map {
      _.collect {
        case name if name startsWith prefix => name.replaceFirst(prefix, "")
      }
    }
  }

  private def stateFromBytes(bytes: Array[Byte]): S = {
    newState().mergeFromProto(bytes)
  }

  private def withLock[T](key: String)(future: => Future[T]): Future[T] = {
    val lock = locks.get(key)
    lock.acquire()
    val result = future
    result.onComplete { _ => lock.release() }
    result
  }
}
