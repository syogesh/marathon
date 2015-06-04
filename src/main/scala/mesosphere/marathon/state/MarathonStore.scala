package mesosphere.marathon.state

import com.codahale.metrics.{ MetricRegistry, Histogram }
import com.codahale.metrics.MetricRegistry._
import mesosphere.marathon.StoreCommandFailedException
import mesosphere.util.{ LockManager, ThreadPoolContext }
import mesosphere.util.state.PersistentStore
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.reflect.ClassTag
import scala.util.control.NonFatal

class MarathonStore[S <: MarathonState[_, S]](
    store: PersistentStore,
    registry: MetricRegistry,
    newState: () => S,
    prefix: String = "app:")(implicit ct: ClassTag[S]) extends EntityStore[S] {

  import ThreadPoolContext.context
  private[this] val log = LoggerFactory.getLogger(getClass)

  private[this] lazy val lockManager = LockManager.create()
  private[this] lazy val bytesRead: Histogram =
    registry.histogram(name(getClass, ct.getClass.getSimpleName, "read-data-size"))
  private[this] lazy val bytesWritten: Histogram =
    registry.histogram(name(getClass, ct.getClass.getSimpleName, "write-data-size"))

  def fetch(key: String): Future[Option[S]] = {
    store.load(prefix + key)
      .map {
        _.map { entity =>
          bytesRead.update(entity.bytes.length)
          stateFromBytes(entity.bytes)
        }
      }
      .recover(exceptionTransform(s"Could not fetch ${ct.runtimeClass.getSimpleName} with key: $key"))
  }

  def modify(key: String)(f: Update): Future[S] = lockManager.executeSequential(key) {
    val res = store.load(prefix + key).flatMap {
      case Some(entity) =>
        bytesRead.update(entity.bytes.length)
        val updated = f(() => stateFromBytes(entity.bytes))
        val updatedEntity = entity.mutate(updated.toProtoByteArray)
        bytesWritten.update(updatedEntity.bytes.length)
        store.update(updatedEntity)
      case None =>
        val created = f(() => newState()).toProtoByteArray
        bytesWritten.update(created.length)
        store.create(prefix + key, created)
    }
    res
      .map { entity => stateFromBytes(entity.bytes) }
      .recover(exceptionTransform(s"Could not modify ${ct.runtimeClass.getSimpleName} with key: $key"))
  }

  def expunge(key: String): Future[Boolean] = lockManager.executeSequential(key) {
    store.delete(prefix + key)
      .recover(exceptionTransform(s"Could not expunge ${ct.runtimeClass.getSimpleName} with key: $key"))
  }

  def names(): Future[Seq[String]] = {
    store.allIds()
      .map {
        _.collect {
          case name if name startsWith prefix => name.replaceFirst(prefix, "")
        }
      }
      .recover(exceptionTransform(s"Could not list names for ${ct.runtimeClass.getSimpleName}"))
  }

  private[this] def exceptionTransform[T](errorMessage: String): PartialFunction[Throwable, T] = {
    case NonFatal(ex) => throw new StoreCommandFailedException(errorMessage, ex)
  }

  private def stateFromBytes(bytes: Array[Byte]): S = {
    newState().mergeFromProto(bytes)
  }
}
