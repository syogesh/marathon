package mesosphere.util.state.zk

import java.util.UUID

import com.fasterxml.uuid.impl.UUIDUtil
import com.google.protobuf.{ InvalidProtocolBufferException, ByteString }
import com.twitter.util.{ Future => TWFuture }
import com.twitter.zk.{ ZNode, ZkClient }
import mesosphere.marathon.{ Protos, StoreCommandFailedException }
import mesosphere.util.ThreadPoolContext
import mesosphere.util.state.zk.ZKStore._
import mesosphere.util.state.{ PersistentEntity, PersistentStore }
import org.apache.log4j.Logger
import org.apache.zookeeper.KeeperException
import org.apache.zookeeper.KeeperException.NoNodeException

import scala.concurrent.duration.Duration
import scala.concurrent.{ Await, Future, Promise }

class ZKStore(val client: ZkClient, rootNode: ZNode) extends PersistentStore {

  private[this] val log = Logger.getLogger(getClass)
  private[this] implicit val ec = ThreadPoolContext.context

  val root = createPath(rootNode)

  /**
    * Fetch data and return entity.
    * The entity is returned also if it is not found in zk, since it is needed for the store operation.
    */
  override def load(key: ID): Future[Option[ZKEntity]] = {
    val node = root(key)
    require(node.parent == root, "Nested paths are not supported!")
    node.getData().asScala
      .map{ data => Some(ZKEntity(node, ZKData(data.bytes), Some(data.stat.getVersion))) }
      .recover { case ex: NoNodeException => None }
      .recover(exceptionTransform(s"Could not load key $key"))
  }

  override def create(key: ID, content: Array[Byte]): Future[ZKEntity] = {
    val node = root(key)
    require(node.parent == root, "Nested paths are not supported!")
    val data = ZKData(key, UUID.randomUUID(), content)
    node.create(data.toProto.toByteArray).asScala
      .map(n => ZKEntity(n, data, Some(0))) //first version after create is 0
      .recover(exceptionTransform(s"Can not create entity $key"))
  }

  /**
    * This will store a previously fetched entity.
    * The entity will be either created or updated, depending on the read state.
    * @return Some value, if the store operation is successful otherwise None
    */
  override def save(entity: PersistentEntity): Future[ZKEntity] = {
    val zk = zkEntity(entity)
    def update(version: Int): Future[ZKEntity] = {
      zk.node.setData(zk.data.toProto.toByteArray, version).asScala
        .map { data => zk.copy(version = Some(data.stat.getVersion)) }
        .recover(exceptionTransform(s"Can not update entity $entity"))
    }
    def create(): Future[ZKEntity] = {
      zk.node.create(zk.data.toProto.toByteArray).asScala
        .flatMap { _.getData().map{ data => zk.copy(version = Some(data.stat.getVersion)) }.asScala }
        .recover(exceptionTransform(s"Can not update entity $entity"))
    }
    zk.version.map(update).getOrElse(create())
  }

  /**
    * Delete an entry with given identifier.
    */
  override def delete(key: ID): Future[ZKEntity] = {
    val node = root(key)
    require(node.parent == root, "Nested paths are not supported!")
    node.exists().asScala
      .flatMap { d => node.delete(d.stat.getVersion).asScala.map(n => ZKEntity(n, ZKData(key, UUID.randomUUID()))) }
      .recover(exceptionTransform(s"Can not delete entity $key"))
  }

  override def allIds(): Future[Seq[ID]] = {
    root.getChildren().asScala
      .map(_.children.map(_.name))
      .recover(exceptionTransform("Can not list all identifiers"))
  }

  private def exceptionTransform[T](errorMessage: String): PartialFunction[Throwable, T] = {
    case ex: KeeperException => throw new StoreCommandFailedException(errorMessage, ex)
    case ex: Throwable       => throw ex
  }

  private def zkEntity(entity: PersistentEntity): ZKEntity = {
    entity match {
      case zk: ZKEntity => zk
      case _            => throw new IllegalArgumentException("Can not handle this kind of entity")
    }
  }

  private def createPath(path: ZNode): ZNode = {
    def createParent(node: ZNode): ZNode = {
      val exists = Await.result(node.exists().asScala.map(_ => true)
        .recover { case ex: NoNodeException => false }
        .recover(exceptionTransform("Can not query for exists")), Duration.Inf)

      if (!exists) {
        createParent(node.parent)
        Await.result(node.create().asScala.recover(exceptionTransform("Can not create")), Duration.Inf)
      }
      node
    }
    createParent(path)
  }
}

case class ZKEntity(node: ZNode, data: ZKData, version: Option[Int] = None) extends PersistentEntity {
  override def id: String = node.name
  override def mutate(updated: Array[Byte]): PersistentEntity = copy(data = data.copy(bytes = updated))
  override def bytes: Array[Byte] = data.bytes
}

case class ZKData(name: String, uuid: UUID, bytes: Array[Byte] = Array.empty) {
  def toProto: Protos.ZKStoreEntry = Protos.ZKStoreEntry.newBuilder()
    .setName(name)
    .setUuid(ByteString.copyFromUtf8(uuid.toString))
    .setValue(ByteString.copyFrom(bytes))
    .build()
}
object ZKData {
  def apply(bytes: Array[Byte]): ZKData = {
    try {
      val proto = Protos.ZKStoreEntry.parseFrom(bytes)
      new ZKData(proto.getName, UUIDUtil.uuid(proto.getUuid.toByteArray), proto.getValue.toByteArray)
    }
    catch {
      case ex: InvalidProtocolBufferException =>
        throw new StoreCommandFailedException(s"Can not deserialize Protobuf from ${bytes.length}", ex)
    }
  }
}

object ZKStore {
  implicit class Twitter2Scala[T](val twitterF: TWFuture[T]) extends AnyVal {
    def asScala: Future[T] = {
      val promise = Promise[T]()
      twitterF.onSuccess(promise.success(_))
      twitterF.onFailure(promise.failure(_))
      promise.future
    }
  }
}
