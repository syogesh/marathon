package mesosphere.util.state.zk

import com.twitter.zk.ZkClient
import com.twitter.util.Await
import mesosphere.marathon.integration.setup.StartedZookeeper
import mesosphere.util.state.PersistentStoreTest
import org.apache.zookeeper.ZooDefs.Ids
import org.scalatest._

import scala.collection.JavaConverters._

class ZKStoreTest extends PersistentStoreTest with StartedZookeeper with Matchers {

  //
  // See PersistentStoreTests for general store tests
  //

  test("Create nested root path") {
    val store = new ZKStore(persistentStore.client, persistentStore.client("/some/nested/path"))
  }

  lazy val persistentStore: ZKStore = {
    implicit val timer = com.twitter.util.Timer.Nil
    val timeout = com.twitter.util.TimeConversions.intToTimeableNumber(10).minutes
    val client = ZkClient(config.zkHostAndPort, timeout).withAcl(Ids.OPEN_ACL_UNSAFE.asScala)
    new ZKStore(client, client(config.zkPath))
  }

  override protected def beforeAll(configMap: ConfigMap): Unit = {
    super.beforeAll(configMap + ("zkPort" -> "2185"))
  }

  override protected def afterAll(configMap: ConfigMap): Unit = {
    Await.ready(persistentStore.client.release())
    super.afterAll(configMap)
  }
}
