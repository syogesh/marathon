package mesosphere.util.state.memory

import mesosphere.util.state.{ PersistentStore, PersistentStoreTest }
import org.scalatest.Matchers

class InMemoryStoreTest extends PersistentStoreTest with Matchers {

  //
  // See PersistentStoreTests for general store tests
  //

  lazy val persistentStore: PersistentStore = new InMemoryStore

}

