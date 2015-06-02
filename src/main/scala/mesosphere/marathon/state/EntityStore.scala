package mesosphere.marathon.state

import scala.concurrent.Future

trait EntityStore[T] {

  type Deserialize = () => T //returns deserialized T value.
  type Update = Deserialize => T //Update function Gets an Read and returns the (modified) T

  def fetch(key: String): Future[Option[T]]

  def store(key: String, value: T): Future[T] = modify(key)(_ => value)

  def modify(key: String)(update: Update): Future[T]

  def expunge(key: String): Future[Boolean]

  def names(): Future[Seq[String]]

}
