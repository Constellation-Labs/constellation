
import scala.concurrent.{Await, Future}

/**
  * Project wide convenience functions.
  */
package object constellation {

  implicit class EasyFutureBlock[T](f: Future[T]) {
    def get(t: Int = 5): T = {
      import scala.concurrent.duration._
      Await.result(f, t.seconds)
    }
  }

}
