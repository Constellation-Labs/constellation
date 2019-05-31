/*
package org.constellation.storage

import cats.effect.{ContextShift, IO}
import org.constellation.primitives.Schema.Address
import org.constellation.primitives.concurrency.MultiLock
import org.scalamock.scalatest.MockFactory
import org.scalatest.WordSpec

import scala.concurrent.Future

// TODO: AddressService tests

class AddressServiceTest extends WordSpec with MockFactory {
  implicit val ioContextShift: ContextShift[IO] =
    IO.contextShift(scala.concurrent.ExecutionContext.Implicits.global)

  "AddressService transaction" when {
    "transfer amount from source to destination" should {
      "lock both source and destination" in {

//        val locks = new MultiLock[IO, String]()

        val addressService = new AddressService(50000)
        val a = Address("a")
        val b = Address("b")

        import scala.concurrent.ExecutionContext.Implicits.global

        val r = Future.sequence(List(
          addressService.transfer(a, b, 1L).unsafeToFuture(),
          addressService.transfer(a, b, 2L).unsafeToFuture()))

        r.foreach {
          case a :: b :: Nil =>
            println(a)
            println(b)
          case _ => ()
        }

        assert(???)
      }
    }
  }
}
 */
