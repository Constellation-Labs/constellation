package org.constellation.primitives.concurrency

import java.util.concurrent.atomic.AtomicInteger

import cats.effect.{ContextShift, IO}
import org.scalatest.{Matchers, WordSpec}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class MultiLockTest extends WordSpec with Matchers {
  "MultiLock" should {
    "not allow concurrent modifications on same keys" in {
      implicit val ioContextShift: ContextShift[IO] =
        IO.contextShift(scala.concurrent.ExecutionContext.Implicits.global)

      val counter = new AtomicInteger()
      counter.set(0)

      val lock = new MultiLock[IO, String]()

      val t1 = lock.acquire(List("a", "b")) {
        IO {
          Thread.sleep(2000)
          counter.incrementAndGet()
        }
      }

      val t2 = lock.acquire(List("b", "c")) {
        IO {
          counter.incrementAndGet()
        }
      }

      val r = Future.sequence(List(t1.unsafeToFuture(), t2.unsafeToFuture()))

      r.foreach {
        case a :: b :: Nil =>
          assert(a == 1)
          assert(b == 2)
        case _ => ()
      }
    }
  }
}
