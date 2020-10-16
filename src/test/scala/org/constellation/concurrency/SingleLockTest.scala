package org.constellation.concurrency
import java.util.concurrent.atomic.AtomicInteger

import cats.effect.concurrent.Semaphore
import cats.effect.{ContextShift, IO}
import cats.syntax.all._
import org.constellation.ConstellationExecutionContext
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

class SingleLockTest extends AnyWordSpec with Matchers {
  "SingleLock" should {
    implicit val ec = ExecutionContext.global
    implicit val ioContextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
    implicit val timer = IO.timer(ExecutionContext.global)

    "not allow concurrent modifications of same resource" in {
      val items = scala.collection.mutable.Stack[Int](1, 2, 3)
      val processedItems = scala.collection.mutable.Stack[Int]()
      val op = IO {
        val item = items.pop
        processedItems.push(item)
        ()
      }
      val program = for {
        s <- Semaphore[IO](1)
        r1 = new SingleLock[IO, Unit]("R1", s).use(op)
        r2 = new SingleLock[IO, Unit]("R2", s).use(op)
        r3 = new SingleLock[IO, Unit]("R3", s).use(op)
        _ <- List(r1, r2, r3).parSequence.void
      } yield ()
      val res = Await.result(Future.sequence(List(program.unsafeToFuture())), 10.seconds)
      res.foreach { r =>
        assert(processedItems.distinct.size == 3)
      }
    }
    "Handle exceptions thrown" in {
      lazy val throwError = IO {
        throw new RuntimeException("throwError")
      }
      implicit val timer = IO.timer(ExecutionContext.global)
      val items = scala.collection.mutable.Stack[Int](1, 2, 3)
      val processedItems = scala.collection.mutable.Stack[Int]()
      val op = IO {
        val item = items.pop
        processedItems.push(item)
        ()
      }
      val program = for {
        s <- Semaphore[IO](1)
        r1 = new SingleLock[IO, Unit]("R1", s).use(op)
        r2 = new SingleLock[IO, Unit]("R2", s).use(throwError).handleErrorWith(_ => IO.unit)
        r3 = new SingleLock[IO, Unit]("R3", s).use(op)
        _ <- List(r1, r2, r3).parSequence.void
      } yield ()

      val res = Await.result(Future.sequence(List(program.unsafeToFuture())), 10.seconds)
      res.foreach { r =>
        assert(processedItems.distinct.size == 2)
      }
    }
  }
}
