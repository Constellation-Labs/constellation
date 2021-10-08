package org.constellation.trust

import cats.effect.{ConcurrentEffect, ContextShift, IO}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.ExecutionContext

class WalkTest extends AnyFreeSpec with Matchers {
  implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  implicit val effect: ConcurrentEffect[IO] = IO.ioConcurrentEffect(contextShift)
  val generator = new DataGenerator[IO]()
  "Should not create a stack overflow" in {

    /*
    Doesn't seem to re-create issue even with 10k+ nodes
    Must be related to underlying data distribution. Maybe need a fully connected
    graph to reproduce?
     */
    val data = generator.generateData(1000).unsafeRunSync()
    val res = SelfAvoidingWalk.runWalkBatchesFeedback(0, data)
//    println(res.toList)
  }
}
