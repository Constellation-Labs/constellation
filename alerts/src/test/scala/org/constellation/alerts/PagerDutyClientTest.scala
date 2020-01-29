package org.constellation.alerts

import java.util.concurrent.Executors

import cats.effect.{ContextShift, IO, Timer}
import cats.implicits._
import cats.instances._
import org.constellation.alerts.primitives.{JVMCPUUsageAlert, JVMHeapSizeAlert}
import org.mockito.IdiomaticMockito
import org.mockito.cats.IdiomaticMockitoCats
import org.scalatest.{BeforeAndAfter, FreeSpec, Matchers}

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

class PagerDutyClientTest extends FreeSpec
  with Matchers
  with BeforeAndAfter
  with IdiomaticMockito
  with IdiomaticMockitoCats {

  "PagerDutyClientTest" - {
    "should allow to enable simulating timeouts" in {
//      val ex = ExecutionContext.fromExecutor(Executors.newWorkStealingPool())
//      implicit val cs: ContextShift[IO] = IO.contextShift(ex)
//      implicit val timer: Timer[IO] = IO.timer(ex)
//
//      val client = new PagerDutyClient[IO]("34.94.119.200", "abc")
//
//      client.sendAlert(JVMCPUUsageAlert(99)).unsafeRunSync()
    }
  }
}
