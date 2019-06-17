package org.constellation

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import com.typesafe.scalalogging.Logger
import org.constellation.util.{APIClient, HealthChecker, Metrics}
import org.scalatest.{BeforeAndAfterAll, CancelAfterFailure, FlatSpecLike}
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.{FiniteDuration, _}

class ClusterHealthCheckTest
    extends TestKit(ActorSystem("ClusterHealthCheckTest"))
    with FlatSpecLike
    with BeforeAndAfterAll
    with CancelAfterFailure {

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  implicit val materialize: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  val logger = Logger(LoggerFactory.getLogger(getClass.getName))

  val (ignoreIPs, auxAPIs) = ComputeTestUtil.getAuxiliaryNodes()
  val apis: Seq[APIClient] = ComputeTestUtil.createApisFromIpFile(
    ignoreIPs
  )


  "All nodes" should "have valid metrics" in {
    HealthChecker.checkAllMetrics(apis) match {
      case Left(err) => fail(err.getMessage)
      case Right(_)  =>
    }
  }

  "All nodes" should "continually increase total block created (not stall or remain the same) across a period" in {
    assertMetricIncreasing(
      Metrics.checkpointAccepted,
      ConfigUtil.getDurationFromConfig("constellation.it.block-creation.check-interval",
                                       10 seconds),
      ConfigUtil.getOrElse("constellation.it.block-creation.check-retries", 5)
    )
  }

  "All nodes" should "continually increase total snapshots count" in {
    assertMetricIncreasing(
      Metrics.snapshotCount,
      ConfigUtil.getDurationFromConfig("constellation.it.snapshot-creation.check-interval",
                                       4 minutes),
      ConfigUtil.getOrElse("constellation.it.snapshot-creation.check-retries", 2)
    )
  }

  private def assertMetricIncreasing(metricKey: String,
                                    delay: FiniteDuration,
                                    retries: Int): Unit = {
    var lastMetrics = apis.map(a => (a.baseURI, -1)).toMap
    runPeriodically(
      apis.foreach { a =>
        val id = a.baseURI
        val currentMetricCount = a.metrics.getOrElse(metricKey, "0").toInt
        logger.info(s"Checking if metric: $metricKey is increasing on node ${a.baseURI}")
        val isIncreasing = currentMetricCount > lastMetrics(id)
        if (!isIncreasing) {
          fail(
            s"Metric $metricKey did not increase after delay $delay for node: ${a.baseURI} and stopped at $currentMetricCount"
          )
        } else {
          lastMetrics = lastMetrics ++ Map(id -> currentMetricCount)
        }
      },
      retries,
      delay
    )
  }

  private def runPeriodically(
    func: => Any,
    maxRetries: Int,
    delay: FiniteDuration
  ): Unit = {

    var retries = 0

    while (retries < maxRetries) {
      retries += 1
      func
      Thread.sleep(delay.toMillis)
    }
  }

}
