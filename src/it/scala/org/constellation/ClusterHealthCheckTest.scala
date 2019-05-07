package org.constellation

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import org.constellation.crypto.KeyUtils
import org.constellation.util.{APIClient, HealthChecker, Metrics}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.{FiniteDuration, _}

/**
  *
  */
class ClusterHealthCheckTest
    extends TestKit(ActorSystem("ClusterHealthCheckTest"))
    with FlatSpecLike
    with BeforeAndAfterAll {

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  implicit val materialize: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  val (ignoreIPs, auxAPIs) = ComputeTestUtil.getAuxiliaryNodes()
  val apis: Seq[APIClient] = ComputeTestUtil.createApisFromIpFile(
    System.getenv().getOrDefault("HOSTS_FILE", "hosts-2.txt"),
    ignoreIPs
  )

  // For fixing some old bug, revisit later if necessary
  KeyUtils.makeKeyPair()

  "All nodes" should "continually increase total block created (not stall or remain the same) across a period" in {
    checkMetricIncreasing(
      Metrics.checkpointAccepted,
      ConfigUtil.getDurationFromConfig("constellation.it.block-creation.check-interval", 10 seconds),
      ConfigUtil.getOrElse("constellation.it.block-creation.check-retries", 5)
    )
  }

  "All nodes" should "continually increase total snapshots count" in {
    checkMetricIncreasing(Metrics.snapshotCount,
      ConfigUtil.getDurationFromConfig("constellation.it.snapshot-creation.check-interval",
                                     4 minutes),
      ConfigUtil.getOrElse("constellation.it.snapshot-creation.check-retries", 2)
    )
  }

  "All nodes" should "have valid metrics" in {
    HealthChecker.checkAllMetrics(apis).foreach(e => fail(e.getMessage))
  }

  private def checkMetricIncreasing(metricKey: String,
                                    delay: FiniteDuration,
                                    retries: Int): Unit = {
    var lastMetrics = apis.map(a => (a.baseURI, -1)).toMap
    runPeriodically(
      apis.foreach { a =>
        val id = a.id.short
        val currentMetricCount = a.metrics.getOrElse(metricKey, "0").toInt
        val isIncreasing = currentMetricCount > lastMetrics(id)
        if (!isIncreasing) {
          fail(
            s"Metric $metricKey did not increase after delay $delay for node: ${a.baseURI} and stopped at $currentMetricCount"
          )
        } else {
          lastMetrics = lastMetrics ++ Map(id -> currentMetricCount)
        }
        isIncreasing
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
