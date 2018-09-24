package org.constellation.primitives

import akka.actor.Actor
import com.typesafe.scalalogging.Logger
import org.constellation.primitives.Schema.InternalHeartbeat

case object GetMetrics

case class UpdateMetric(key: String, value: String)

case class IncrementMetric(key: String)

class MetricsManager extends Actor {

  var round = 0L

  val logger = Logger("Metrics")

  var lastCheckTime: Long = System.currentTimeMillis()
  var lastTXCount: Long = 0

  override def receive = active(Map.empty)

  def active(metrics: Map[String, String]): Receive = {

    case GetMetrics => sender() ! metrics

    case UpdateMetric(key, value) => context become active(metrics + (key -> value))

    case IncrementMetric(key) =>
      // Why are the values strings if we're just going to convert back and forth from longs?
      val updatedMap = metrics + (key -> metrics.get(key).map{z => (z.toLong + 1).toString}.getOrElse("1"))
      context become active(updatedMap)

    case InternalHeartbeat =>

      round += 1
      if (round % 10 == 0) {
        logger.info("Metrics: " + metrics)
        val count = metrics.getOrElse("transactionAccepted", "0").toLong
        val delta = System.currentTimeMillis() - lastCheckTime
        val deltaTX = count - lastTXCount
        val tps = deltaTX.toDouble * 1000 / delta
        lastTXCount = count
        lastCheckTime = System.currentTimeMillis()
        context become active(metrics + ("TPS" -> tps.toString))
      }

  }
}
