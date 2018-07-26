package org.constellation.primitives

import akka.actor.Actor

case object GetMetrics

case class UpdateMetric(key: String, value: String)

case class IncrementMetric(key: String)

class MetricsManager extends Actor {

  override def receive = active(Map.empty)

  def active(metrics: Map[String, String]): Receive = {

    case GetMetrics => sender() ! metrics

    case UpdateMetric(key, value) => context become active(metrics + (key -> value))

    case IncrementMetric(key) =>
      // Why are the values strings if we're just going to convert back and forth from longs?
      val updatedMap = metrics + (key -> metrics.get(key).map{z => (z.toLong + 1).toString}.getOrElse("1"))
      context become active(updatedMap)

  }
}
