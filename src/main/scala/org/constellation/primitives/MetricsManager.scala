package org.constellation.primitives

import akka.actor.Actor

import scala.collection.mutable

case object GetMetrics

case class UpdateMetric(key: String, value: String)

case class IncrementMetric(key: String)

class MetricsManager extends Actor {

  private val metrics = mutable.HashMap[String, String]()

  override def receive: Receive = {

    case GetMetrics => sender() ! metrics.toMap

    case UpdateMetric(key, value) => metrics(key) = value

    case IncrementMetric(key) =>
      metrics(key) = metrics.get(key).map{z => (z.toLong + 1).toString}.getOrElse("1")

  }
}
