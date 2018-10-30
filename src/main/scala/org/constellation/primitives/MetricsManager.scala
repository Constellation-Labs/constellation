package org.constellation.primitives

import java.util.concurrent.TimeUnit

import akka.actor.Actor
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.scalalogging.Logger
import constellation._
import org.constellation.DAO
import org.constellation.primitives.Schema.{Id, InternalHeartbeat}
import org.constellation.util.HeartbeatSubscribe
import org.joda.time.DateTime

case object GetMetrics

case class UpdateMetric(key: String, value: String)

case class IncrementMetric(key: String)

class MetricsManager()(implicit dao: DAO) extends Actor {

  val logger = Logger("Metrics")

  var lastCheckTime: Long = System.currentTimeMillis()
  var lastTXCount: Long = 0
  implicit val timeout: Timeout = Timeout(15, TimeUnit.SECONDS)

  dao.heartbeatActor ! HeartbeatSubscribe

  override def receive: Receive = active(Map("id" -> dao.id.b58))

  def active(metrics: Map[String, String]): Receive = {

    case GetMetrics => sender() ! metrics

    case UpdateMetric(key, value) => context become active(metrics + (key -> value))

    case IncrementMetric(key) =>
      // Why are the values strings if we're just going to convert back and forth from longs?
      val updatedMap = metrics + (key -> metrics.get(key).map{z => (z.toLong + 1).toString}.getOrElse("1"))
      context become active(updatedMap)

    case InternalHeartbeat(round) =>

      if (round % dao.processingConfig.metricCheckInterval == 0) {

        val peers = (dao.peerManager ? GetPeerInfo).mapTo[Map[Id, PeerData]].get().toSeq

        val allAddresses = peers.map{_._1.address.address} :+ dao.selfAddressStr

        val balanceMetrics = allAddresses.map{a =>
          val balance = dao.dbActor.getAddressCacheData(a).map{_.balanceByLatestSnapshot}.getOrElse(0L)
          a.slice(0, 8) + " " + balance
        }.mkString(", ")


     //   logger.info("Metrics: " + metrics)
        val count = metrics.getOrElse("transactionAccepted", "0").toLong
        val startTime = metrics.getOrElse("nodeStartTimeMS", "1").toLong
        val delta = System.currentTimeMillis() - lastCheckTime
        val deltaStart = System.currentTimeMillis() - startTime
        val deltaTX = count - lastTXCount
        val tps = deltaTX.toDouble * 1000 / delta
        val tpsAll = count.toDouble * 1000 / deltaStart
        lastTXCount = count
        lastCheckTime = System.currentTimeMillis()
        context become active(
          metrics + (
            "TPS_last_" + dao.processingConfig.metricCheckInterval + "_seconds" -> tps.toString,
            "TPS_all" -> tpsAll.toString,
            "balances" -> balanceMetrics,
            "nodeCurrentTimeMS" -> System.currentTimeMillis().toString,
            "nodeCurrentDate" -> new DateTime().toString()
          )
        )
      }

  }
}
