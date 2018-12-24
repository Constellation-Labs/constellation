package org.constellation.primitives

import java.io.File
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
import io.kontainers.micrometer.akka.AkkaMetricRegistry
import io.micrometer.prometheus.{PrometheusConfig, PrometheusMeterRegistry}
import io.micrometer.core.instrument.Clock
import io.micrometer.core.instrument.binder.jvm._
import io.micrometer.core.instrument.binder.system._
import io.micrometer.core.instrument.binder.logging._
import io.micrometer.core.instrument.binder.jvm
//import io.micrometer.core.instrument.binder.db._

import io.prometheus.client.CollectorRegistry

case object GetMetrics

case class UpdateMetric(key: String, value: String)

case class IncrementMetric(key: String)

class MetricsManager()(implicit dao: DAO) extends Actor {

  val logger = Logger("Metrics")

  var lastCheckTime: Long = System.currentTimeMillis()
  var lastTXCount: Long = 0
  implicit val timeout: Timeout = Timeout(15, TimeUnit.SECONDS)

  dao.heartbeatActor ! HeartbeatSubscribe

  //Create micrometer registry, tell it to gather jvm metrics
  val prometheusMeterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT, CollectorRegistry.defaultRegistry, Clock.SYSTEM)
  AkkaMetricRegistry.setRegistry(prometheusMeterRegistry)

  new JvmMemoryMetrics().bindTo(prometheusMeterRegistry)
  new JvmGcMetrics().bindTo(prometheusMeterRegistry)
  new JvmThreadMetrics().bindTo(prometheusMeterRegistry)
  new UptimeMetrics().bindTo(prometheusMeterRegistry)
  new ProcessorMetrics().bindTo(prometheusMeterRegistry)
  new FileDescriptorMetrics().bindTo(prometheusMeterRegistry)
  new LogbackMetrics().bindTo(prometheusMeterRegistry)
  new ClassLoaderMetrics()bindTo((prometheusMeterRegistry))
  new DiskSpaceMetrics(new File(System.getProperty("user.dir"))).bindTo(prometheusMeterRegistry);
  // new DatabaseTableMetrics().bindTo(prometheusMeterRegistry)


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

        val balancesBySnapshotMetrics = allAddresses.map{a =>
          val balance = dao.addressService.get(a).map{_.balanceByLatestSnapshot}.getOrElse(0L)
          a.slice(0, 8) + " " + balance
        }.sorted.mkString(", ")


        val balancesMetrics = allAddresses.map{a =>
          val balance = dao.addressService.get(a).map{_.balance}.getOrElse(0L)
          a.slice(0, 8) + " " + balance
        }.sorted.mkString(", ")


     //   logger.info("Metrics: " + metrics)
        val countAll = metrics.getOrElse("transactionAccepted", "0").toLong - dao.transactionAcceptedAfterDownload
        val startTime = dao.downloadFinishedTime // metrics.getOrElse("nodeStartTimeMS", "1").toLong
        val deltaStart = System.currentTimeMillis() - startTime
        val tpsAll = countAll.toDouble * 1000 / deltaStart

        val delta = System.currentTimeMillis() - lastCheckTime
        val deltaTX = countAll - lastTXCount
        val tps = deltaTX.toDouble * 1000 / delta
        lastTXCount = countAll
        lastCheckTime = System.currentTimeMillis()
        context become active(
          metrics + (
            "TPS_last_" + dao.processingConfig.metricCheckInterval + "_seconds" -> tps.toString,
            "TPS_all" -> tpsAll.toString,
            "balancesBySnapshot" -> balancesBySnapshotMetrics,
            "balances" -> balancesMetrics,
            "nodeCurrentTimeMS" -> System.currentTimeMillis().toString,
            "nodeCurrentDate" -> new DateTime().toString()
          )
        )
      }

  }
}
