package org.constellation.util

import java.util.concurrent._

import akka.util.Timeout
import better.files.File
import com.typesafe.scalalogging.Logger
import io.kontainers.micrometer.akka.AkkaMetricRegistry
import io.micrometer.core.instrument.Clock
import io.micrometer.core.instrument.binder.jvm._
import io.micrometer.core.instrument.binder.logging.LogbackMetrics
import io.micrometer.core.instrument.binder.system.{FileDescriptorMetrics, ProcessorMetrics, UptimeMetrics}
import io.micrometer.prometheus.{PrometheusConfig, PrometheusMeterRegistry}
import io.prometheus.client.CollectorRegistry
import constellation._
import org.constellation.DAO
import org.constellation.primitives.PeerData
import org.constellation.primitives.Schema.Id
import org.joda.time.DateTime

import scala.collection.concurrent.TrieMap

object Metrics {

  def prometheusSetup(keyHash: String): Unit = {
    val prometheusMeterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT, CollectorRegistry.defaultRegistry, Clock.SYSTEM)
    prometheusMeterRegistry.config().commonTags("application", s"Constellation_$keyHash")
    AkkaMetricRegistry.setRegistry(prometheusMeterRegistry)
    new JvmMemoryMetrics().bindTo(prometheusMeterRegistry)
    new JvmGcMetrics().bindTo(prometheusMeterRegistry)
    new JvmThreadMetrics().bindTo(prometheusMeterRegistry)
    new UptimeMetrics().bindTo(prometheusMeterRegistry)
    new ProcessorMetrics().bindTo(prometheusMeterRegistry)
    new FileDescriptorMetrics().bindTo(prometheusMeterRegistry)
    new LogbackMetrics().bindTo(prometheusMeterRegistry)
    new ClassLoaderMetrics().bindTo(prometheusMeterRegistry)
    new DiskSpaceMetrics(File(System.getProperty("user.dir")).toJava).bindTo(prometheusMeterRegistry)
    // new DatabaseTableMetrics().bindTo(prometheusMeterRegistry)
  }

}


class TransactionRateTracker()(implicit dao: DAO){

  private var lastTXCount: Long = 0
  private var lastCheckTime: Long = System.currentTimeMillis()

  def getMetrics(transactionAccepted: Long): Map[String, String] = {
    val countAll = transactionAccepted - dao.transactionAcceptedAfterDownload
    val startTime = dao.downloadFinishedTime // metrics.getOrElse("nodeStartTimeMS", "1").toLong
    val deltaStart = System.currentTimeMillis() - startTime
    val tpsAll = countAll.toDouble * 1000 / deltaStart
    val delta = System.currentTimeMillis() - lastCheckTime
    val deltaTX = countAll - lastTXCount
    val tps = deltaTX.toDouble * 1000 / delta
    lastTXCount = countAll
    lastCheckTime = System.currentTimeMillis()
    Map(
      "TPS_last_" + dao.processingConfig.metricCheckInterval + "_seconds" -> tps.toString,
      "TPS_all" -> tpsAll.toString
    )
  }

}

class Metrics(periodSeconds: Int = 1)(implicit dao: DAO) {

  val logger = Logger("Metrics")

  private val executor = new ScheduledThreadPoolExecutor(1)

  private var round: Long = 0L

  private val stringMetrics : TrieMap[String, String] = TrieMap()
  private val countMetrics : TrieMap[String, Long] = TrieMap()

  val rateCounter = new TransactionRateTracker()

  updateMetric("id", dao.id.b58)

  def updateMetric(key: String, value: String): Unit = {
    stringMetrics(key) = value
  }

  def updateMetric(key: String, value: Int): Unit = {
    countMetrics(key) = value
  }

  def incrementMetric(key: String): Unit = {
    countMetrics(key) = countMetrics.getOrElse(key, 0L) + 1
  }

  Metrics.prometheusSetup(dao.keyPair.getPublic.hash)

  def checkInterval[T](f: => T): Unit = {
    if (round % dao.processingConfig.metricCheckInterval == 0) {
      f
    }
  }

  // Temporary, for debugging only. Would cause a problem with many peers
  def updateBalanceMetrics(): Unit = {

    val peers = dao.peerInfo.toSeq

    val allAddresses = peers.map{_._1.address.address} :+ dao.selfAddressStr

    val balancesBySnapshotMetrics = allAddresses.map{a =>
      val balance = dao.addressService.get(a).map{_.balanceByLatestSnapshot}.getOrElse(0L)
      a.slice(0, 8) + " " + balance
    }.sorted.mkString(", ")

    val balancesMetrics = allAddresses.map{a =>
      val balance = dao.addressService.get(a).map{_.balance}.getOrElse(0L)
      a.slice(0, 8) + " " + balance
    }.sorted.mkString(", ")

    updateMetric("balancesBySnapshot", balancesBySnapshotMetrics)
    updateMetric("balances", balancesMetrics)

  }

  private val task = new Runnable {
    def run(): Unit = {
      round += 1
      checkInterval{
        Thread.currentThread().setName("Metrics")
        updateBalanceMetrics()
        rateCounter.getMetrics(countMetrics.getOrElse("transactionAccepted", 0L)).foreach{
          case (k,v) => updateMetric(k,v)
        }

        updateMetric("nodeCurrentTimeMS", System.currentTimeMillis().toString)
        updateMetric("nodeCurrentDate", new DateTime().toString())

      }
    }
  }

  private val scheduledFuture = executor.scheduleAtFixedRate(task, 1, periodSeconds, TimeUnit.SECONDS)

  def shutdown(): Boolean = scheduledFuture.cancel(false)

}