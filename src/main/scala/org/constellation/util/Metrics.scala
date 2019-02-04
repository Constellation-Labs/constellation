package org.constellation.util

import better.files.File
import com.typesafe.scalalogging.Logger
import io.kontainers.micrometer.akka.AkkaMetricRegistry
import io.micrometer.core.instrument.Clock
import io.micrometer.core.instrument.binder.jvm._
import io.micrometer.core.instrument.binder.logging.LogbackMetrics
import io.micrometer.core.instrument.binder.system.{FileDescriptorMetrics, ProcessorMetrics, UptimeMetrics}
import io.micrometer.prometheus.{PrometheusConfig, PrometheusMeterRegistry}
import io.prometheus.client.CollectorRegistry
import org.joda.time.DateTime
import scala.collection.concurrent.TrieMap
import scala.concurrent.Future

import constellation._
import org.constellation.{ConstellationNode, DAO}

/** For Grafana usage. */
object Metrics {

  /** Documentation. */
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

/**
  * TPS reports
  * @param dao: Data access object
  */
class TransactionRateTracker()(implicit dao: DAO){

  private var lastTXCount: Long = 0
  private var lastCheckTime: Long = System.currentTimeMillis()

  /**
    * Determine transaction per second (TPS) rates for last N seconds and all time
    * @param transactionAccepted: Current number of transactions accepted from stored metrics
    * @return TPS all time and TPS last n seconds in metrics form
    */
  def calculate(transactionAccepted: Long): Map[String, String] = {
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
      "TPS_last_" + dao.nodeConfig.metricIntervalSeconds + "_seconds" -> tps.toString,
      "TPS_all" -> tpsAll.toString
    )
  }

}

/**
  * TrieMap backed metrics store for string values and counters
  * @param periodSeconds: How often to recalculate moving window metrics (e.g. TPS)
  * @param dao: Data access object
  */
class Metrics(periodSeconds: Int = 1)(implicit dao: DAO)
  extends Periodic("Metrics", periodSeconds) {

  val logger = Logger("Metrics")

  private val stringMetrics : TrieMap[String, String] = TrieMap()
  private val countMetrics : TrieMap[String, Long] = TrieMap()

  val rateCounter = new TransactionRateTracker()

  // Init
  updateMetric("id", dao.id.hex)
  Metrics.prometheusSetup(dao.keyPair.getPublic.hash)
  updateMetric("nodeState", dao.nodeState.toString)
  updateMetric("address", dao.selfAddressStr)
  updateMetric("nodeStartTimeMS", System.currentTimeMillis().toString)
  updateMetric("nodeStartDate", new DateTime(System.currentTimeMillis()).toString)
  updateMetric("externalHost", dao.externalHostString)
  updateMetric("version", ConstellationNode.ConstellationVersion)

  /** Documentation. */
  def updateMetric(key: String, value: String): Unit = {
    stringMetrics(key) = value
  }

  /** Documentation. */
  def updateMetric(key: String, value: Int): Unit = {
    countMetrics(key) = value
  }

  /** Documentation. */
  def incrementMetric(key: String): Unit = {
    countMetrics(key) = countMetrics.getOrElse(key, 0L) + 1
  }

  /**
    * Converts counter metrics to string for export / display
    * @return : Key value map of all metrics
    */
  def getMetrics: Map[String, String] = {
    stringMetrics.toMap ++ countMetrics.toMap.mapValues(_.toString)
  }

  // Temporary, for debugging only. Would cause a problem with many peers

  /** Documentation. */
  def updateBalanceMetrics(): Unit = {

    val peers = dao.peerInfo.toSeq

    val allAddresses = peers.map{_._1.address} :+ dao.selfAddressStr

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

  /**
    * Recalculates window based / periodic metrics
    */
  override def trigger(): concurrent.Future[Any] = Future {
    updateBalanceMetrics()
    rateCounter.calculate(countMetrics.getOrElse("transactionAccepted", 0L)).foreach {
      case (k, v) => updateMetric(k, v)
    }
    updateMetric("nodeCurrentTimeMS", System.currentTimeMillis().toString)
    updateMetric("nodeCurrentDate", new DateTime().toString())
    updateMetric("metricsRound", round.toString)
  }(scala.concurrent.ExecutionContext.global)

}
