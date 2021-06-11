package org.constellation.util

import java.util.concurrent.atomic.AtomicLong

import better.files.File
import cats.effect.{IO, Sync}
import cats.syntax.all._
import com.google.common.util.concurrent.AtomicDouble
import constellation._
import io.micrometer.core.instrument.Metrics.globalRegistry
import io.micrometer.core.instrument.binder.jvm._
import io.micrometer.core.instrument.binder.logging.LogbackMetrics
import io.micrometer.core.instrument.binder.system.{FileDescriptorMetrics, ProcessorMetrics, UptimeMetrics}
import io.micrometer.core.instrument.{Clock, Tag, Timer}
import io.micrometer.prometheus.{PrometheusConfig, PrometheusMeterRegistry}
import io.prometheus.client.CollectorRegistry
import io.prometheus.client.cache.caffeine.CacheMetricsCollector
import org.constellation.{BuildInfo, DAO}
import org.joda.time.DateTime

import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

/** For Grafana usage. */
object Metrics {

  val snapshotAttempt = "snapshotAttempt"
  val reDownloadFinished = "reDownloadFinished"
  val reDownloadError = "reDownloadError"
  val snapshotWriteToDisk = "snapshotWriteToDisk"
  val checkpointAccepted = "checkpointAccepted"
  val snapshotCount = "snapshotCount"
  val lastSnapshotHash = "lastSnapshotHash"
  val heightEmpty = "heightEmpty"
  val heightBelow = "heightBelow"
  val checkpointValidationFailure = "checkpointValidationFailure"
  val batchTransactionsEndpoint = "batchTransactionsEndpoint"
  val batchObservationsEndpoint = "batchObservationsEndpoint"
  val blacklistedAddressesSize = "blacklistedAddressesSize"

  val success = "_success"
  val failure = "_failure"

  val cacheMetrics = new CacheMetricsCollector()
  var isCacheRegistered = false

  def prometheusSetup(keyHash: String, collectorRegistry: CollectorRegistry): PrometheusMeterRegistry = {
    val prometheusMeterRegistry =
      new PrometheusMeterRegistry(PrometheusConfig.DEFAULT, collectorRegistry, Clock.SYSTEM)

    if (!isCacheRegistered) { // TODO: use cacheMetrics as a dependency
      collectorRegistry.register(cacheMetrics)
      isCacheRegistered = true
    }
    prometheusMeterRegistry.config().commonTags("application", "Constellation")
    globalRegistry.add(prometheusMeterRegistry)
    prometheusMeterRegistry.config().commonTags("application", s"Constellation_$keyHash")
    io.micrometer.core.instrument.Metrics.globalRegistry.add(prometheusMeterRegistry)

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

    prometheusMeterRegistry
  }

}

/**
  * TPS reports
  * @param dao: Data access object
  */
class TransactionRateTracker()(implicit dao: DAO) {

  private var lastTXCount: Long = 0
  private var lastCheckTime: Long = System.currentTimeMillis()

  /**
    * Determine transaction per second (TPS) rates for last N seconds and all time
    * @param transactionAccepted: Current number of transactions accepted from stored metrics
    * @return TPS all time and TPS last n seconds in metrics form
    */
  def calculate(transactionAccepted: Long): Map[String, Double] = {
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
      "TPS_last_" + dao.nodeConfig.processingConfig.metricCheckInterval + "_seconds" -> tps,
      "TPS_all" -> tpsAll
    )
  }

}

/**
  * TrieMap backed metrics store for string values and counters
  * @param periodSeconds: How often to recalculate moving window metrics (e.g. TPS)
  * @param dao: Data access object
  */
class Metrics(
  val collectorRegistry: CollectorRegistry,
  periodSeconds: Int = 1,
  unboundedExecutionContext: ExecutionContext
)(implicit dao: DAO)
    extends PeriodicIO("Metrics", unboundedExecutionContext) {

  type TagSeq = Seq[(String, String)]

  private val stringMetrics: TrieMap[(String, TagSeq), String] = TrieMap()
  private val longMetrics: TrieMap[(String, TagSeq), AtomicLong] = TrieMap()
  private val doubleMetrics: TrieMap[(String, TagSeq), AtomicDouble] = TrieMap()

  val rateCounter = new TransactionRateTracker()

  // Init

  val registry = Metrics.prometheusSetup(dao.keyPair.getPublic.hash, collectorRegistry)

  val init = for {
    currentTime <- cats.effect.Clock[IO].realTime(MILLISECONDS)
    _ <- updateMetricAsync[IO]("id", dao.id.hex)
    _ <- updateMetricAsync[IO]("alias", dao.alias.getOrElse(dao.id.short))
    _ <- updateMetricAsync[IO]("address", dao.selfAddressStr)
    _ <- updateMetricAsync[IO]("nodeStartTimeMS", currentTime.toString)
    _ <- updateMetricAsync[IO]("nodeStartDate", new DateTime(currentTime).toString)
    _ <- updateMetricAsync[IO]("externalHost", dao.externalHostString)
    _ <- updateMetricAsync[IO]("version", BuildInfo.version)
  } yield ()

  init.unsafeRunSync

  private def gaugedAtomicLong(key: String, tags: TagSeq): AtomicLong =
    registry.gauge(s"dag_$key", tags.toMicrometer(key), new AtomicLong(0L))

  // Note: AtomicDouble comes from guava
  private def gaugedAtomicDouble(key: String, tags: TagSeq): AtomicDouble =
    registry.gauge(s"dag_$key", tags.toMicrometer(key), new AtomicDouble(0d))

  def updateMetric(key: String, value: Double): Unit =
    updateMetric(key, value, Seq.empty)

  def updateMetric(key: String, value: Double, tags: TagSeq): Unit =
    doubleMetrics.getOrElseUpdate((key, tags), gaugedAtomicDouble(key, tags)).set(value)

  def updateMetric(key: String, value: String): Unit =
    updateMetric(key, value, Seq.empty)

  def updateMetric(key: String, value: String, tags: TagSeq): Unit =
    stringMetrics((key, tags)) = value

  def updateMetric(key: String, value: Int): Unit =
    updateMetric(key, value, Seq.empty)

  def updateMetric(key: String, value: Int, tags: TagSeq): Unit =
    updateMetric(key, value.toLong, tags)

  def updateMetric(key: String, value: Long): Unit =
    updateMetric(key, value, Seq.empty)

  def updateMetric(key: String, value: Long, tags: TagSeq): Unit =
    longMetrics.getOrElseUpdate((key, tags), gaugedAtomicLong(key, tags)).set(value)

  def incrementMetric(key: String): Unit = incrementMetric(key, Seq.empty)

  def incrementMetric(key: String, tags: TagSeq): Unit = {
    registry.counter(s"dag_$key", tags.toMicrometer(key)).increment()
    longMetrics.getOrElseUpdate((key, tags), new AtomicLong(0)).incrementAndGet()
  }

  def startTimer: Timer.Sample = Timer.start()

  def stopTimer(key: String, timer: Timer.Sample): Unit =
    timer.stop(Timer.builder(key).register(registry))

  def updateMetricAsync[F[_]: Sync](key: String, value: String): F[Unit] =
    Sync[F].delay(updateMetric(key, value))

  def updateMetricAsync[F[_]: Sync](key: String, value: Double, tags: TagSeq): F[Unit] =
    Sync[F].delay(updateMetric(key, value, tags))

  def updateMetricAsync[F[_]: Sync](key: String, value: Double): F[Unit] =
    updateMetricAsync(key, value, Seq.empty)

  def updateMetricAsync[F[_]: Sync](key: String, value: Int, tags: TagSeq): F[Unit] =
    Sync[F].delay(updateMetric(key, value, tags))

  def updateMetricAsync[F[_]: Sync](key: String, value: Int): F[Unit] =
    updateMetricAsync(key, value, Seq.empty)

  def updateMetricAsync[F[_]: Sync](key: String, value: Long, tags: TagSeq): F[Unit] =
    Sync[F].delay(updateMetric(key, value, tags))

  def updateMetricAsync[F[_]: Sync](key: String, value: Long): F[Unit] =
    updateMetricAsync(key, value, Seq.empty)

  def incrementMetricAsync[F[_]: Sync](key: String): F[Unit] = incrementMetricAsync(key, Seq.empty)

  def incrementMetricAsync[F[_]: Sync](key: String, tags: TagSeq): F[Unit] = Sync[F].delay(incrementMetric(key, tags))

  def incrementMetricAsync[F[_]: Sync](key: String, either: Either[Any, Any]): F[Unit] =
    incrementMetricAsync(key, either, Seq.empty)

  def incrementMetricAsync[F[_]: Sync](key: String, either: Either[Any, Any], tags: TagSeq): F[Unit] =
    Sync[F].delay(either match {
      case Left(_)  => incrementMetric(key + Metrics.failure, tags)
      case Right(_) => incrementMetric(key + Metrics.success, tags)
    })

  /**
    * Converts counter metrics to string for export / display
    *
    * @return : Key value map of all metrics
    */
  def getSimpleMetrics: Map[String, String] =
    formatSimpleMetrics(stringMetrics) ++
      formatSimpleMetrics(longMetrics) ++
      formatSimpleMetrics(doubleMetrics)

  def getTaggedMetrics: Map[String, Map[String, String]] =
    formatTaggedMetric(stringMetrics) ++
      formatTaggedMetric(longMetrics) ++
      formatTaggedMetric(doubleMetrics)

  private def formatSimpleMetrics[T](metrics: TrieMap[(String, TagSeq), T]): Map[String, String] =
    metrics.toList.filter { case ((_, tags), _) => tags.isEmpty }.map {
      case ((k, _), v) => (k, v.toString)
    }.toMap

  private def formatTaggedMetric[T](metrics: TrieMap[(String, TagSeq), T]): Map[String, Map[String, String]] =
    metrics.toList.filter { case ((_, tags), _) => tags.nonEmpty }.groupBy { case ((k, _), _) => k }.map {
      case (k, metrics) =>
        (k, metrics.map {
          case ((_, tags), v) =>
            (tags.map(t => s"${t._1}=${t._2}").mkString(","), v.toString)
        }.toMap)
    }

  def getCountMetric(key: String): Option[Long] =
    longMetrics.get((key, Seq.empty)).map(_.get())

  def getCountMetric(key: String, tags: Seq[(String, String)]): Option[Long] =
    longMetrics.get((key, tags)).map(_.get())

  // Temporary, for debugging only. Would cause a problem with many peers
  def updateBalanceMetrics(): IO[Unit] =
    for {
      peers <- dao.peerInfo
      allAddresses = peers.map(_._1.address).toSeq :+ dao.selfAddressStr

      balancesBySnapshot <- balancesBySnapshotMetrics(allAddresses)
      balances <- balancesMetrics(allAddresses)

      _ <- updateMetricAsync[IO]("balancesBySnapshot", balancesBySnapshot.sorted.mkString(", "))
      _ <- updateMetricAsync[IO]("balancesBySnapshot", balances.sorted.mkString(", "))
    } yield ()

  private def balancesBySnapshotMetrics(allAddresses: Seq[String]) =
    allAddresses.toList.traverse { address =>
      dao.addressService
        .lookup(address)
        .map(_.map(_.balanceByLatestSnapshot).getOrElse(0L))
        .map(address.slice(0, 8) + " " + _)
    }

  private def balancesMetrics(allAddresses: Seq[String]) =
    allAddresses.toList.traverse { address =>
      dao.addressService
        .lookup(address)
        .map(_.map(_.balance).getOrElse(0L))
        .map(address.slice(0, 8) + " " + _)
    }

  private def updateTransactionAcceptedMetrics(): IO[Unit] =
    IO {
      rateCounter.calculate(
        longMetrics
          .get(("transactionAccepted", Seq.empty))
          .map {
            _.get()
          }
          .getOrElse(0L)
      )
    }.map(_.toList)
      .map(_.map { case (k, v) => updateMetricAsync[IO](k, v) })
      .flatMap(_.sequence)
      .void

  private def updateTransactionServiceMetrics(): IO[Unit] =
    dao.transactionService.getMetricsMap
      .map(_.toList.map { case (k, v) => updateMetricAsync[IO](s"transactionService_${k}_size", v) })
      .flatMap(_.sequence)
      .void

  private def updateObservationServiceMetrics(): IO[Unit] =
    dao.observationService.getMetricsMap
      .map(_.toList.map { case (k, v) => updateMetricAsync[IO](s"observationService_${k}_size", v) })
      .flatMap(_.sequence)
      .void

  private def updateBlacklistedAddressesMetrics(): IO[Unit] =
    dao.blacklistedAddresses.get.flatMap(b => updateMetricAsync[IO](Metrics.blacklistedAddressesSize, b.size))

  private def updatePeriodicMetrics(): IO[Unit] =
    updateBalanceMetrics >>
      updateBlacklistedAddressesMetrics() >>
      updateTransactionAcceptedMetrics() >>
      updateObservationServiceMetrics() >>
      updateMetricAsync[IO]("nodeCurrentTimeMS", System.currentTimeMillis().toString) >>
      updateMetricAsync[IO]("nodeCurrentDate", new DateTime().toString()) >>
      updateMetricAsync[IO]("metricsRound", executionNumber.get()) >>
      dao.addressService.size.flatMap(size => updateMetricAsync[IO]("addressCount", size)) >>
      updateMetricAsync[IO]("channelCount", dao.threadSafeMessageMemPool.activeChannels.size) >>
      updateTransactionServiceMetrics()

  implicit class TagSeqOps(val tagSeq: TagSeq) {

    def toMicrometer(key: String): java.util.List[Tag] =
      (Tag.of("metric", key) +: tagSeq.map { case (k, v) => Tag.of(k, v) }).asJava
  }

  /**
    * Recalculates window based / periodic metrics
    */
  override def trigger(): IO[Unit] =
    updatePeriodicMetrics()

  schedule(0.seconds, periodSeconds.seconds)
}
