package org.constellation.util

import java.util.concurrent.atomic.AtomicLong

import better.files.File
import cats.effect.{IO, Sync}
import cats.implicits._
import com.google.common.util.concurrent.AtomicDouble
import com.typesafe.scalalogging.Logger
import constellation._
import io.micrometer.core.instrument.Metrics.globalRegistry
import io.micrometer.core.instrument.binder.jvm._
import io.micrometer.core.instrument.binder.logging.LogbackMetrics
import io.micrometer.core.instrument.binder.system.{FileDescriptorMetrics, ProcessorMetrics, UptimeMetrics}
import io.micrometer.core.instrument.{Clock, Counter, Tag, Timer}
import io.micrometer.prometheus.{PrometheusConfig, PrometheusMeterRegistry}
import io.prometheus.client.CollectorRegistry
import io.prometheus.client.cache.caffeine.CacheMetricsCollector
import org.constellation.{BuildInfo, ConstellationExecutionContext, DAO}
import org.joda.time.DateTime

import scala.collection.concurrent.TrieMap
import scala.concurrent.{Future, duration}

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
  val checkpointValidationFailure = "checkpointValidationFailure"
  val batchTransactionEndpoint = "batchTransactionsEndpoint"

  val success = "_success"
  val failure = "_failure"

  val cacheMetrics = new CacheMetricsCollector()
  cacheMetrics.register()

  def prometheusSetup(keyHash: String): PrometheusMeterRegistry = {
    val prometheusMeterRegistry =
      new PrometheusMeterRegistry(PrometheusConfig.DEFAULT, CollectorRegistry.defaultRegistry, Clock.SYSTEM)

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
    new DiskSpaceMetrics(File(System.getProperty("user.dir")).toJava)
      .bindTo(prometheusMeterRegistry)
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
      "TPS_last_" + dao.nodeConfig.metricIntervalSeconds + "_seconds" -> tps,
      "TPS_all" -> tpsAll
    )
  }

}

/**
  * TrieMap backed metrics store for string values and counters
  * @param periodSeconds: How often to recalculate moving window metrics (e.g. TPS)
  * @param dao: Data access object
  */
class Metrics(periodSeconds: Int = 1)(implicit dao: DAO) extends Periodic[Unit]("Metrics", periodSeconds) {

  val logger = Logger("Metrics")

  private val stringMetrics: TrieMap[String, String] = TrieMap()
  private val countMetrics: TrieMap[String, AtomicLong] = TrieMap()
  private val doubleMetrics: TrieMap[String, AtomicDouble] = TrieMap()

  val rateCounter = new TransactionRateTracker()
  val micrometerCounter = new TrieMap[String, Counter]
//  val micrometerTimer = new TrieMap[String, Timer]

  // Init

  val registry = Metrics.prometheusSetup(dao.keyPair.getPublic.hash)

  implicit val timer: cats.effect.Timer[IO] = IO.timer(ConstellationExecutionContext.unbounded)

  val init = for {
    currentTime <- cats.effect.Clock[IO].realTime(duration.MILLISECONDS)
    _ <- updateMetricAsync[IO]("id", dao.id.hex)
    _ <- dao.cluster.getNodeState.map(_.toString).flatMap(updateMetricAsync[IO]("nodeState", _))
    _ <- updateMetricAsync[IO]("address", dao.selfAddressStr)
    _ <- updateMetricAsync[IO]("nodeStartTimeMS", currentTime.toString)
    _ <- updateMetricAsync[IO]("nodeStartDate", new DateTime(currentTime).toString)
    _ <- updateMetricAsync[IO]("externalHost", dao.externalHostString)
    _ <- updateMetricAsync[IO]("version", BuildInfo.version)
  } yield ()

  init.unsafeRunSync

  private def guagedAtomicLong(key: String): AtomicLong = {
    import scala.collection.JavaConverters._
    val tags = List(Tag.of("metric", key)).asJava
    registry.gauge(s"dag_$key", tags, new AtomicLong(0L))
  }

  // Note: AtomicDouble comes from guava
  private def guagedAtomicDouble(key: String): AtomicDouble = {
    import scala.collection.JavaConverters._
    val tags = List(Tag.of("metric", key)).asJava
    registry.gauge(s"dag_$key", tags, new AtomicDouble(0d))
  }

  def updateMetric(key: String, value: Double): Unit =
    doubleMetrics.getOrElseUpdate(key, guagedAtomicDouble(key)).set(value)

  def updateMetric(key: String, value: String): Unit =
    stringMetrics(key) = value

  def updateMetric(key: String, value: Int): Unit =
    updateMetric(key, value.toLong)

  def updateMetric(key: String, value: Long): Unit =
    countMetrics.getOrElseUpdate(key, guagedAtomicLong(key)).set(value)

  def incrementMetric(key: String): Unit = {
    import scala.collection.JavaConverters._
    val tags = List(Tag.of("metric", key)).asJava
    micrometerCounter.getOrElseUpdate(s"dag_$key", { registry.counter(s"dag_$key", tags) }).increment()
    countMetrics.getOrElseUpdate(key, new AtomicLong(0L)).getAndUpdate(_ + 1L)
  }

  def startTimer: Timer.Sample = Timer.start()

  def stopTimer(key: String, timer: Timer.Sample): Unit =
    timer.stop(Timer.builder(key).register(registry))

  def updateMetricAsync[F[_]: Sync](key: String, value: String): F[Unit] = Sync[F].delay(updateMetric(key, value))
  def updateMetricAsync[F[_]: Sync](key: String, value: Double): F[Unit] = Sync[F].delay(updateMetric(key, value))
  def updateMetricAsync[F[_]: Sync](key: String, value: Int): F[Unit] = Sync[F].delay(updateMetric(key, value))
  def updateMetricAsync[F[_]: Sync](key: String, value: Long): F[Unit] = Sync[F].delay(updateMetric(key, value))
  def incrementMetricAsync[F[_]: Sync](key: String): F[Unit] = Sync[F].delay(incrementMetric(key))

  /**
    * Converts counter metrics to string for export / display
    * @return : Key value map of all metrics
    */
  def getMetrics: Map[String, String] =
    stringMetrics.toMap ++ countMetrics.toMap.mapValues(_.toString) ++ doubleMetrics.toMap.mapValues(_.toString)

  def getCountMetric(key: String): Option[Long] =
    countMetrics.get(key).map(_.get())

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
    IO { rateCounter.calculate(countMetrics.get("transactionAccepted").map { _.get() }.getOrElse(0L)) }
      .map(_.toList)
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

  private def updatePeriodicMetrics(): IO[Unit] =
    updateBalanceMetrics >>
      updateTransactionAcceptedMetrics() >>
      updateObservationServiceMetrics() >>
      updateMetricAsync[IO]("nodeCurrentTimeMS", System.currentTimeMillis().toString) >>
      updateMetricAsync[IO]("nodeCurrentDate", new DateTime().toString()) >>
      updateMetricAsync[IO]("metricsRound", round) >>
      dao.addressService.size().flatMap(size => updateMetricAsync[IO]("addressCount", size)) >>
      updateMetricAsync[IO]("channelCount", dao.threadSafeMessageMemPool.activeChannels.size) >>
      updateTransactionServiceMetrics()

  /**
    * Recalculates window based / periodic metrics
    */
  override def trigger(): Future[Unit] = updatePeriodicMetrics().unsafeToFuture()
}
