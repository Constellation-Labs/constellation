package org.constellation.domain.cloud

import better.files.File
import cats.effect.Concurrent
import cats.effect.concurrent.Ref
import cats.syntax.all._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import fs2._
import fs2.concurrent.Queue
import org.constellation.domain.cloud.CloudService._
import org.constellation.domain.cloud.config._
import org.constellation.domain.cloud.providers.{CloudServiceProvider, GCPProvider, LocalProvider, S3Provider}
import org.constellation.schema.GenesisObservation

class CloudService[F[_]](providers: List[CloudServiceProvider[F]])(implicit F: Concurrent[F]) {

  private val logger = Slf4jLogger.getLogger[F]

  private val sentData: Ref[F, Set[SnapshotSent]] = Ref.unsafe(Set.empty)

  def cloudSendingQueue(queue: Queue[F, DataToSend]): F[CloudServiceEnqueue[F]] = {
    val send: F[Unit] = queue.dequeue.through(sendFile).compile.drain

    F.start(send).map { _ =>
      // TODO: fiber -> we may want to cancel a queue
      new CloudServiceEnqueue[F]() {
        def enqueueGenesis(genesis: GenesisObservation): F[Unit] =
          queue.enqueue1(GenesisToSend(genesis)) >> logger.debug(s"Enqueued genesis")

        def enqueueSnapshot(snapshot: File, height: Long, hash: String): F[Unit] =
          queue.enqueue1(SnapshotToSend(snapshot, height, hash)) >> logger
            .debug(s"Enqueued snapshot height=$height hash=$hash")

        def enqueueSnapshotInfo(snapshotInfo: File, height: Long, hash: String): F[Unit] =
          queue.enqueue1(SnapshotInfoToSend(snapshotInfo, height, hash)) >> logger
            .debug(s"Enqueued snapshot info height=$height hash=$hash")
        def getAlreadySent(): F[Set[SnapshotSent]] =
          sentData.get.map(_.filter(snap => snap.snapshot && snap.snapshotInfo))
        def removeSentSnapshot(hash: String): F[Unit] =
          sentData.modify { s =>
            val updated = s.filterNot(_.hash == hash)
            (updated, ())
          }
      }
    }
  }

  private def sendFile: Pipe[F, DataToSend, Unit] =
    _.evalMap {
      case GenesisToSend(genesis) => sendGenesis(genesis)
      case s @ SnapshotToSend(_, height, hash) =>
        sendSnapshot(s) >>= { ops =>
          if (atLeastOne(ops)) markSnapshotAsSent(height, hash) else F.unit
        }
      case s @ SnapshotInfoToSend(_, height, hash) =>
        sendSnapshotInfo(s) >>= { ops =>
          if (atLeastOne(ops)) markSnapshotInfoAsSent(height, hash) else F.unit
        }
    }

  private def atLeastOne(ops: List[Either[Throwable, Unit]]): Boolean =
    ops.count(_.isRight) > 0

  private def markSnapshotAsSent(height: Long, hash: String): F[Unit] =
    sentData.modify { s =>
      val updated = s
        .find(_.hash == hash)
        .fold(s + SnapshotSent(height, hash, snapshot = true, snapshotInfo = false))(
          snap =>
            s.filterNot(_.hash == hash) + SnapshotSent(height, hash, snapshot = true, snapshotInfo = snap.snapshotInfo)
        )
      (updated, ())
    }

  private def markSnapshotInfoAsSent(height: Long, hash: String): F[Unit] =
    sentData.modify { s =>
      val updated = s
        .find(_.hash == hash)
        .fold(s + SnapshotSent(height, hash, snapshot = false, snapshotInfo = true))(
          snap =>
            s.filterNot(_.hash == hash) + SnapshotSent(height, hash, snapshot = snap.snapshot, snapshotInfo = true)
        )
      (updated, ())
    }

  private def sendGenesis(genesis: GenesisObservation): F[Unit] =
    if (providers.isEmpty)
      logger.debug(s"Sending genesis to cloud, provider list is empty, PRETENDING that genesis has been sent.") >>
        F.unit
    else {
      providers.traverse { p =>
        logger.debug(s"Sending genesis to cloud, provider=${p.name}") >>
          p.storeGenesis(genesis)
            .rethrowT
            .handleErrorWith(logger.error(_)(s"Sending genesis to cloud failed, provider=${p.name}"))
      }.void
    }

  private def sendSnapshot(snapshot: SnapshotToSend): F[List[Either[Throwable, Unit]]] =
    if (providers.isEmpty)
      logger.debug(
        s"Sending snapshot to cloud, provider list is empty, PRETENDING that snapshot for height=${snapshot.height} hash=${snapshot.hash} has been sent."
      ) >>
        List(().asRight[Throwable]).pure[F]
    else {
      providers.traverse { p =>
        logger
          .debug(s"Sending snapshot to cloud, provider=${p.name} height=${snapshot.height} hash=${snapshot.hash}") >>
          p.storeSnapshot(snapshot.file, snapshot.height, snapshot.hash)
            .rethrowT
            .map(_.asRight[Throwable])
            .handleErrorWith { error =>
              logger
                .error(error) {
                  s"Sending snapshot to cloud failed, provider=${p.name} height=${snapshot.height} hash=${snapshot.hash}"
                } >> error.asLeft[Unit].pure[F]
            }
      }
    }

  private def sendSnapshotInfo(snapshotInfo: SnapshotInfoToSend): F[List[Either[Throwable, Unit]]] =
    if (providers.isEmpty) {
      logger.debug(
        s"Sending snapshot info to cloud, provider list is empty, PRETENDING that snapshot info for height=${snapshotInfo.height} hash=${snapshotInfo.hash} has been sent."
      ) >>
        List(().asRight[Throwable]).pure[F]
    } else {
      providers.traverse { p =>
        logger
          .debug(
            s"Sending snapshot info to cloud, provider=${p.name} height=${snapshotInfo.height} hash=${snapshotInfo.hash}"
          ) >>
          p.storeSnapshotInfo(snapshotInfo.file, snapshotInfo.height, snapshotInfo.hash)
            .rethrowT
            .map(_.asRight[Throwable])
            .handleErrorWith { error =>
              logger
                .error(error) {
                  s"Sending snapshot info to cloud failed, provider=${p.name} height=${snapshotInfo.height} hash=${snapshotInfo.hash}"
                } >> error.asLeft[Unit].pure[F]
            }
      }
    }
}

object CloudService {

  def apply[F[_]: Concurrent](cloudConfig: CloudConfig): CloudService[F] = {
    val providers: List[CloudServiceProvider[F]] = cloudConfig.providers.map {
      case config @ S3(_, _, _)          => S3Provider(config)
      case config @ GCP(_, _)            => GCPProvider(config)
      case config @ S3Compat(_, _, _, _) => S3Provider(config)
      case config @ Local(_)             => LocalProvider(config)
    }

    new CloudService(providers)
  }

  trait CloudServiceEnqueue[F[_]] {
    def enqueueGenesis(genesis: GenesisObservation): F[Unit]
    def enqueueSnapshot(snapshot: File, height: Long, hash: String): F[Unit]
    def enqueueSnapshotInfo(snapshotInfo: File, height: Long, hash: String): F[Unit]
    def getAlreadySent(): F[Set[SnapshotSent]]
    def removeSentSnapshot(hash: String): F[Unit]
  }

  sealed trait DataToSend
  sealed trait ClassToSend[A] extends DataToSend {
    val data: A
  }
  sealed trait FileToSend extends DataToSend {
    val file: File
  }

  case class GenesisToSend(data: GenesisObservation) extends ClassToSend[GenesisObservation]
  case class SnapshotToSend(file: File, height: Long, hash: String) extends FileToSend
  case class SnapshotInfoToSend(file: File, height: Long, hash: String) extends FileToSend

  case class SnapshotSent(height: Long, hash: String, snapshot: Boolean, snapshotInfo: Boolean)
}
