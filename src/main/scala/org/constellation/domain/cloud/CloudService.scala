package org.constellation.domain.cloud

import better.files.File
import cats.implicits._
import cats.effect.Concurrent
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.domain.cloud.CloudService.{
  CloudServiceEnqueue,
  DataToSend,
  GenesisToSend,
  SnapshotInfoToSend,
  SnapshotToSend
}
import org.constellation.domain.cloud.config.{CloudConfig, GCP, Local, S3, S3Compat}
import org.constellation.domain.cloud.providers.{CloudServiceProvider, GCPProvider, LocalProvider, S3Provider}
import org.constellation.primitives.Schema.GenesisObservation
import fs2._
import fs2.concurrent.Queue

class CloudService[F[_]](providers: List[CloudServiceProvider[F]])(implicit F: Concurrent[F]) {

  private val logger = Slf4jLogger.getLogger[F]

  def cloudSendingQueue(queue: Queue[F, DataToSend]): F[CloudServiceEnqueue[F]] = {
    val send: F[Unit] = queue.dequeue.through(sendFile).compile.drain

    F.start(send).map { fiber =>
      // TODO: fiber -> we may want to cancel a queue
      new CloudServiceEnqueue[F]() {
        def enqueueGenesis(genesis: GenesisObservation): F[Unit] =
          queue.enqueue1(GenesisToSend(genesis)) >> logger.debug(s"Enqueued genesis")
        def enqueueSnapshot(snapshot: File, height: Long, hash: String) =
          queue.enqueue1(SnapshotToSend(snapshot, height, hash)) >> logger
            .debug(s"Enqueued snapshot height=${height} hash=${hash}")
        def enqueueSnapshotInfo(snapshotInfo: File, height: Long, hash: String) =
          queue.enqueue1(SnapshotInfoToSend(snapshotInfo, height, hash)) >> logger
            .debug(s"Enqueued snapshot info height=${height} hash=${hash}")
      }
    }
  }

  private def sendFile: Pipe[F, DataToSend, Unit] =
    _.evalMap {
      case GenesisToSend(genesis)          => sendGenesis(genesis)
      case s @ SnapshotToSend(_, _, _)     => sendSnapshot(s)
      case s @ SnapshotInfoToSend(_, _, _) => sendSnapshotInfo(s)
    }

  private def sendGenesis(genesis: GenesisObservation): F[Unit] =
    providers.traverse { p =>
      logger.debug(s"Sending genesis to cloud, provider=${p.name}") >>
        p.storeGenesis(genesis)
          .rethrowT
          .handleErrorWith(logger.error(_)(s"Sending genesis to cloud failed, provider=${p.name}"))
    }.void

  private def sendSnapshot(snapshot: SnapshotToSend): F[Unit] =
    providers.traverse { p =>
      logger.debug(s"Sending snapshot to cloud, provider=${p.name} height=${snapshot.height} hash=${snapshot.hash}") >>
        p.storeSnapshot(snapshot.file, snapshot.height, snapshot.hash).rethrowT.handleErrorWith {
          logger.error(_) {
            s"Sending snapshot to cloud failed, provider=${p.name} height=${snapshot.height} hash=${snapshot.hash}"
          }
        }
    }.void

  private def sendSnapshotInfo(snapshotInfo: SnapshotInfoToSend): F[Unit] =
    providers.traverse { p =>
      logger.debug(
        s"Sending snapshot info to cloud, provider=${p.name} height=${snapshotInfo.height} hash=${snapshotInfo.hash}"
      ) >>
        p.storeSnapshotInfo(snapshotInfo.file, snapshotInfo.height, snapshotInfo.hash).rethrowT.handleErrorWith {
          logger.error(_) {
            s"Sending snapshot info to cloud failed, provider=${p.name} height=${snapshotInfo.height} hash=${snapshotInfo.hash}"
          }
        }
    }.void
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
}
