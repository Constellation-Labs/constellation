package org.constellation.util

import cats.effect.IO
import cats.syntax.all._
import org.constellation.domain.checkpointBlock.CheckpointStorageAlgebra
import org.constellation.domain.redownload.RedownloadStorageAlgebra
import org.constellation.domain.snapshot.SnapshotStorageAlgebra

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class MetricsUpdater(
  metrics: Metrics,
  checkpointStorage: CheckpointStorageAlgebra[IO],
  snapshotStorage: SnapshotStorageAlgebra[IO],
  redownloadStorage: RedownloadStorageAlgebra[IO],
  unboundedExecutionContext: ExecutionContext
) extends PeriodicIO("metrics_updater", unboundedExecutionContext) {

  def trigger(): IO[Unit] =
    for {
      _ <- checkpointStorage.countCheckpoints >>= { value =>
        metrics.updateMetricAsync[IO]("checkpoints_total", value)
      }
      _ <- checkpointStorage.countAccepted >>= { value =>
        metrics.updateMetricAsync[IO]("checkpoints_accepted", value)
      }
      _ <- checkpointStorage.countInAcceptance >>= { value =>
        metrics.updateMetricAsync[IO]("checkpoints_inAcceptance", value)
      }
      _ <- checkpointStorage.countInSnapshot >>= { value =>
        metrics.updateMetricAsync[IO]("checkpoints_inSnapshot", value)
      }
      _ <- checkpointStorage.countAwaiting >>= { value =>
        metrics.updateMetricAsync[IO]("checkpoints_awaiting", value)
      }
      _ <- checkpointStorage.countWaitingForAcceptance >>= { value =>
        metrics.updateMetricAsync[IO]("checkpoints_waitingForAcceptance", value)
      }
      _ <- checkpointStorage.countWaitingForResolving >>= { value =>
        metrics.updateMetricAsync[IO]("checkpoints_waitingForResolving", value)
      }
      _ <- checkpointStorage.countWaitingForAcceptanceAfterDownload >>= { value =>
        metrics.updateMetricAsync[IO]("checkpoints_waitingForAcceptanceAfterDownload", value)
      }
      _ <- checkpointStorage.countTips >>= { value =>
        metrics.updateMetricAsync[IO]("activeTips", value)
      }
      _ <- checkpointStorage.countMissingTips >>= { value =>
        metrics.updateMetricAsync[IO]("missingActiveTips", value)
      }
      _ <- checkpointStorage.getMinTipHeight >>= { value =>
        metrics.updateMetricAsync[IO]("minTipHeight", value)
      }

      _ <- snapshotStorage.getNextSnapshotHash >>= { value =>
        metrics.updateMetricAsync[IO]("nextSnapshotHash", value)
      }
      _ <- snapshotStorage.getLastSnapshotHeight >>= { value =>
        metrics.updateMetricAsync[IO]("lastSnapshotHeight", value)
      }
      _ <- snapshotStorage.getStoredSnapshot >>= { value =>
        metrics.updateMetricAsync[IO]("storedSnapshotHeight", value.height)
      }

      _ <- redownloadStorage.getAcceptedSnapshots.map(_.size) >>= { value =>
        metrics.updateMetricAsync[IO]("redownload_acceptedSnapshots", value)
      }

      _ <- redownloadStorage.getCreatedSnapshots.map(_.size) >>= { value =>
        metrics.updateMetricAsync[IO]("redownload_createdSnapshots", value)
      }

      _ <- redownloadStorage.getLatestMajorityHeight >>= { value =>
        metrics.updateMetricAsync[IO]("redownload_lastestMajorityHeight", value)
      }

      _ <- redownloadStorage.getLowestMajorityHeight >>= { value =>
        metrics.updateMetricAsync[IO]("redownload_lowestMajorityHeight", value)
      }

      _ <- redownloadStorage.getLastSentHeight >>= { value =>
        metrics.updateMetricAsync[IO]("redownload_lastSentHeight", value)
      }
    } yield ()

  schedule(2.seconds)
}
