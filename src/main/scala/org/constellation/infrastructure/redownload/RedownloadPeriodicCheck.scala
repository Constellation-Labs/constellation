package org.constellation.infrastructure.redownload

import cats.effect.IO
import cats.syntax.all._
import org.constellation.domain.cluster.ClusterStorageAlgebra
import org.constellation.domain.redownload.RedownloadService
import org.constellation.util.Logging.logThread
import org.constellation.util.PeriodicIO

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class RedownloadPeriodicCheck(
  periodSeconds: Int = 30,
  unboundedExecutionContext: ExecutionContext,
  redownloadService: RedownloadService[IO],
  clusterStorage: ClusterStorageAlgebra[IO]
) extends PeriodicIO("RedownloadPeriodicCheck", unboundedExecutionContext) {

  private def triggerRedownloadCheck(): IO[Unit] =
    clusterStorage.isAnActiveFullPeer.ifM(
      for {
        _ <- redownloadService.checkForAlignmentWithMajoritySnapshot()
      } yield (),
      IO.delay {
        logger.debug(s"Node is not an active peers currently! Skipping redownload check!")
      }
    )

  override def trigger(): IO[Unit] = logThread(triggerRedownloadCheck(), "triggerRedownloadCheck", logger)

  schedule(periodSeconds seconds)

}
