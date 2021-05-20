package org.constellation.infrastructure.redownload

import cats.effect.IO
import cats.syntax.all._
import org.constellation.util.Logging.logThread
import org.constellation.DAO
import org.constellation.util.PeriodicIO

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class RedownloadPeriodicCheck(periodSeconds: Int = 30, unboundedExecutionContext: ExecutionContext)(implicit dao: DAO)
    extends PeriodicIO("RedownloadPeriodicCheck", unboundedExecutionContext) {

  private def triggerRedownloadCheck(): IO[Unit] =
    dao.cluster.isNodeAnActiveFullNode.ifM(
      for {
        // TODO: function below removed by gossip, check if active pool needs it or not and how to fix active pool now
        //_ <- dao.redownloadService.fetchAndUpdatePeersProposals()
        _ <- dao.redownloadService.checkForAlignmentWithMajoritySnapshot()
      } yield (),
      IO.delay {
        logger.debug(s"Node is not an active peers currently! Skipping redownload check!")
      }
    )

  override def trigger(): IO[Unit] = logThread(triggerRedownloadCheck(), "triggerRedownloadCheck", logger)

  schedule(periodSeconds seconds)

}
