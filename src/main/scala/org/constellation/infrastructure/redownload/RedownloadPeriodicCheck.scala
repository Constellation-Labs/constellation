package org.constellation.infrastructure.redownload

import cats.effect.{ContextShift, IO}
import cats.implicits._
import org.constellation.util.Logging.logThread
import org.constellation.{ConstellationExecutionContext, DAO}
import org.constellation.util.PeriodicIO

import scala.concurrent.duration._

class RedownloadPeriodicCheck(periodSeconds: Int = 30)(implicit dao: DAO)
    extends PeriodicIO("RedownloadPeriodicCheck") {

  val contextShift: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.bounded)

  private def triggerRedownloadCheck(): IO[Unit] =
    for {
      _ <- dao.redownloadService.fetchAndSetPeerProposals()
//      _ <- dao.redownloadService.recalculateMajoritySnapshot()
      _ <- dao.redownloadService.checkForAlignmentWithMajoritySnapshot()
    } yield ()

  override def trigger(): IO[Unit] = logThread(triggerRedownloadCheck(), "triggerRedownloadCheck", logger)

  schedule(periodSeconds seconds)

}
