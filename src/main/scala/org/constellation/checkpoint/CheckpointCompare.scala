package org.constellation.checkpoint

import cats.effect.IO
import cats.syntax.all._
import org.constellation.domain.cluster.ClusterStorageAlgebra
import org.constellation.util.PeriodicIO

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class CheckpointCompare(
  checkpointService: CheckpointService[IO],
  clusterStorage: ClusterStorageAlgebra[IO],
  unboundedExecutionContext: ExecutionContext
) extends PeriodicIO("CheckpointCompare", unboundedExecutionContext) {

  override def trigger(): IO[Unit] = clusterStorage.isAnActivePeer.ifM(
    checkpointService.compareAcceptedCheckpoints(),
    IO.unit
  )

  schedule(10.seconds)
}
