package org.constellation.primitives

import java.time.{LocalDateTime, Duration => JDuration}
import java.util.concurrent.TimeUnit

import akka.actor.ActorRef
import akka.util.Timeout
import constellation.{EasyFutureBlock, futureTryWithTimeoutMetric}
import org.constellation.DAO
import org.constellation.consensus.CrossTalkConsensus.StartNewBlockCreationRound
import org.constellation.consensus.EdgeProcessor
import org.constellation.primitives.Schema.NodeState
import org.constellation.primitives.storage.TransactionStatus
import org.constellation.util.Periodic

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Random, Try}

class CheckpointFormationManager(
  periodSeconds: Int = 10,
  undersizedCheckpointThresholdSeconds: Int = 30,
  crossTalkConsensusActor: ActorRef
)(implicit dao: DAO)
  extends Periodic[Try[Option[Boolean]]]("RandomTransactionManager", periodSeconds) {

  def toFiniteDuration(d: java.time.Duration): FiniteDuration = Duration.fromNanos(d.toNanos)

  implicit val ec: ExecutionContextExecutor = dao.edgeExecutionContext

  private var formEmptyCheckpointAfterThreshold: FiniteDuration =
    undersizedCheckpointThresholdSeconds.seconds

  @volatile private var lastCheckpoint = LocalDateTime.now

  override def trigger() = {
    val memPoolCount = dao.transactionService.count(TransactionStatus.Pending).unsafeRunSync()
    val elapsedTime = toFiniteDuration(JDuration.between(lastCheckpoint, LocalDateTime.now))

    val minTXInBlock = dao.processingConfig.minCheckpointFormationThreshold

    if ((memPoolCount >= minTXInBlock || (elapsedTime >= formEmptyCheckpointAfterThreshold)) &&
        dao.formCheckpoints &&
        dao.nodeState == NodeState.Ready &&
        !dao.blockFormationInProgress) {

      if (elapsedTime >= formEmptyCheckpointAfterThreshold) {
        // randomize the next threshold window to prevent nodes from synchronizing empty block formation.
        formEmptyCheckpointAfterThreshold = Duration(
          (undersizedCheckpointThresholdSeconds / 2) + Random.nextInt(
            undersizedCheckpointThresholdSeconds
          ),
          TimeUnit.SECONDS
        )
      }

      if (dao.nodeConfig.isGenesisNode) {
        EdgeProcessor.formCheckpoint(dao.threadSafeMessageMemPool.pull().getOrElse(Seq()))
      } else {
        crossTalkConsensusActor ! StartNewBlockCreationRound
      }
    }

    Future.successful(Try(None))
  }
}
