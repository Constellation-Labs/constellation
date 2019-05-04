package org.constellation.primitives

import java.time.{LocalDateTime, Duration => JDuration}
import java.util.concurrent.TimeUnit

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.scalalogging.StrictLogging
import constellation.{EasyFutureBlock, futureTryWithTimeoutMetric}
import org.constellation.DAO
import org.constellation.consensus.CrossTalkConsensus.{OwnRoundInProgress, StartNewBlockCreationRound}
import org.constellation.consensus.EdgeProcessor
import org.constellation.primitives.Schema.NodeState
import org.constellation.util.Periodic

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Random, Try}

class CheckpointFormationManager(
  periodSeconds: Int = 1,
  undersizedCheckpointThresholdSeconds: Int = 30,
  crossTalkConsensusActor: ActorRef
)(implicit dao: DAO)
  extends Periodic[Try[Option[Boolean]]]("RandomTransactionManager", periodSeconds) with StrictLogging {

  def toFiniteDuration(d: java.time.Duration): FiniteDuration = Duration.fromNanos(d.toNanos)

  implicit val ec: ExecutionContextExecutor = dao.edgeExecutionContext

  implicit val timeout: Timeout = Timeout(1.seconds)

  private var formEmptyCheckpointAfterThreshold: FiniteDuration =
    undersizedCheckpointThresholdSeconds.seconds

  @volatile private var lastCheckpoint = LocalDateTime.now

  //noinspection ScalaStyle
  override def trigger() = {
    val memPoolCount = dao.threadSafeTXMemPool.unsafeCount
    val elapsedTime = toFiniteDuration(JDuration.between(dao.lastCheckpoint, LocalDateTime.now))

    val ownRoundInProgress =
      (crossTalkConsensusActor ? OwnRoundInProgress).mapTo[Boolean].get(1)

    val minTXInBlock = dao.processingConfig.minCheckpointFormationThreshold

    logger.info(s"Attempting to create checkpoint: Mempoolcount: $memPoolCount," + s" elapsedTime: $elapsedTime," +
      s" ownRoundInProgress: $ownRoundInProgress, blockFormationInProgress: ${dao.blockFormationInProgress}")

    if ((memPoolCount >= minTXInBlock || (elapsedTime >= formEmptyCheckpointAfterThreshold)) &&
        dao.formCheckpoints &&
        dao.nodeState == NodeState.Ready &&
        !dao.blockFormationInProgress &&
        !ownRoundInProgress) {

      logger.info("forming checkpoint")
      dao.blockFormationInProgress = true

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
        val op = EdgeProcessor.formCheckpoint(dao.threadSafeMessageMemPool.pull().getOrElse(Seq()))
        op.onComplete {
          _ => dao.blockFormationInProgress = false
          dao.lastCheckpoint = LocalDateTime.now()
        }
      } else {
        crossTalkConsensusActor ! StartNewBlockCreationRound
      }
      dao.metrics.updateMetric("blockFormationInProgress", dao.blockFormationInProgress.toString)
    } else { logger.info("Skipping checkpoint formation") }

    Future.successful(Try(None))
  }
}
