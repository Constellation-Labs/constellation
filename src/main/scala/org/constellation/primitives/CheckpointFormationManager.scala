package org.constellation.primitives

import java.time.{LocalDateTime, Duration => JDuration}
import java.util.concurrent.TimeUnit

import constellation.{EasyFutureBlock, futureTryWithTimeoutMetric}
import org.constellation.DAO
import org.constellation.consensus.EdgeProcessor
import org.constellation.primitives.Schema.NodeState
import org.constellation.util.Periodic

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Random, Try}

class CheckpointFormationManager(periodSeconds: Int = 1,
                                 undersizedCheckpointThresholdSeconds: Int = 30)(implicit dao: DAO)
    extends Periodic[Try[Option[Boolean]]]("RandomTransactionManager", periodSeconds) {

  def toFiniteDuration(d: java.time.Duration): FiniteDuration = Duration.fromNanos(d.toNanos)

  implicit val ec: ExecutionContextExecutor = dao.edgeExecutionContext

  private var formEmptyCheckpointAfterThreshold: FiniteDuration =
    undersizedCheckpointThresholdSeconds.seconds

  @volatile private var lastCheckpoint = LocalDateTime.now

  override def trigger() = {
    val memPoolCount = dao.threadSafeTXMemPool.unsafeCount
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

      dao.blockFormationInProgress = true

      val messages =
        dao.threadSafeMessageMemPool.pull(dao.processingConfig.maxMessagesInBlock).getOrElse(Seq())
      val res = futureTryWithTimeoutMetric(
        EdgeProcessor.formCheckpoint(messages).getTry(60),
        "formCheckpointFromRandomTXManager",
        timeoutSeconds = dao.processingConfig.formCheckpointTimeout, {
          messages.foreach { m =>
            dao.threadSafeMessageMemPool
              .activeChannels(m.signedMessageData.data.channelId)
              .release()
          }
        }
      )(dao.edgeExecutionContext, dao).map(_.flatten)
      res.onComplete(_ => {
        dao.blockFormationInProgress = false; lastCheckpoint = LocalDateTime.now
      })
      res
    } else Future.successful(Try(None))
  }
}
