package org.constellation.consensus

import akka.actor.{Actor, ActorLogging, Cancellable, Props}
import cats.effect.IO
import cats.implicits._
import constellation.{wrapFutureWithMetric, _}
import org.constellation.consensus.Round._
import org.constellation.consensus.RoundManager.{
  BroadcastLightTransactionProposal,
  BroadcastSelectedUnionBlock,
  BroadcastUnionBlockProposal
}
import org.constellation.p2p.DataResolver
import org.constellation.primitives.Schema.{
  CheckpointCache,
  EdgeHashType,
  SignedObservationEdge,
  TypedEdgeHash
}
import org.constellation.primitives._
import org.constellation.util.PeerApiClient
import org.constellation.{ConfigUtil, DAO}

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContextExecutor, Future}

class Round(roundData: RoundData,
            arbitraryTransactions: Seq[(Transaction, Int)],
            arbitraryMessages: Seq[(ChannelMessage, Int)],
            dao: DAO,
            dataResolver: DataResolver)
    extends Actor
    with ActorLogging {

  implicit val shadedDao: DAO = dao
  implicit val ec: ExecutionContextExecutor = dao.edgeExecutionContext

  private[consensus] val transactionProposals
    : mutable.Map[FacilitatorId, LightTransactionsProposal] =
    mutable.Map()
  private[consensus] val checkpointBlockProposals: mutable.Map[FacilitatorId, CheckpointBlock] =
    mutable.Map()
  private[consensus] val selectedCheckpointBlocks: mutable.Map[FacilitatorId, CheckpointBlock] =
    mutable.Map()

  private[consensus] var unionTransactionProposalsTikTok: Cancellable = _
  private[consensus] var acceptMajorityCheckpointBlockTikTok: Cancellable = _
  protected var sendArbitraryDataProposalsTikTok: Cancellable = _

  override def preStart(): Unit = {
    super.preStart()
    sendArbitraryDataProposalsTikTok = scheduleArbitraryDataProposals(1)
    unionTransactionProposalsTikTok = scheduleUnionProposals
  }

  private[consensus] var resolveMajorityCheckpointBlockTikTok: Cancellable = _

  private[consensus] var majorityCheckpointBlock: Option[CheckpointBlock] = None

  override def receive: Receive = {
    case StartTransactionProposal(_) =>
      sendArbitraryDataProposalsTikTok.cancel()
      val transactions = dao.transactionService
        .pullForConsensus(1)
        .map(_.map(_.transaction))
        .unsafeRunSync()

      val messages = dao.threadSafeMessageMemPool.pull()

      val distance = 0
      val proposal = LightTransactionsProposal(
        roundData.roundId,
        FacilitatorId(dao.id),
        transactions.map(_.hash) ++ arbitraryTransactions.filter(_._2 == distance).map(_._1.hash),
        messages
          .map(_.map(_.signedMessageData.hash))
          .getOrElse(Seq()) ++ arbitraryMessages
          .filter(_._2 == distance)
          .map(_._1.signedMessageData.hash),
        dao.peerInfo.unsafeRunSync().flatMap(_._2.notification).toSeq
      )

      passToParentActor(
        BroadcastLightTransactionProposal(
          roundData.peers,
          proposal
        )
      )

      sendArbitraryDataProposalsTikTok = scheduleArbitraryDataProposals(distance + 1)
      self ! proposal

    case newProp: LightTransactionsProposal =>
      val merged = if (transactionProposals.contains(newProp.facilitatorId)) {
        val old = transactionProposals(newProp.facilitatorId)
        old.copy(txHashes = old.txHashes ++ newProp.txHashes,
                 messages = old.messages ++ newProp.messages,
                 notifications = old.notifications ++ newProp.notifications)
      } else
        newProp
      transactionProposals += (newProp.facilitatorId -> merged)
      if (receivedAllTransactionProposals) {
        cancelUnionTransactionProposalsTikTok()
        self ! UnionProposals
      }

    case UnionBlockProposal(roundId, facilitatorId, checkpointBlock) =>
      checkpointBlockProposals += (facilitatorId -> checkpointBlock)
      if (receivedAllCheckpointBlockProposals) {
        cancelResolveMajorityCheckpointBlockTikTok()
        self ! ResolveMajorityCheckpointBlock(roundId)
      }

    case SelectedUnionBlock(roundId, facilitatorId, checkpointBlock) =>
      selectedCheckpointBlocks += (facilitatorId -> checkpointBlock)
      if (receivedAllSelectedUnionedBlocks) {
        cancelAcceptMajorityCheckpointBlockTikTok()
        self ! AcceptMajorityCheckpointBlock(roundId)
      }

    case UnionProposals => unionProposals()

    case ArbitraryDataProposals(distance) =>
      val proposals = transactionProposals.values
      val proposedTxs = proposals.flatMap(_.txHashes).toSet
      val proposedMessages = proposals.flatMap(_.messages).toSet
      val maybeData = {
        (arbitraryTransactions
           .filter(a => a._2 == distance && !proposedTxs.contains(a._1.hash))
           .map(_._1.hash),
         arbitraryMessages
           .filter(a => a._2 == distance && !proposedMessages.contains(a._1.signedMessageData.hash))
           .map(_._1.signedMessageData.hash))
      }
      maybeData match {
        case (Nil, Nil) =>
        case (txs, msgs) =>
          context.parent ! BroadcastLightTransactionProposal(
            roundData.peers,
            LightTransactionsProposal(
              roundData.roundId,
              FacilitatorId(dao.id),
              txs,
              msgs
            )
          )
      }
      scheduleArbitraryDataProposals(distance + 1)

    case ResolveMajorityCheckpointBlock(_, triggeredFromTimeout) =>
      resolveMajorityCheckpointBlock(triggeredFromTimeout)

    case AcceptMajorityCheckpointBlock(_) => acceptMajorityCheckpointBlock()

    case msg => log.info(s"Received unknown message: $msg")
  }

  private[consensus] def scheduleUnionProposals: Cancellable =
    context.system.scheduler.scheduleOnce(
      ConfigUtil.getDurationFromConfig("constellation.consensus.union-proposals-timeout",
                                       15.second),
      self,
      UnionProposals
    )

  private def scheduleArbitraryDataProposals(distance: Int): Cancellable =
    context.system.scheduler.scheduleOnce(
      ConfigUtil.getDurationFromConfig("constellation.consensus.arbitrary-data-proposals-timeout",
                                       4.second),
      self,
      ArbitraryDataProposals(distance)
    )

  private[consensus] def scheduleResolveMajorityCheckpointBlock: Cancellable =
    context.system.scheduler.scheduleOnce(
      ConfigUtil.getDurationFromConfig(
        "constellation.consensus.checkpoint-block-resolve-majority-timeout",
        5.second
      ),
      self,
      ResolveMajorityCheckpointBlock(roundData.roundId, triggeredFromTimeout = true)
    )

  private[consensus] def scheduleAcceptMajorityCheckpointBlock: Cancellable =
    context.system.scheduler.scheduleOnce(
      ConfigUtil.getDurationFromConfig(
        "constellation.consensus.accept-resolved-majority-block-timeout",
        5.second
      ),
      self,
      AcceptMajorityCheckpointBlock(roundData.roundId)
    )

  private[consensus] def receivedAllTransactionProposals: Boolean =
    transactionProposals.size >= roundData.peers.size + 1

  private[consensus] def receivedAllCheckpointBlockProposals: Boolean =
    checkpointBlockProposals.size >= roundData.peers.size + 1

  private[consensus] def receivedAllSelectedUnionedBlocks: Boolean =
    selectedCheckpointBlocks.size >= roundData.peers.size + 1

  private[consensus] def cancelUnionTransactionProposalsTikTok(): Unit = {
    Option(unionTransactionProposalsTikTok).foreach(_.cancel())
  }

  private[consensus] def cancelResolveMajorityCheckpointBlockTikTok(): Unit = {
    Option(resolveMajorityCheckpointBlockTikTok).foreach(_.cancel())
  }

  private[consensus] def cancelAcceptMajorityCheckpointBlockTikTok(): Unit = {
    Option(acceptMajorityCheckpointBlockTikTok).foreach(_.cancel())
  }

  private[consensus] def unionProposals(): Unit = {

    val readyPeers = dao.readyPeers.unsafeRunSync()

    val transactions = transactionProposals.values
      .flatMap(_.txHashes)
      .filter(hash ⇒ dao.transactionService.contains(hash).unsafeRunSync())
      .map(
        hash ⇒
          dao.transactionService
            .lookup(hash)
            .map(
              _.map(_.transaction)
          )
      )
      .toList
      .sequence[IO, Option[Transaction]]
      .map {
        _.flatten.toSet
          .union(roundData.transactions.toSet)
          .toSeq
      }
      .unsafeRunSync()

    val resolvedTxs = transactionProposals.values
      .flatMap(proposal ⇒ proposal.txHashes.map(hash ⇒ (hash, proposal)))
      .filterNot(p ⇒ dao.transactionService.contains(p._1).unsafeRunSync())
      .toList
      .map(
        p ⇒
          dataResolver
            .resolveTransactions(p._1,
                                 readyPeers.map(r => PeerApiClient(r._1, r._2.client)),
                                 readyPeers
                                   .get(p._2.facilitatorId.id)
                                   .map(rp => PeerApiClient(p._2.facilitatorId.id, rp.client)))
            .map(_.map(_.transaction))
      )
      .sequence
      .unsafeRunSync()
      .flatten

    val messages = transactionProposals.values
      .flatMap(_.messages)
      .map(
        hash ⇒
          dao.messageService
            .lookup(hash)
            .map(
              _.map(_.channelMessage)
          )
      )
      .toList
      .sequence[IO, Option[ChannelMessage]]
      .map {
        _.flatten.toSet
          .union(roundData.messages.toSet)
          .toSeq
      }
      .unsafeRunSync()

    val resolvedMessages = transactionProposals.values
      .flatMap(proposal ⇒ proposal.messages.map(hash ⇒ (hash, proposal)))
      .filterNot(p ⇒ dao.messageService.contains(p._1).unsafeRunSync())
      .toList
      .map(
        p ⇒
          dataResolver
            .resolveMessages(p._1,
                             readyPeers.map(r => PeerApiClient(r._1, r._2.client)),
                             readyPeers
                               .get(p._2.facilitatorId.id)
                               .map(rp => PeerApiClient(p._2.facilitatorId.id, rp.client)))
            .map(_.map(_.channelMessage))
      )
      .sequence
      .unsafeRunSync()
      .flatten

    val notifications = transactionProposals
      .flatMap(_._2.notifications)
      .toSet
      .union(roundData.peers.flatMap(_.notification))
      .toSeq

    log.info(s"--------------- Transactions: ${transactions.size} + ${resolvedTxs.size}")
    log.info(s"--------------- Messages: ${messages.size} + ${resolvedMessages.size}")
//    if (messages.nonEmpty) {
//      log.info(s"${messages.head.signedMessageData.data.hash} / ${resolvedMessages.head.signedMessageData.data.hash}")
//    }

    val cb = CheckpointBlock.createCheckpointBlock(
      transactions ++ resolvedTxs,
      roundData.tipsSOE.map(soe => TypedEdgeHash(soe.hash, EdgeHashType.CheckpointHash, Some(soe.baseHash))),
      messages ++ resolvedMessages,
      notifications
    )(dao.keyPair)
    val blockProposal = UnionBlockProposal(roundData.roundId, FacilitatorId(dao.id), cb)
    passToParentActor(BroadcastUnionBlockProposal(roundData.peers, blockProposal))
    resolveMajorityCheckpointBlockTikTok = scheduleResolveMajorityCheckpointBlock
    self ! blockProposal
  }

  private[consensus] def resolveMajorityCheckpointBlock(triggeredFromTimeout: Boolean): Unit = {
    dao.metrics.incrementMetric(
      "resolveMajorityCheckpointBlockActorTriggeredFromTimeout_" + triggeredFromTimeout
    )

    majorityCheckpointBlock = if (checkpointBlockProposals.nonEmpty) {
      val sameBlocks = checkpointBlockProposals
        .groupBy(_._2.baseHash)
        .maxBy(_._2.size)
        ._2

      dao.metrics.incrementMetric(
        "resolveMajorityCheckpointBlockProposalCount_" + checkpointBlockProposals.size
      )

      val checkpointBlock = sameBlocks.values.foldLeft(sameBlocks.head._2)(_ + _)

      val selectedCheckpointBlock =
        SelectedUnionBlock(roundData.roundId, FacilitatorId(dao.id), checkpointBlock)
      passToParentActor(BroadcastSelectedUnionBlock(roundData.peers, selectedCheckpointBlock))
      acceptMajorityCheckpointBlockTikTok = scheduleAcceptMajorityCheckpointBlock
      self ! selectedCheckpointBlock

      Some(checkpointBlock)
    } else {
      dao.metrics.incrementMetric("resolveMajorityCheckpointBlockProposalCount_0")
      None
    }
  }

  private[consensus] def acceptMajorityCheckpointBlock(): Unit = {
    val acceptedBlock = if (selectedCheckpointBlocks.nonEmpty) {
      val sameBlocks = selectedCheckpointBlocks
        .groupBy(_._2.soeHash)
        .maxBy(_._2.size)
        ._2

      val checkpointBlock = sameBlocks.head._2

      dao.metrics.incrementMetric(
        "acceptMajorityCheckpointBlockSelectedCount_" + selectedCheckpointBlocks.size
      )

      val finalFacilitators = selectedCheckpointBlocks.keySet.map(_.id).toSet
      val cache = CheckpointCache(Some(checkpointBlock), height = checkpointBlock.calculateHeight())
      log.debug(
        s"[${dao.id.short}] accepting majority checkpoint block ${checkpointBlock.baseHash}  " +
          s" with txs ${checkpointBlock.transactions.map(_.hash)} " +
          s"proposed by ${sameBlocks.head._1.id.short} other blocks ${sameBlocks.size} in round ${roundData.roundId}"
      )
      dao.threadSafeSnapshotService
        .accept(cache)
        .map { _ =>
          broadcastSignedBlockToNonFacilitators(FinishedCheckpoint(cache, finalFacilitators))
          Option(checkpointBlock)
        }
        .recoverWith {
          case error @ (CheckpointAcceptBlockAlreadyStored(_) | PendingAcceptance(_)) =>
            IO { log.warning(error.getMessage) } >> IO.pure(None)
          case unknownError =>
            IO { log.error(
              s"Failed to accept majority checkpoint block due to: ${unknownError.getMessage}",
              unknownError
            ) } >> IO.pure(None)
        }
        .unsafeRunSync()

    } else None

    passToParentActor(StopBlockCreationRound(roundData.roundId, acceptedBlock))
  }

  private[consensus] def broadcastSignedBlockToNonFacilitators(
    finishedCheckpoint: FinishedCheckpoint
  ): Future[List[Option[FinishedCheckpointResponse]]] = {
    val allFacilitators = roundData.peers.map(p => p.peerMetadata.id -> p).toMap
    val signatureResponses = Future.sequence(
      dao.peerInfo
        .unsafeRunSync()
        .values
        .toList
        .filterNot(pd => allFacilitators.contains(pd.peerMetadata.id))
        .map { peer =>
          log.info(
            s"[${dao.id.short}] broadcasting cb ${finishedCheckpoint.checkpointCacheData.checkpointBlock.get.baseHash} to  ${peer.client.id} in round ${roundData.roundId}"
          )
          wrapFutureWithMetric(
            peer.client.postNonBlocking[Option[FinishedCheckpointResponse]](
              "finished/checkpoint",
              finishedCheckpoint,
              timeout = 20.seconds
            ),
            "finishedCheckpointBroadcast",
          )(dao, ec).recoverWith {
            case e: Throwable =>
              log.warning("Failure gathering signature", e)
              dao.metrics.incrementMetric(
                "formCheckpointSignatureResponseError"
              )
              Future.failed(e)
          }
        }
    )

    wrapFutureWithMetric(signatureResponses, "checkpointBlockFormation")(dao, ec)
  }

  private[consensus] def passToParentActor(cmd: Any): Unit = {
    context.parent ! cmd
  }
}

case class FacilitatorId(id: Schema.Id) extends AnyVal
case class RoundId(id: String) extends AnyVal

object Round {
  sealed trait RoundCommand {
    def roundId: RoundId
  }

  case object UnionProposals
  case class ArbitraryDataProposals(distance: Int)

  case class ResolveMajorityCheckpointBlock(roundId: RoundId, triggeredFromTimeout: Boolean = false)
      extends RoundCommand

  case class AcceptMajorityCheckpointBlock(roundId: RoundId) extends RoundCommand

  case class StartTransactionProposal(roundId: RoundId) extends RoundCommand

  case class LightTransactionsProposal(
    roundId: RoundId,
    facilitatorId: FacilitatorId,
    txHashes: Seq[String],
    messages: Seq[String] = Seq(),
    notifications: Seq[PeerNotification] = Seq()
  ) extends RoundCommand

  case class UnionBlockProposal(
    roundId: RoundId,
    facilitatorId: FacilitatorId,
    checkpointBlock: CheckpointBlock
  ) extends RoundCommand

  case class RoundData(
    roundId: RoundId,
    peers: Set[PeerData],
    lightPeers: Set[PeerData],
    facilitatorId: FacilitatorId,
    transactions: Seq[Transaction],
    tipsSOE: Seq[SignedObservationEdge],
    messages: Seq[ChannelMessage]
  )

  case class ConsensusTimeout(roundId: RoundId) extends RoundCommand
  case class StopBlockCreationRound(roundId: RoundId,
                                    maybeCB: Option[CheckpointBlock])
      extends RoundCommand

  case class SelectedUnionBlock(
    roundId: RoundId,
    facilitatorId: FacilitatorId,
    checkpointBlock: CheckpointBlock
  ) extends RoundCommand

  def props(roundData: RoundData,
            arbitraryTransactions: Seq[(Transaction, Int)],
            arbitraryMessages: Seq[(ChannelMessage, Int)],
            dao: DAO,
            dataResolver: DataResolver): Props =
    Props(new Round(roundData, arbitraryTransactions, arbitraryMessages, dao, dataResolver))
}
