package org.constellation.consensus

import akka.actor.{Actor, Cancellable, Props}
import cats.effect.IO
import cats.implicits._
import com.softwaremill.sttp.Response
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import constellation.{wrapFutureWithMetric, _}
import org.constellation.consensus.Round.RoundStage.RoundStage
import org.constellation.consensus.Round.StageState.StageState
import org.constellation.consensus.Round._
import org.constellation.consensus.RoundManager.{
  BroadcastLightTransactionProposal,
  BroadcastSelectedUnionBlock,
  BroadcastUnionBlockProposal
}
import org.constellation.p2p.DataResolver
import org.constellation.primitives.Schema.{CheckpointCache, EdgeHashType, SignedObservationEdge, TypedEdgeHash}
import org.constellation.primitives._
import org.constellation.util.{APIClient, PeerApiClient}
import org.constellation.{ConfigUtil, ConstellationExecutionContext, DAO}

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success, Try}

class Round(
  roundData: RoundData,
  arbitraryTransactions: Seq[(Transaction, Int)],
  arbitraryMessages: Seq[(ChannelMessage, Int)],
  dao: DAO,
  dataResolver: DataResolver,
  config: Config
) extends Actor
    with StrictLogging {

  implicit val shadedDao: DAO = dao
  implicit val ec: ExecutionContextExecutor = ConstellationExecutionContext.edge

  private[consensus] val transactionProposals: mutable.Map[FacilitatorId, LightTransactionsProposal] =
    mutable.Map()
  private[consensus] val checkpointBlockProposals: mutable.Map[FacilitatorId, CheckpointBlock] =
    mutable.Map()
  private[consensus] val selectedCheckpointBlocks: mutable.Map[FacilitatorId, CheckpointBlock] =
    mutable.Map()

  private[consensus] var unionTransactionProposalsTikTok: Cancellable = _
  private[consensus] var acceptMajorityCheckpointBlockTikTok: Cancellable = _
  protected var sendArbitraryDataProposalsTikTok: Cancellable = _

  private[consensus] var roundStage: RoundStage = RoundStage.STARTING

  override def preStart(): Unit = {
    super.preStart()
    sendArbitraryDataProposalsTikTok = scheduleArbitraryDataProposals(1)
    unionTransactionProposalsTikTok = scheduleUnionProposals
  }

  private[consensus] var resolveMajorityCheckpointBlockTikTok: Cancellable = _

  private[consensus] var majorityCheckpointBlock: Option[CheckpointBlock] = None

  override def receive: Receive = {
    case StartTransactionProposal(id) if id != roundData.roundId =>
      throw new RuntimeException("Received message from different round.")
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
        transactions
          .map(_.hash) ++ arbitraryTransactions.filter(_._2 == distance).map(_._1.hash),
        messages
          .map(_.map(_.signedMessageData.hash))
          .getOrElse(Seq()) ++ arbitraryMessages
          .filter(_._2 == distance)
          .map(_._1.signedMessageData.hash),
        dao.peerInfo.unsafeRunSync().flatMap(_._2.notification).toSeq
      )
      setRoundStage(RoundStage.WAITING_FOR_PROPOSALS)
      passToParentActor(
        BroadcastLightTransactionProposal(roundData.roundId, roundData.peers, proposal)
      )

      sendArbitraryDataProposalsTikTok = scheduleArbitraryDataProposals(distance + 1)
      self ! proposal

    case newProp: LightTransactionsProposal =>
      logger.debug(
        s"[${dao.id.short}] ${roundData.roundId} received LightTransactionsProposal from ${newProp.facilitatorId.id.short}"
      )

      verifyRoundStage(
        Set(
          RoundStage.WAITING_FOR_BLOCK_PROPOSALS,
          RoundStage.RESOLVING_MAJORITY_CB,
          RoundStage.WAITING_FOR_SELECTED_BLOCKS,
          RoundStage.ACCEPTING_MAJORITY_CB
        )
      )

      if (transactionProposals.contains(newProp.facilitatorId)) {
        logger.error(
          s"[${dao.id.short}] ${roundData.roundId} received doubled LightTransactionProposal is ${newProp.facilitatorId.id.short}"
        )
      }

      val merged = if (transactionProposals.contains(newProp.facilitatorId)) {
        val old = transactionProposals(newProp.facilitatorId)
        old.copy(
          txHashes = old.txHashes ++ newProp.txHashes,
          messages = old.messages ++ newProp.messages,
          notifications = old.notifications ++ newProp.notifications
        )
      } else
        newProp
      transactionProposals += (newProp.facilitatorId -> merged)
      if (receivedAllTransactionProposals) {
        setRoundStage(RoundStage.WAITING_FOR_BLOCK_PROPOSALS)
        logger.debug(s"[${dao.id.short}] ${roundData.roundId} received all transaction proposals")
        cancelUnionTransactionProposalsTikTok()
        self ! UnionProposals(StageState.FINISHED)
      }

    case UnionBlockProposal(roundId, facilitatorId, checkpointBlock) =>
      logger.debug(
        s"[${dao.id.short}] ${roundData.roundId} received UnionBlockProposal from ${facilitatorId.id.short}"
      )

      verifyRoundStage(
        Set(
          RoundStage.RESOLVING_MAJORITY_CB,
          RoundStage.WAITING_FOR_SELECTED_BLOCKS,
          RoundStage.ACCEPTING_MAJORITY_CB
        )
      )

      if (checkpointBlockProposals.contains(facilitatorId)) {
        logger.error(
          s"[${dao.id.short}] ${roundData.roundId} received doubled UnionBlockProposal is ${facilitatorId.id.short}"
        )
      }
      checkpointBlockProposals += (facilitatorId -> checkpointBlock)

//      if (facilitatorId.id != dao.id && !receivedAllTransactionProposals && roundData.peers.size == checkpointBlockProposals
//            .filterNot(_._1 == FacilitatorId(dao.id))
//            .size) {
//        cancelUnionTransactionProposalsTikTok()
//        self ! UnionProposals(StageState.BEHIND)
//      }
      if (receivedAllCheckpointBlockProposals) {
        setRoundStage(RoundStage.RESOLVING_MAJORITY_CB)
        logger.debug(
          s"[${dao.id.short}] ${roundData.roundId} received receivedAllCheckpointBlockProposals"
        )
        cancelResolveMajorityCheckpointBlockTikTok()
        self ! ResolveMajorityCheckpointBlock(roundId, StageState.FINISHED)
      }

    case SelectedUnionBlock(roundId, facilitatorId, checkpointBlock) =>
      logger.debug(
        s"[${dao.id.short}] ${roundData.roundId} received SelectedUnionBlock from ${facilitatorId.id.short}"
      )

      verifyRoundStage(Set(RoundStage.ACCEPTING_MAJORITY_CB))

      if (selectedCheckpointBlocks.contains(facilitatorId)) {
        logger.error(
          s"[${dao.id.short}] ${roundData.roundId} received doubled SelectedUnionBlock is ${facilitatorId.id.short}"
        )
      }
      selectedCheckpointBlocks += (facilitatorId -> checkpointBlock)

//      if (facilitatorId.id != dao.id && !receivedAllCheckpointBlockProposals && roundData.peers.size == selectedCheckpointBlocks
//            .filterNot(_._1 == FacilitatorId(dao.id))
//            .size) {
//        cancelResolveMajorityCheckpointBlockTikTok()
//        self ! ResolveMajorityCheckpointBlock(roundId, StageState.BEHIND)
//      }

      if (receivedAllSelectedUnionedBlocks) {
        setRoundStage(RoundStage.ACCEPTING_MAJORITY_CB)
        logger.debug(
          s"[${dao.id.short}] ${roundData.roundId} received receivedAllSelectedUnionedBlocks"
        )
        cancelUnionTransactionProposalsTikTok()
        cancelAcceptMajorityCheckpointBlockTikTok()
        self ! AcceptMajorityCheckpointBlock(roundId)
      }

    case UnionProposals(StageState.BEHIND) =>
      logger.debug(s"[${dao.id.short}] ${roundData.roundId} self send UnionState")

      verifyRoundStage(
        Set(
          RoundStage.RESOLVING_MAJORITY_CB,
          RoundStage.WAITING_FOR_SELECTED_BLOCKS,
          RoundStage.ACCEPTING_MAJORITY_CB
        )
      )
      unionProposals()

    case UnionProposals(state) =>
      logger.debug(s"[${dao.id.short}] ${roundData.roundId} self send UnionState ${state}")

      verifyRoundStage(
        Set(
          RoundStage.RESOLVING_MAJORITY_CB,
          RoundStage.WAITING_FOR_SELECTED_BLOCKS,
          RoundStage.ACCEPTING_MAJORITY_CB
        )
      )
      validateAndUnionTransactionProposals()

    case ArbitraryDataProposals(distance) =>
      val proposals = transactionProposals.values
      val proposedTxs = proposals.flatMap(_.txHashes).toSet
      val proposedMessages = proposals.flatMap(_.messages).toSet
      val maybeData = {
        (
          arbitraryTransactions
            .filter(a => a._2 == distance && !proposedTxs.contains(a._1.hash))
            .map(_._1.hash),
          arbitraryMessages
            .filter(
              a => a._2 == distance && !proposedMessages.contains(a._1.signedMessageData.hash)
            )
            .map(_._1.signedMessageData.hash)
        )
      }
      maybeData match {
        case (Nil, Nil) =>
        case (txs, msgs) =>
          context.parent ! BroadcastLightTransactionProposal(
            roundData.roundId,
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

    case ResolveMajorityCheckpointBlock(_, StageState.BEHIND) =>
      logger.debug(
        s"[${dao.id.short}] ${roundData.roundId} received ResolveMajorityCheckpointBlock BEHIND"
      )

      verifyRoundStage(
        Set(
          RoundStage.WAITING_FOR_SELECTED_BLOCKS,
          RoundStage.ACCEPTING_MAJORITY_CB
        )
      )
      resolveMajorityCheckpointBlock()

    case ResolveMajorityCheckpointBlock(_, state) =>
      logger.debug(
        s"[${dao.id.short}] ${roundData.roundId} received ResolveMajorityCheckpointBlock state: ${state}"
      )
      dao.metrics.incrementMetric(
        "resolveMajorityCheckpointBlockActor_" + state
      )

      verifyRoundStage(
        Set(
          RoundStage.WAITING_FOR_SELECTED_BLOCKS,
          RoundStage.ACCEPTING_MAJORITY_CB
        )
      )
      validateAndResolveMajorityCheckpointBlock()

    case AcceptMajorityCheckpointBlock(_) =>
      acceptMajorityCheckpointBlock()

    case msg => logger.warn(s"Received unknown message: $msg")
  }

  private[consensus] def scheduleUnionProposals: Cancellable =
    context.system.scheduler.scheduleOnce(
      ConfigUtil.getDurationFromConfig("constellation.consensus.union-proposals-timeout", 15.second, config),
      self,
      UnionProposals(StageState.TIMEOUT)
    )

  private def scheduleArbitraryDataProposals(distance: Int): Cancellable =
    context.system.scheduler.scheduleOnce(
      ConfigUtil.getDurationFromConfig("constellation.consensus.arbitrary-data-proposals-timeout", 4.second, config),
      self,
      ArbitraryDataProposals(distance)
    )

  private[consensus] def scheduleResolveMajorityCheckpointBlock: Cancellable =
    context.system.scheduler.scheduleOnce(
      ConfigUtil.getDurationFromConfig(
        "constellation.consensus.checkpoint-block-resolve-majority-timeout",
        5.second,
        config
      ),
      self,
      ResolveMajorityCheckpointBlock(roundData.roundId, StageState.TIMEOUT)
    )

  private[consensus] def scheduleAcceptMajorityCheckpointBlock: Cancellable =
    context.system.scheduler.scheduleOnce(
      ConfigUtil.getDurationFromConfig(
        "constellation.consensus.accept-resolved-majority-block-timeout",
        5.second,
        config
      ),
      self,
      AcceptMajorityCheckpointBlock(roundData.roundId)
    )

  private def roundStartedByMe: Boolean = roundData.facilitatorId.id == dao.id

  private[consensus] def receivedAllTransactionProposals: Boolean = {
    val extraMessage = if (roundStartedByMe) 1 else 0
    transactionProposals.size == roundData.peers.size + extraMessage
  }

  private[consensus] def receivedAllCheckpointBlockProposals: Boolean =
    checkpointBlockProposals.size == roundData.peers.size + 1

  private[consensus] def receivedAllSelectedUnionedBlocks: Boolean =
    selectedCheckpointBlocks.size == roundData.peers.size + 1

  private[consensus] def cancelUnionTransactionProposalsTikTok(): Unit =
    Option(unionTransactionProposalsTikTok).foreach(_.cancel())

  private[consensus] def cancelResolveMajorityCheckpointBlockTikTok(): Unit =
    Option(resolveMajorityCheckpointBlockTikTok).foreach(_.cancel())

  private[consensus] def cancelAcceptMajorityCheckpointBlockTikTok(): Unit =
    Option(acceptMajorityCheckpointBlockTikTok).foreach(_.cancel())

  private[consensus] def validateAndUnionTransactionProposals(): Unit =
    validateReceivedProposals(transactionProposals.toMap, "unionProposals", countSelfAsPeer = roundStartedByMe) match {
      case Failure(exception) => passToParentActor(exception)
      case Success(_)         => unionProposals()
    }

  private[consensus] def unionProposals(): Unit = {

    val readyPeers = dao.readyPeers
      .map(_.mapValues(p => PeerApiClient(p.peerMetadata.id, p.client)))
      .unsafeRunSync()

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
            .resolveTransactions(p._1, readyPeers.values.toList, readyPeers.get(p._2.facilitatorId.id))
            .map(_.transaction)
      )
      .sequence
      .unsafeRunSync()

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
            .resolveMessages(p._1, readyPeers.values.toList, readyPeers.get(p._2.facilitatorId.id))
            .map(_.channelMessage)
      )
      .sequence
      .unsafeRunSync()

    val notifications = transactionProposals
      .flatMap(_._2.notifications)
      .toSet
      .union(roundData.peers.flatMap(_.notification))
      .toSeq

    val cb = CheckpointBlock.createCheckpointBlock(
      transactions ++ resolvedTxs,
      roundData.tipsSOE.soe
        .map(soe => TypedEdgeHash(soe.hash, EdgeHashType.CheckpointHash, Some(soe.baseHash))),
      messages ++ resolvedMessages,
      notifications
    )(dao.keyPair)

    val blockProposal = UnionBlockProposal(roundData.roundId, FacilitatorId(dao.id), cb)
    passToParentActor(
      BroadcastUnionBlockProposal(roundData.roundId, roundData.peers, blockProposal)
    )
    self ! blockProposal
    resolveMajorityCheckpointBlockTikTok = scheduleResolveMajorityCheckpointBlock
  }

  private[consensus] def validateAndResolveMajorityCheckpointBlock(): Unit =
    validateReceivedProposals(checkpointBlockProposals.toMap, "resolveMajorityCheckpointBlock") match {
      case Failure(exception) => passToParentActor(exception)
      case Success(_)         => resolveMajorityCheckpointBlock()
    }
  private[consensus] def resolveMajorityCheckpointBlock(): Unit = {

    val sameBlocks = checkpointBlockProposals
      .groupBy(_._2.baseHash)
      .maxBy(_._2.size)
      ._2

    val uniques = checkpointBlockProposals.groupBy(_._2.baseHash).size

    dao.metrics.incrementMetric(
      "resolveMajorityCheckpointBlockProposalCount_" + checkpointBlockProposals.size
    )
    dao.metrics.incrementMetric(
      "resolveMajorityCheckpointBlockUniquesCount_" + uniques
    )
    val checkpointBlock = sameBlocks.values.foldLeft(sameBlocks.head._2)(_ + _)
    setRoundStage(RoundStage.WAITING_FOR_SELECTED_BLOCKS)
    val selectedCheckpointBlock =
      SelectedUnionBlock(roundData.roundId, FacilitatorId(dao.id), checkpointBlock)
    passToParentActor(
      BroadcastSelectedUnionBlock(roundData.roundId, roundData.peers, selectedCheckpointBlock)
    )
    acceptMajorityCheckpointBlockTikTok = scheduleAcceptMajorityCheckpointBlock
    self ! selectedCheckpointBlock

  }

  private[consensus] def acceptMajorityCheckpointBlock(): Unit =
    validateReceivedProposals(selectedCheckpointBlocks.toMap, "acceptMajorityCheckpointBlock", 100) match {
      case Failure(exception) => passToParentActor(exception)
      case Success(_) =>
        val sameBlocks = selectedCheckpointBlocks
          .groupBy(_._2.soeHash)
          .maxBy(_._2.size)
          ._2

        val checkpointBlock = sameBlocks.head._2
        val uniques = checkpointBlockProposals.groupBy(_._2.baseHash).size

        dao.metrics.incrementMetric(
          "acceptMajorityCheckpointBlockSelectedCount_" + selectedCheckpointBlocks.size
        )
        dao.metrics.incrementMetric(
          "acceptMajorityCheckpointBlockUniquesCount_" + uniques
        )

        val finalFacilitators = selectedCheckpointBlocks.keySet.map(_.id).toSet
        val cache =
          CheckpointCache(Some(checkpointBlock), height = checkpointBlock.calculateHeight())
        logger.debug(
          s"[${dao.id.short}] accepting majority checkpoint block ${checkpointBlock.baseHash}  " +
            s" with txs ${checkpointBlock.transactions.map(_.hash)} " +
            s"proposed by ${sameBlocks.head._1.id.short} other blocks ${sameBlocks.size} in round ${roundData.roundId} with soeHash ${checkpointBlock.soeHash} and parent ${checkpointBlock.parentSOEHashes} and height ${cache.height}"
        )
        val acceptedBlock = dao.checkpointService
          .accept(cache)
          .map { _ =>
            Option(checkpointBlock)
          }
          .recoverWith {
            case error @ (CheckpointAcceptBlockAlreadyStored(_) | PendingAcceptance(_)) =>
              IO { logger.warn(error.getMessage) } >> IO.pure(None)
            case unknownError =>
              IO {
                logger.error(s"[${dao.id.short}] Failed to accept majority checkpoint block", unknownError)
              } >> IO.pure(None)
          }
          .unsafeRunSync()
        acceptedBlock.foreach(
          _ => broadcastSignedBlockToNonFacilitators(FinishedCheckpoint(cache, finalFacilitators))
        )
        passToParentActor(
          StopBlockCreationRound(
            roundData.roundId,
            acceptedBlock,
            transactionProposals(FacilitatorId(dao.id)).txHashes
              .diff(acceptedBlock.map(_.transactions.map(_.hash)).getOrElse(Seq.empty))
          )
        )
    }

  private[consensus] def broadcastSignedBlockToNonFacilitators(
    finishedCheckpoint: FinishedCheckpoint
  ): Future[List[Response[Unit]]] = {
    val allFacilitators = roundData.peers.map(p => p.peerMetadata.id -> p).toMap
    val signatureResponses = Future.sequence(
      dao.peerInfo
        .unsafeRunSync()
        .values
        .toList
        .filterNot(pd => allFacilitators.contains(pd.peerMetadata.id))
        .map { peer =>
          logger.debug(
            s"[${dao.id.short}] broadcasting cb ${finishedCheckpoint.checkpointCacheData.checkpointBlock.get.baseHash} to  ${peer.client.id.short} in round ${roundData.roundId}"
          )
          wrapFutureWithMetric(
            peer.client.postNonBlockingUnit(
              "finished/checkpoint",
              finishedCheckpoint,
              timeout = 10.seconds,
              Map(
                "ReplyTo" -> APIClient(dao.nodeConfig.hostName, dao.nodeConfig.peerHttpPort)
                  .base("finished/reply")
              )
            ),
            "finishedCheckpointBroadcast"
          )(dao, ec).recoverWith {
            case e: Throwable =>
              logger.warn("Failure gathering signature", e)
              dao.metrics.incrementMetric(
                "formCheckpointSignatureResponseError"
              )
              Future.failed(e)
          }
        }
    )

    wrapFutureWithMetric(signatureResponses, "checkpointBlockFormation")(dao, ec)
  }

  private[consensus] def passToParentActor(cmd: Any): Unit =
    context.parent ! cmd

  private[consensus] def getOwnTransactionsToReturn() =
    transactionProposals.get(FacilitatorId(dao.id)).map(_.txHashes).getOrElse(Seq.empty)

  def validateReceivedProposals(
    proposals: Map[FacilitatorId, AnyRef],
    stage: String,
    minimumPercentage: Int = 51,
    countSelfAsPeer: Boolean = true
  ): Try[Unit] = {
    val peerSize = roundData.peers.size + (if (countSelfAsPeer) 1 else 0)
    val proposalPercentage: Float = proposals.size * 100 / peerSize
    (proposalPercentage, proposals.size) match {
      case (0, _) =>
        Failure(EmptyProposals(roundData.roundId, stage, getOwnTransactionsToReturn()))
      case (_, size) if size == 1 =>
        Failure(EmptyProposals(roundData.roundId, stage, getOwnTransactionsToReturn()))
      case (p, _) if p < minimumPercentage =>
        Failure(
          NotEnoughProposals(roundData.roundId, proposals.size, peerSize, stage, getOwnTransactionsToReturn())
        )
      case _ => Success(())
    }
  }

  def setRoundStage(stage: RoundStage): Unit =
    roundStage = stage

  def verifyRoundStage(forbiddenStages: Set[RoundStage]): Unit =
    if (forbiddenStages.contains(roundStage)) {
      throw new RuntimeException(s"Received message from previous round stage. Current round stage is $roundStage")
    }
}

case class FacilitatorId(id: Schema.Id) extends AnyVal
case class RoundId(id: String) extends AnyVal

object Round {
  sealed trait RoundCommand {
    def roundId: RoundId
  }
  abstract class RoundException(msg: String) extends Exception(msg) {
    def roundId: RoundId
    def transactionsToReturn: Seq[String]
  }

  object RoundStage extends Enumeration {
    type RoundStage = Value

    val STARTING, WAITING_FOR_PROPOSALS, WAITING_FOR_BLOCK_PROPOSALS, RESOLVING_MAJORITY_CB,
      WAITING_FOR_SELECTED_BLOCKS, ACCEPTING_MAJORITY_CB =
      Value
  }

  object StageState extends Enumeration {
    type StageState = Value
    val TIMEOUT, BEHIND, FINISHED = Value
  }

  case class UnionProposals(state: StageState)
  case class ArbitraryDataProposals(distance: Int)

  case class ResolveMajorityCheckpointBlock(roundId: RoundId, stageState: StageState) extends RoundCommand

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
    transactions: List[Transaction],
    tipsSOE: TipSoe,
    messages: Seq[ChannelMessage]
  )

  case class StopBlockCreationRound(
    roundId: RoundId,
    maybeCB: Option[CheckpointBlock],
    transactionsToReturn: Seq[String]
  ) extends RoundCommand

  case class EmptyProposals(roundId: RoundId, stage: String, transactionsToReturn: Seq[String])
      extends RoundException(s"Proposals for stage: $stage and round: $roundId are empty.")
  case class NotEnoughProposals(
    roundId: RoundId,
    proposals: Int,
    facilitators: Int,
    stage: String,
    transactionsToReturn: Seq[String]
  ) extends RoundException(
        s"Proposals number: $proposals for stage: $stage and round: $roundId are below given percentage. Number of facilitators: $facilitators"
      )

  case class SelectedUnionBlock(
    roundId: RoundId,
    facilitatorId: FacilitatorId,
    checkpointBlock: CheckpointBlock
  ) extends RoundCommand

  def props(
    roundData: RoundData,
    arbitraryTransactions: Seq[(Transaction, Int)],
    arbitraryMessages: Seq[(ChannelMessage, Int)],
    dao: DAO,
    dataResolver: DataResolver,
    config: Config
  ): Props =
    Props(
      new Round(roundData, arbitraryTransactions, arbitraryMessages, dao, dataResolver, config)
    )
}