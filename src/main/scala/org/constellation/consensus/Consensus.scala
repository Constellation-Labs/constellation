package org.constellation.consensus

import cats.effect.concurrent.{Ref, Semaphore}
import cats.effect.{Blocker, Bracket, Concurrent, ContextShift, IO, LiftIO, Sync, Timer}
import cats.syntax.all._
import com.typesafe.config.Config
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto._
import org.constellation.checkpoint.CheckpointService
import org.constellation.consensus.Consensus.ConsensusStage.ConsensusStage
import org.constellation.consensus.Consensus.StageState.StageState
import org.constellation.consensus.Consensus._
import org.constellation.consensus.ConsensusManager.{BroadcastConsensusDataProposal, BroadcastSelectedUnionBlock, BroadcastUnionBlockProposal}
import org.constellation.domain.consensus.ConsensusStatus
import org.constellation.domain.observation.ObservationService
import org.constellation.p2p.PeerData
import org.constellation.schema.edge.{EdgeHashType, TypedEdgeHash}
import org.constellation.domain.transaction.TransactionService
import org.constellation.infrastructure.p2p.{ClientInterpreter, PeerResponse}
import org.constellation.schema.checkpoint.{CheckpointBlock, CheckpointBlockPayload, CheckpointCache, FinishedCheckpoint, FinishedCheckpointBlock}
import org.constellation.schema.consensus.RoundId
import org.constellation.schema.observation.Observation
import org.constellation.schema.transaction.{Transaction, TransactionCacheData}
import org.constellation.schema.{ChannelMessage, Id, NodeState, PeerNotification}
import org.constellation.storage._
import org.constellation.{CheckpointAcceptBlockAlreadyStored, ConfigUtil, ContainsInvalidTransactionsException, DAO, MissingParents, MissingTransactionReference, PendingAcceptance}
import org.constellation.ConstellationExecutionContext.createSemaphore
import org.constellation.gossip.checkpoint.CheckpointBlockGossipService
import org.constellation.schema.signature.Signed.signed

import scala.concurrent.duration._

class Consensus[F[_]: Concurrent: ContextShift](
  roundData: RoundData,
  arbitraryMessages: Seq[(ChannelMessage, Int)],
  transactionService: TransactionService[F],
  checkpointService: CheckpointService[F],
  messageService: MessageService[F],
  observationService: ObservationService[F],
  remoteSender: ConsensusRemoteSender[F],
  consensusManager: ConsensusManager[F],
  apiClient: ClientInterpreter[F],
  checkpointBlockGossipService: CheckpointBlockGossipService[F],
  dao: DAO,
  config: Config,
  remoteCall: Blocker,
  calculationContext: ContextShift[F]
) {

  val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]

  implicit val shadowDAO: DAO = dao

  val updateSemaphore: Semaphore[F] = createSemaphore[F](1)

  private[consensus] val consensusDataProposals: Ref[F, Map[FacilitatorId, ConsensusDataProposal]] =
    Ref.unsafe(Map.empty[FacilitatorId, ConsensusDataProposal])
  private[consensus] val checkpointBlockProposals: Ref[F, Map[FacilitatorId, CheckpointBlock]] =
    Ref.unsafe(Map.empty[FacilitatorId, CheckpointBlock])
  private[consensus] val selectedCheckpointBlocks: Ref[F, Map[FacilitatorId, CheckpointBlock]] =
    Ref.unsafe(Map.empty[FacilitatorId, CheckpointBlock])

  private[consensus] val stage: Ref[F, ConsensusStage] = Ref.unsafe(ConsensusStage.STARTING)

  private def withLock[R](thunk: => F[R]): F[R] =
    Bracket[F, Throwable].bracket(updateSemaphore.acquire)(_ => thunk)(_ => updateSemaphore.release)

  def startConsensusDataProposal(): F[Unit] =
    for {
      transactions <- transactionService
        .pullForConsensus(ConfigUtil.constellation.getInt("consensus.maxTransactionThreshold"))
        .map(_.map(_.transaction))
      _ <- logger
        .info(s"Pulled for participating consensus: ${transactions.size}")
      messages <- Sync[F].delay(dao.threadSafeMessageMemPool.pull())
      notifications <- LiftIO[F].liftIO(dao.peerInfo.map(_.values.flatMap(_.notification).toSeq))
      observations <- observationService.pullForConsensus(
        ConfigUtil.constellation.getInt("consensus.maxObservationThreshold")
      )
      proposal = ConsensusDataProposal(
        roundData.roundId,
        FacilitatorId(dao.id),
        transactions,
        messages
          .map(_.map(_.signedMessageData.hash))
          .getOrElse(Seq()) ++ arbitraryMessages
          .filter(_._2 == 0)
          .map(_._1.signedMessageData.hash),
        notifications,
        observations
      )
      _ <- remoteSender.broadcastConsensusDataProposal(
        BroadcastConsensusDataProposal(roundData.roundId, roundData.peers, proposal)
      )
      _ <- addConsensusDataProposal(proposal)
    } yield ()

  def addBlockProposal(proposal: UnionBlockProposal): F[Unit] =
    for {
      _ <- verifyStage(
        Set(
          ConsensusStage.RESOLVING_MAJORITY_CB,
          ConsensusStage.WAITING_FOR_SELECTED_BLOCKS,
          ConsensusStage.ACCEPTING_MAJORITY_CB
        )
      )
      receivedAllBlockProposals <- checkpointBlockProposals.modify { curr =>
        val updated = curr + (proposal.facilitatorId -> proposal.checkpointBlock)
        (updated, receivedAllCheckpointBlockProposals(updated.size))
      }
      _ <- logger.debug(s"[${dao.id.short}] ${roundData.roundId} received block proposal $receivedAllBlockProposals")
      _ <- if (receivedAllBlockProposals)
        stage
          .modify(_ => (ConsensusStage.RESOLVING_MAJORITY_CB, ()))
          .flatMap(_ => validateAndMergeBlockProposals())
      else Sync[F].unit
    } yield ()

  def addConsensusDataProposal(proposal: ConsensusDataProposal): F[Unit] =
    for {
      _ <- verifyStage(
        Set(
          ConsensusStage.WAITING_FOR_BLOCK_PROPOSALS,
          ConsensusStage.RESOLVING_MAJORITY_CB,
          ConsensusStage.WAITING_FOR_SELECTED_BLOCKS,
          ConsensusStage.ACCEPTING_MAJORITY_CB
        )
      )

      _ <- storeProposal(proposal)

      receivedAllConsensusDataProposals <- withLock(consensusDataProposals.modify { curr =>
        val merged = if (curr.contains(proposal.facilitatorId)) {
          val old = curr(proposal.facilitatorId)
          old.copy(
            transactions = old.transactions ++ proposal.transactions,
            messages = old.messages ++ proposal.messages,
            notifications = old.notifications ++ proposal.notifications,
            observations = old.observations ++ proposal.observations
          )
        } else
          proposal
        val updated = curr + (proposal.facilitatorId -> merged)
        (updated, receivedAllConsensusDataProposals(updated.size))
      })
      _ <- logger.debug(
        s"[${dao.id.short}] ${roundData.roundId} received consensus data proposal $receivedAllConsensusDataProposals"
      )

      _ <- if (receivedAllConsensusDataProposals)
        stage
          .modify(_ => (ConsensusStage.WAITING_FOR_BLOCK_PROPOSALS, ()))
          .flatMap(_ => unionConsensusDataProposals(StageState.FINISHED))
      else Sync[F].unit
    } yield ()

  private def storeProposal(proposal: ConsensusDataProposal): F[Unit] =
    for {
      txs <- (roundData.transactions ++ proposal.transactions).pure[F]
      existingTxs <- txs
        .traverse(tx => transactionService.lookup(tx.hash))
        .map(_.flatten.map(_.hash).toList)
      leftTxs = txs.filterNot(tx => existingTxs.contains(tx.hash))
      _ <- leftTxs.traverse(tx => transactionService.put(TransactionCacheData(tx), ConsensusStatus.Unknown))

      obs = roundData.observations ++ proposal.observations
      existingObs <- obs
        .traverse(tx => observationService.lookup(tx.hash))
        .map(_.flatten.map(_.hash).toList)
      leftObs = obs.filterNot(o => existingObs.contains(o.hash))

      _ <- leftObs.traverse(o => observationService.put(o, ConsensusStatus.Unknown))

      // TODO: store messages and notifications
    } yield ()

  def unionConsensusDataProposals(stageState: StageState): F[Unit] = {
    val action = stageState match {
      case StageState.BEHIND => mergeConsensusDataProposalsAndBroadcastBlock()
      case _                 => validateAndMergeConsensusDataProposals()
    }
    verifyStage(
      Set(
        ConsensusStage.RESOLVING_MAJORITY_CB,
        ConsensusStage.WAITING_FOR_SELECTED_BLOCKS,
        ConsensusStage.ACCEPTING_MAJORITY_CB
      )
    ).flatTap(_ => action)
  }

  private[consensus] def validateAndMergeBlockProposals(): F[Unit] =
    for {
      proposals <- withLock(checkpointBlockProposals.get)
      validationResult <- validateReceivedProposals(proposals, "blockProposals")
      _ <- validationResult match {
        case Left(exception) => consensusManager.handleRoundError(exception)
        case Right(_)        => mergeBlockProposalsToMajorityBlock(proposals)
      }
    } yield ()

  private[consensus] def validateAndAcceptMajorityBlockProposals(): F[Unit] =
    for {
      proposals <- withLock(selectedCheckpointBlocks.get)
      _ <- logger.debug("validate majority block proposal")
      validationResult <- validateReceivedProposals(proposals, "majorityProposals", 100)
      _ <- validationResult match {
        case Left(exception) => consensusManager.handleRoundError(exception)
        case Right(_)        => acceptMajorityCheckpointBlock(proposals)
      }
    } yield ()

  def addSelectedBlockProposal(proposal: SelectedUnionBlock): F[Unit] =
    for {
      _ <- verifyStage(Set(ConsensusStage.ACCEPTING_MAJORITY_CB))

      receivedAllSelectedProposals <- withLock(selectedCheckpointBlocks.modify { curr =>
        val updated = curr + (proposal.facilitatorId -> proposal.checkpointBlock)
        (updated, receivedAllSelectedUnionBlocks(updated.size))
      })
      _ <- logger.debug(
        s"[${dao.id.short}] ${roundData.roundId} received selected proposal $receivedAllSelectedProposals"
      )
      _ <- if (receivedAllSelectedProposals)
        stage
          .modify(_ => (ConsensusStage.ACCEPTING_MAJORITY_CB, ()))
          .flatTap(_ => validateAndAcceptMajorityBlockProposals())
      else Sync[F].unit
    } yield ()

  private[consensus] def acceptMajorityCheckpointBlock(proposals: Map[FacilitatorId, CheckpointBlock]): F[Unit] = {

    val sameBlocks = proposals
      .groupBy(_._2.soeHash)
      .maxBy(_._2.size)
      ._2

    val checkpointBlock = sameBlocks.head._2 // TODO: unsafe
    val uniques = proposals.groupBy(_._2.baseHash).size

    for {
      maybeHeight <- checkpointService.calculateHeight(checkpointBlock)
      cache = CheckpointCache(checkpointBlock, height = maybeHeight)
      _ <- logger.debug(s"Unique to accept: ${proposals.groupBy(_._2.baseHash).keys}")
      _ <- dao.metrics.incrementMetricAsync(
        "acceptMajorityCheckpointBlockSelectedCount_" + proposals.size
      )
      _ <- dao.metrics.incrementMetricAsync(
        "acceptMajorityCheckpointBlockUniquesCount_" + uniques
      )
      _ <- logger.debug(
        s"[${dao.id.short}] accepting majority checkpoint block ${checkpointBlock.baseHash}  " +
          s" with txs ${checkpointBlock.transactions.map(_.hash)} " +
          s" with obs ${checkpointBlock.observations.map(_.hash)} " +
          s"proposed by ${sameBlocks.head._1.id.short} other blocks ${sameBlocks.size} in round ${roundData.roundId} with soeHash ${checkpointBlock.soeHash} and parent ${checkpointBlock.parentSOEHashes} and height ${cache.height}"
      )

      finalResult <- checkpointService
        .accept(cache)
        .map(_ => ConsensusFinalResult(Option(checkpointBlock)))
        .handleErrorWith {
          case error @ (CheckpointAcceptBlockAlreadyStored(_) | PendingAcceptance(_)) =>
            logger.warn(error.getMessage) >> ConsensusFinalResult(None).pure[F]
          case error @ MissingTransactionReference(cb) =>
            logger.warn(error.getMessage) >> ConsensusFinalResult(None).pure[F]
          case error @ MissingParents(cb) =>
            logger.warn(error.getMessage) >> ConsensusFinalResult(None).pure[F]
          case tipConflict: TipConflictException =>
            logger.error(tipConflict)(
              s"[${dao.id.short}] Failed to accept majority checkpoint block due: ${tipConflict.getMessage}"
            ) >> ConsensusFinalResult(None, true, tipConflict.conflictingTxs).pure[F]
          case containsInvalidTransactions: ContainsInvalidTransactionsException =>
            logger.error(containsInvalidTransactions)(
              s"[${dao.id.short}] Failed to accept majority checkpoint block due: ${containsInvalidTransactions.getMessage}"
            ) >> ConsensusFinalResult(None, shouldReturnData = true, containsInvalidTransactions.txsToExclude).pure[F]
          case unknownError =>
            logger.error(unknownError)(
              s"[${dao.id.short}] Failed to accept majority checkpoint block due: ${unknownError.getMessage}"
            ) >> ConsensusFinalResult(None, shouldReturnData = true).pure[F]
        }

      _ <- if (finalResult.cb.isEmpty) {
        Concurrent[F].unit
      } else {
        broadcastSignedBlockToNonFacilitators(
          FinishedCheckpoint(cache, proposals.keySet.map(_.id))
        )
      }

      _ <- logger.debug(s"[Accept checkpoint] Txs hashes to exclude : ${finalResult.txsToExclude}")
      transactionsToReturn <- if (finalResult.shouldReturnData) {
        getOwnTransactionsToReturn.map(
          txs =>
            txs
              .diff(finalResult.cb.map(_.transactions).getOrElse(Seq.empty))
              .filterNot(tx => finalResult.txsToExclude.contains(tx.hash))
        )
      } else Seq.empty[Transaction].pure[F]

      observationsToReturn <- if (finalResult.shouldReturnData) {
        getOwnObservationsToReturn.map(
          _.diff(finalResult.cb.map(_.observations).getOrElse(Seq.empty))
        )
      } else Seq.empty[Observation].pure[F]

      _ <- consensusManager.stopBlockCreationRound(
        StopBlockCreationRound(
          roundData.roundId,
          finalResult.cb,
          transactionsToReturn,
          observationsToReturn
        )
      )

      _ <- logger.debug(
        s"[${dao.id.short}] round stopped ${roundData.roundId} block is empty ? ${finalResult.cb.isEmpty}"
      )

    } yield ()

  }

  private[consensus] def broadcastSignedBlockToNonFacilitators(
    finishedCheckpoint: FinishedCheckpoint
  ): F[Unit] = {
    val allFacilitators = roundData.peers.map(p => p.peerMetadata.id -> p).toMap
    for {
      nonFacilitators <- LiftIO[F]
        .liftIO(dao.peerInfo)
        .map(
          info =>
            info.values.toList
              .filter(pd => NodeState.isNotOffline(pd.peerMetadata.nodeState))
              .filterNot(pd => allFacilitators.contains(pd.peerMetadata.id))
        )
      baseHash = finishedCheckpoint.checkpointCacheData.checkpointBlock.baseHash
      _ <- logger.debug(
        s"[${dao.id.short}] ${roundData.roundId} Broadcasting checkpoint block with baseHash ${baseHash}"
      )

      payload = CheckpointBlockPayload(
        signed(FinishedCheckpointBlock(
          finishedCheckpoint.checkpointCacheData, finishedCheckpoint.facilitators
        ), dao.keyPair))

      _ <- Concurrent[F].start(checkpointBlockGossipService.spread(payload))
    } yield ()
  }

  private[consensus] def mergeBlockProposalsToMajorityBlock(
    proposals: Map[FacilitatorId, CheckpointBlock]
  ): F[Unit] = {
    val sameBlocks = proposals
      .groupBy(_._2.baseHash)
      .maxBy(_._2.size)
      ._2

    val uniques = proposals.groupBy(_._2.baseHash).size

    val checkpointBlock = sameBlocks.values.reduce((a, b) => a.plusEdge(b))
    val selectedCheckpointBlock = SelectedUnionBlock(roundData.roundId, FacilitatorId(dao.id), checkpointBlock)

    for {
      _ <- stage.modify(_ => (ConsensusStage.WAITING_FOR_SELECTED_BLOCKS, ()))
      _ <- logger.debug(s"Unique in resolve: ${proposals.groupBy(_._2.baseHash).keys}")
      _ <- dao.metrics.incrementMetricAsync(
        "resolveMajorityCheckpointBlockProposalCount_" + proposals.size
      )
      _ <- dao.metrics.incrementMetricAsync(
        "resolveMajorityCheckpointBlockUniquesCount_" + uniques
      )

      _ <- remoteSender.broadcastSelectedUnionBlock(
        BroadcastSelectedUnionBlock(roundData.roundId, roundData.peers, selectedCheckpointBlock)
      )
      _ <- addSelectedBlockProposal(selectedCheckpointBlock)
    } yield ()
  }

  private[consensus] def mergeConsensusDataProposalsAndBroadcastBlock(): F[Unit] =
    for {
      proposals <- withLock(consensusDataProposals.get)

      messages = List.empty[ChannelMessage]

      notifications = proposals
        .flatMap(_._2.notifications)
        .toSet
        .union(roundData.peers.flatMap(_.notification))
        .toSeq

      proposal = UnionBlockProposal(
        roundData.roundId,
        FacilitatorId(dao.id),
        CheckpointBlock.createCheckpointBlock(
          (roundData.transactions ++ proposals.flatMap(_._2.transactions)),
          roundData.tipsSOE.soe
            .map(soe => TypedEdgeHash(soe.hash, EdgeHashType.CheckpointHash, Some(soe.baseHash))),
          messages,
          notifications,
          (roundData.observations ++ proposals.flatMap(_._2.observations))
        )(dao.keyPair)
      )
      _ <- remoteSender.broadcastBlockUnion(
        BroadcastUnionBlockProposal(roundData.roundId, roundData.peers, proposal)
      )
      _ <- addBlockProposal(proposal)
    } yield ()

  private[consensus] def validateAndMergeConsensusDataProposals(): F[Unit] =
    for {
      proposals <- withLock(consensusDataProposals.get)
      validationResult <- validateReceivedProposals(
        proposals,
        "consensusDataProposals",
        countSelfAsPeer = false
      )
      _ <- validationResult match {
        case Left(exception) => consensusManager.handleRoundError(exception)
        case Right(_)        => mergeConsensusDataProposalsAndBroadcastBlock()
      }
    } yield ()

  def verifyStage(forbiddenStages: Set[ConsensusStage]): F[Unit] =
    stage.get
      .flatMap(
        stage =>
          if (forbiddenStages.contains(stage))
            getOwnTransactionsToReturn
              .flatMap(
                txs =>
                  getOwnObservationsToReturn.flatMap(
                    exs => consensusManager.handleRoundError(PreviousStage(roundData.roundId, stage, txs, exs))
                  )
              )
          else Sync[F].unit
      )

  private[consensus] def getOwnTransactionsToReturn: F[Seq[Transaction]] =
    withLock(consensusDataProposals.get).map(_.get(FacilitatorId(dao.id)).map(_.transactions).getOrElse(Seq.empty))

  private[consensus] def getOwnObservationsToReturn: F[Seq[Observation]] =
    withLock(consensusDataProposals.get).map(_.get(FacilitatorId(dao.id)).map(_.observations).getOrElse(Seq.empty))

  private def roundStartedByMe: Boolean = roundData.facilitatorId.id == dao.id

  private[consensus] def receivedAllSelectedUnionBlocks(size: Int): Boolean =
    size == roundData.peers.size + 1

  private[consensus] def receivedAllCheckpointBlockProposals(size: Int): Boolean =
    size == roundData.peers.size + 1

  private[consensus] def receivedAllConsensusDataProposals(size: Int): Boolean =
    size == roundData.peers.size

  def validateReceivedProposals(
    proposals: Map[FacilitatorId, AnyRef],
    stage: String,
    minimumPercentage: Int = 51,
    countSelfAsPeer: Boolean = true
  ): F[Either[ConsensusException, Unit]] = {
    val peerSize = roundData.peers.size + (if (countSelfAsPeer) 1 else 0)
    val proposalPercentage: Float = proposals.size * 100 / peerSize
    (proposalPercentage, proposals.size) match {
      case (percentage, size) if percentage == 0 || size == 1 =>
        getOwnTransactionsToReturn.flatMap(
          txs => getOwnObservationsToReturn.map(obs => Left(EmptyProposals(roundData.roundId, stage, txs, obs)))
        )
      case (p, _) if p < minimumPercentage =>
        getOwnTransactionsToReturn.flatMap(
          txs =>
            getOwnObservationsToReturn.map(
              obs =>
                Left(
                  NotEnoughProposals(roundData.roundId, proposals.size, peerSize, stage, txs, obs)
                )
            )
        )
      case _ => Sync[F].pure(Right(()))
    }
  }

}

object Consensus {
  sealed trait ConsensusProposal {
    def roundId: RoundId
  }

  abstract class ConsensusException(msg: String) extends Exception(msg) {
    def roundId: RoundId
    def transactionsToReturn: Seq[Transaction]
    def observationsToReturn: Seq[Observation]
  }

  case class ConsensusFinalResult(
    cb: Option[CheckpointBlock],
    shouldReturnData: Boolean = false,
    txsToExclude: List[String] = List.empty[String]
  )

  object ConsensusStage extends Enumeration {
    type ConsensusStage = Value

    val STARTING, WAITING_FOR_PROPOSALS, WAITING_FOR_BLOCK_PROPOSALS, RESOLVING_MAJORITY_CB,
      WAITING_FOR_SELECTED_BLOCKS, ACCEPTING_MAJORITY_CB =
      Value
  }

  object StageState extends Enumeration {
    type StageState = Value
    val TIMEOUT, BEHIND, FINISHED = Value
  }

  case class FacilitatorId(id: Id) extends AnyVal

  object FacilitatorId {
    implicit val facilitatorIdEncoder: Encoder[FacilitatorId] = deriveEncoder
    implicit val facilitatorIdDecoder: Decoder[FacilitatorId] = deriveDecoder
  }

  case class UnionProposals(state: StageState)

  case class ResolveMajorityCheckpointBlock(roundId: RoundId, stageState: StageState)

  case class AcceptMajorityCheckpointBlock(roundId: RoundId)

  case class StartConsensusDataProposal(roundId: RoundId)

  case class ConsensusDataProposal(
    roundId: RoundId,
    facilitatorId: FacilitatorId,
    transactions: Seq[Transaction],
    messages: Seq[String] = Seq(),
    notifications: Seq[PeerNotification] = Seq(),
    observations: Seq[Observation] = Seq.empty[Observation]
  ) extends ConsensusProposal

  object ConsensusDataProposal {
    implicit val consensusDataProposalDecoder: Decoder[ConsensusDataProposal] = deriveDecoder
    implicit val consensusDataProposalEncoder: Encoder[ConsensusDataProposal] = deriveEncoder
  }

  case class UnionBlockProposal(
    roundId: RoundId,
    facilitatorId: FacilitatorId,
    checkpointBlock: CheckpointBlock
  ) extends ConsensusProposal

  object UnionBlockProposal {
    implicit val unionBlockProposalDecoder: Decoder[UnionBlockProposal] = deriveDecoder
    implicit val unionBlockProposalEncoder: Encoder[UnionBlockProposal] = deriveEncoder
  }

  case class RoundData(
    roundId: RoundId,
    peers: Set[PeerData],
    lightPeers: Set[PeerData],
    facilitatorId: FacilitatorId,
    transactions: List[Transaction],
    tipsSOE: TipSoe,
    messages: Seq[ChannelMessage],
    observations: List[Observation]
  )

  case class StopBlockCreationRound(
    roundId: RoundId,
    maybeCB: Option[CheckpointBlock],
    transactionsToReturn: Seq[Transaction],
    observationsToReturn: Seq[Observation]
  )

  case class EmptyProposals(
    roundId: RoundId,
    stage: String,
    transactionsToReturn: Seq[Transaction],
    observationsToReturn: Seq[Observation]
  ) extends ConsensusException(s"Proposals for stage: $stage and round: $roundId are empty.")

  case class PreviousStage(
    roundId: RoundId,
    stage: ConsensusStage,
    transactionsToReturn: Seq[Transaction],
    observationsToReturn: Seq[Observation]
  ) extends ConsensusException(s"Received message from previous round stage. Current round stage is $stage")

  case class NotEnoughProposals(
    roundId: RoundId,
    proposals: Int,
    facilitators: Int,
    stage: String,
    transactionsToReturn: Seq[Transaction],
    observationsToReturn: Seq[Observation]
  ) extends ConsensusException(
        s"Proposals number: $proposals for stage: $stage and round: $roundId are below given percentage. Number of facilitators: $facilitators"
      )

  case class SelectedUnionBlock(
    roundId: RoundId,
    facilitatorId: FacilitatorId,
    checkpointBlock: CheckpointBlock
  ) extends ConsensusProposal

  object SelectedUnionBlock {
    implicit val selectedUnionBlockDecoder: Decoder[SelectedUnionBlock] = deriveDecoder
    implicit val selectedUnionBlockEncoder: Encoder[SelectedUnionBlock] = deriveEncoder
  }

}
