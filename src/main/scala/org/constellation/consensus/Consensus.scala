package org.constellation.consensus

import cats.effect.{Concurrent, ContextShift, IO, LiftIO, Sync}
import cats.implicits._
import com.softwaremill.sttp.Response
import com.typesafe.config.Config
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.checkpoint.CheckpointAcceptanceService
import org.constellation.consensus.Consensus.ConsensusStage.ConsensusStage
import org.constellation.consensus.Consensus.StageState.StageState
import org.constellation.consensus.Consensus._
import org.constellation.consensus.ConsensusManager.{
  BroadcastLightTransactionProposal,
  BroadcastSelectedUnionBlock,
  BroadcastUnionBlockProposal
}
import org.constellation.p2p.{DataResolver, PeerData, PeerNotification}
import org.constellation.primitives.Schema.{CheckpointCache, EdgeHashType, TypedEdgeHash}
import org.constellation.domain.schema.Id
import org.constellation.primitives._
import org.constellation.primitives.concurrency.SingleRef
import org.constellation.storage._
import org.constellation.util.PeerApiClient
import org.constellation.{ConstellationExecutionContext, DAO}

import scala.concurrent.duration._

class Consensus[F[_]: Concurrent](
  roundData: RoundData,
  arbitraryTransactions: Seq[(Transaction, Int)],
  arbitraryMessages: Seq[(ChannelMessage, Int)],
  dataResolver: DataResolver,
  transactionService: TransactionService[F],
  checkpointAcceptanceService: CheckpointAcceptanceService[F],
  messageService: MessageService[F],
  observationService: ObservationService[F],
  remoteSender: ConsensusRemoteSender[F],
  consensusManager: ConsensusManager[F],
  dao: DAO,
  config: Config,
  remoteCall: ContextShift[F],
  calculationContext: ContextShift[F]
) {

  val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]

  implicit val contextShift
    : ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.bounded) // TODO: wkoszycki apply from calculationContext[F]

  implicit val shadowDAO: DAO = dao

  private[consensus] val transactionProposals: SingleRef[F, Map[FacilitatorId, LightTransactionsProposal]] =
    SingleRef(Map.empty[FacilitatorId, LightTransactionsProposal])
  private[consensus] val checkpointBlockProposals: SingleRef[F, Map[FacilitatorId, CheckpointBlock]] =
    SingleRef(Map.empty[FacilitatorId, CheckpointBlock])
  private[consensus] val selectedCheckpointBlocks: SingleRef[F, Map[FacilitatorId, CheckpointBlock]] =
    SingleRef(Map.empty[FacilitatorId, CheckpointBlock])

  private[consensus] val stage: SingleRef[F, ConsensusStage] = SingleRef(ConsensusStage.STARTING)

  def startTransactionProposal(): F[Unit] =
    for {
      transactions <- transactionService
        .pullForConsensus(dao.processingConfig.maxCheckpointFormationThreshold)
        .map(_.map(_.transaction))
      messages <- Sync[F].delay(dao.threadSafeMessageMemPool.pull())
      notifications <- LiftIO[F].liftIO(dao.peerInfo.map(_.values.flatMap(_.notification).toSeq))
      observations <- observationService.pullForConsensus(1) // TODO: adjust size
      proposal = LightTransactionsProposal(
        roundData.roundId,
        FacilitatorId(dao.id),
        transactions
          .map(_.hash) ++ arbitraryTransactions.filter(_._2 == 0).map(_._1.hash),
        messages
          .map(_.map(_.signedMessageData.hash))
          .getOrElse(Seq()) ++ arbitraryMessages
          .filter(_._2 == 0)
          .map(_._1.signedMessageData.hash),
        notifications,
        observations.map(_.hash)
      )
      _ <- remoteCall.shift >> remoteSender.broadcastLightTransactionProposal(
        BroadcastLightTransactionProposal(roundData.roundId, roundData.peers, proposal)
      )
      _ <- addTransactionProposal(proposal)
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
          .set(ConsensusStage.RESOLVING_MAJORITY_CB)
          .flatMap(_ => validateAndMergeBlockProposals())
      else Sync[F].unit
    } yield ()

  def addTransactionProposal(proposal: LightTransactionsProposal): F[Unit] =
    for {
      _ <- verifyStage(
        Set(
          ConsensusStage.WAITING_FOR_BLOCK_PROPOSALS,
          ConsensusStage.RESOLVING_MAJORITY_CB,
          ConsensusStage.WAITING_FOR_SELECTED_BLOCKS,
          ConsensusStage.ACCEPTING_MAJORITY_CB
        )
      )
      receivedAllTransactionProposals <- transactionProposals.modify { curr =>
        val merged = if (curr.contains(proposal.facilitatorId)) {
          val old = curr(proposal.facilitatorId)
          old.copy(
            txHashes = old.txHashes ++ proposal.txHashes,
            messages = old.messages ++ proposal.messages,
            notifications = old.notifications ++ proposal.notifications
          )
        } else
          proposal
        val updated = curr + (proposal.facilitatorId -> merged)
        (updated, receivedAllTransactionProposals(updated.size))
      }
      _ <- logger.debug(
        s"[${dao.id.short}] ${roundData.roundId} received transaction proposal $receivedAllTransactionProposals"
      )
      _ <- if (receivedAllTransactionProposals)
        stage
          .set(ConsensusStage.WAITING_FOR_BLOCK_PROPOSALS)
          .flatMap(_ => unionTransactionProposals(StageState.FINISHED))
      else Sync[F].unit
    } yield ()

  def unionTransactionProposals(stageState: StageState): F[Unit] = {
    val action = stageState match {
      case StageState.BEHIND => mergeTxProposalsAndBroadcastBlock()
      case _                 => validateAndMergeTransactionProposals()
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
      proposals <- checkpointBlockProposals.getUnsafe
      validationResult <- validateReceivedProposals(proposals, "blockProposals")
      _ <- validationResult match {
        case Left(exception) => consensusManager.handleRoundError(exception)
        case Right(_)        => mergeBlockProposalsToMajorityBlock(proposals)
      }
    } yield ()

  private[consensus] def validateAndAcceptMajorityBlockProposals(): F[Unit] =
    for {
      proposals <- selectedCheckpointBlocks.getUnsafe
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

      receivedAllSelectedProposals <- selectedCheckpointBlocks.modify { curr =>
        val updated = curr + (proposal.facilitatorId -> proposal.checkpointBlock)
        (updated, receivedAllSelectedUnionBlocks(updated.size))
      }
      _ <- logger.debug(
        s"[${dao.id.short}] ${roundData.roundId} received selected proposal $receivedAllSelectedProposals"
      )
      _ <- if (receivedAllSelectedProposals)
        stage
          .set(ConsensusStage.ACCEPTING_MAJORITY_CB)
          .flatTap(_ => validateAndAcceptMajorityBlockProposals())
      else Sync[F].unit
    } yield ()

  private[consensus] def acceptMajorityCheckpointBlock(proposals: Map[FacilitatorId, CheckpointBlock]): F[Unit] = {
    val sameBlocks = proposals
      .groupBy(_._2.soeHash)
      .maxBy(_._2.size)
      ._2

    val checkpointBlock = sameBlocks.head._2
    val uniques = proposals.groupBy(_._2.baseHash).size

    for {
      maybeHeight <- checkpointAcceptanceService.calculateHeight(checkpointBlock)
      cache = CheckpointCache(Some(checkpointBlock), height = maybeHeight)
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
      acceptedBlock <- checkpointAcceptanceService
        .accept(cache)
        .map { _ =>
          (Option(checkpointBlock), Seq.empty[String])
        }
        .handleErrorWith {
          case error @ (CheckpointAcceptBlockAlreadyStored(_) | PendingAcceptance(_)) =>
            logger
              .warn(error.getMessage)
              .flatMap(
                _ => Sync[F].pure[(Option[CheckpointBlock], Seq[String])]((None, Seq.empty[String]))
              )
          case tipConflict: TipConflictException =>
            logger
              .error(tipConflict)(
                s"[${dao.id.short}] Failed to accept majority checkpoint block due: ${tipConflict.getMessage}"
              )
              .flatMap(
                _ =>
                  Sync[F]
                    .pure[(Option[CheckpointBlock], Seq[String])]((None, tipConflict.conflictingTxs))
              )
          case unknownError =>
            logger
              .error(unknownError)(
                s"[${dao.id.short}] Failed to accept majority checkpoint block due: ${unknownError.getMessage}"
              )
              .flatMap(
                _ => Sync[F].pure[(Option[CheckpointBlock], Seq[String])]((None, Seq.empty[String]))
              )
        }
      _ <- if (acceptedBlock._1.isEmpty) Sync[F].pure(List.empty[Response[Unit]])
      else
        remoteCall.shift >> broadcastSignedBlockToNonFacilitators(
          FinishedCheckpoint(cache, proposals.keySet.map(_.id))
        )
      transactionsToReturn <- getOwnTransactionsToReturn.map(
        txs =>
          txs
            .diff(acceptedBlock._1.map(_.transactions.map(_.hash)).getOrElse(Seq.empty))
            .filterNot(acceptedBlock._2.contains)
      )
      observationsToReturn <- getOwnObservationsToReturn.map(
        obs =>
          obs
            .diff(acceptedBlock._1.map(_.observations.map(_.hash)).getOrElse(Seq.empty))
            .filterNot(acceptedBlock._2.contains)
      )
      _ <- consensusManager.stopBlockCreationRound(
        StopBlockCreationRound(
          roundData.roundId,
          acceptedBlock._1,
          transactionsToReturn,
          observationsToReturn
        )
      )
      _ <- logger.debug(
        s"[${dao.id.short}] round stopped ${roundData.roundId} block is empty ? ${acceptedBlock._1.isEmpty}"
      )

    } yield ()

  }

  private[consensus] def broadcastSignedBlockToNonFacilitators(
    finishedCheckpoint: FinishedCheckpoint
  ): F[List[Response[Unit]]] = {
    val allFacilitators = roundData.peers.map(p => p.peerMetadata.id -> p).toMap
    for {
      nonFacilitators <- LiftIO[F]
        .liftIO(dao.peerInfo)
        .map(info => info.values.toList.filterNot(pd => allFacilitators.contains(pd.peerMetadata.id)))
      _ <- logger.debug(
        s"[${dao.id.short}] ${roundData.roundId} Broadcasting checkpoint block with baseHash ${finishedCheckpoint.checkpointCacheData.checkpointBlock.get.baseHash}"
      )
      responses <- nonFacilitators.traverse(
        pd =>
          pd.client.postNonBlockingUnitF("finished/checkpoint", finishedCheckpoint, timeout = 30.seconds)(
            calculationContext
          )
      )
    } yield responses
  }

  private[consensus] def mergeBlockProposalsToMajorityBlock(
    proposals: Map[FacilitatorId, CheckpointBlock]
  ): F[Unit] = {
    val sameBlocks = proposals
      .groupBy(_._2.baseHash)
      .maxBy(_._2.size)
      ._2

    val uniques = proposals.groupBy(_._2.baseHash).size

    val checkpointBlock = sameBlocks.values.foldLeft(sameBlocks.head._2)(_ + _)
    val selectedCheckpointBlock =
      SelectedUnionBlock(roundData.roundId, FacilitatorId(dao.id), checkpointBlock)

    for {
      _ <- stage.set(ConsensusStage.WAITING_FOR_SELECTED_BLOCKS)
      _ <- dao.metrics.incrementMetricAsync(
        "resolveMajorityCheckpointBlockProposalCount_" + proposals.size
      )
      _ <- dao.metrics.incrementMetricAsync(
        "resolveMajorityCheckpointBlockUniquesCount_" + uniques
      )
      _ <- remoteCall.shift >> remoteSender.broadcastSelectedUnionBlock(
        BroadcastSelectedUnionBlock(roundData.roundId, roundData.peers, selectedCheckpointBlock)
      )
      _ <- addSelectedBlockProposal(selectedCheckpointBlock)
    } yield ()
  }

  private[consensus] def prepareConsensusData[A, T <: AnyRef](
    dataType: String,
    proposals: Map[FacilitatorId, LightTransactionsProposal],
    readyPeers: Map[Id, PeerApiClient],
    mapHashes: LightTransactionsProposal => Seq[String],
    startingHashes: Traversable[String],
    lookupService: String => F[Option[A]],
    resolvedMapper: T => A
  )(implicit m: Manifest[T]): F[List[A]] =
    for {
      _ <- logger.debug(
        s" ${roundData.roundId} preparing $dataType "
      )
      hashes = proposals.mapValues(mapHashes)
      combined <- (hashes + (roundData.facilitatorId -> (hashes
        .getOrElse(roundData.facilitatorId, Seq.empty) ++ startingHashes)))
        .map(x => x._2.map(t => (t, x._1)))
        .flatten
        .toList
        .distinct
        .traverse(x => lookupService(x._1).map((x, _)))
      toResolve = combined.filter(_._2.isEmpty)
      _ <- logger.debug(
        s"Consensus with id ${roundData.roundId} $dataType to resolve size ${toResolve.size} total size ${combined.size}"
      )

      hashesToResolve = toResolve.map(_._1._1)
      resolved <- LiftIO[F].liftIO(
        dataResolver.resolveBatchDataByDistance[T](
          hashesToResolve,
          dataType,
          readyPeers.values.toList
        )(contextShift)
      )

      _ <- logger.debug(s" ${roundData.roundId} $dataType resolved size ${resolved.size}")
    } yield resolved.map(resolvedMapper) ++ combined.flatMap(_._2)

  private[consensus] def mergeTxProposalsAndBroadcastBlock(): F[Unit] =
    for {
      allPeers <- LiftIO[F].liftIO(dao.peerInfo.map(_.mapValues(p => PeerApiClient(p.peerMetadata.id, p.client))))
      proposals <- transactionProposals.get
      transactions <- prepareConsensusData[Transaction, TransactionCacheData](
        "transactions",
        proposals,
        allPeers, { p =>
          p.txHashes
        },
        roundData.transactions.map(_.hash), { s =>
          transactionService.lookup(s).map(_.map(_.transaction))
        }, { tcd =>
          tcd.transaction
        }
      )

      messages <- prepareConsensusData[ChannelMessage, ChannelMessageMetadata](
        "message",
        proposals,
        allPeers, { p =>
          p.messages
        },
        Seq.empty[String], { s =>
          messageService.lookup(s).map(_.map(_.channelMessage))
        }, { cmm =>
          cmm.channelMessage
        }
      ).map(_.union(roundData.messages)) // TODO: wkoszycki include messages to resolve them

      notifications = proposals
        .flatMap(_._2.notifications)
        .toSet
        .union(roundData.peers.flatMap(_.notification))
        .toSeq
      observations <- proposals
        .flatMap(_._2.exHashes)
        .toList
        .traverse(o => observationService.lookup(o).map((o, _)))
      resolvedObs <- prepareConsensusData[Observation, Observation](
        "observation",
        proposals,
        allPeers, { p =>
          p.exHashes
        },
        roundData.observations.map(_.hash), { s =>
          observationService.lookup(s)
        }, { o =>
          o
        }
      )
      proposal = UnionBlockProposal(
        roundData.roundId,
        FacilitatorId(dao.id),
        CheckpointBlock.createCheckpointBlock(
          transactions,
          roundData.tipsSOE.soe
            .map(soe => TypedEdgeHash(soe.hash, EdgeHashType.CheckpointHash, Some(soe.baseHash))),
          messages,
          notifications,
          observations.flatMap(_._2) ++ resolvedObs
        )(dao.keyPair)
      )
      _ <- remoteCall.shift >> remoteSender.broadcastBlockUnion(
        BroadcastUnionBlockProposal(roundData.roundId, roundData.peers, proposal)
      )
      _ <- addBlockProposal(proposal)
    } yield ()

  private[consensus] def validateAndMergeTransactionProposals(): F[Unit] =
    for {
      proposals <- transactionProposals.getUnsafe
      validationResult <- validateReceivedProposals(
        proposals,
        "transactionProposals",
        countSelfAsPeer = roundStartedByMe
      )
      _ <- validationResult match {
        case Left(exception) => consensusManager.handleRoundError(exception)
        case Right(_)        => mergeTxProposalsAndBroadcastBlock()
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

  private[consensus] def getOwnTransactionsToReturn: F[Seq[String]] =
    transactionProposals.get.map(_.get(FacilitatorId(dao.id)).map(_.txHashes).getOrElse(Seq.empty))

  private[consensus] def getOwnObservationsToReturn: F[Seq[String]] =
    transactionProposals.get.map(_.get(FacilitatorId(dao.id)).map(_.exHashes).getOrElse(Seq.empty))

  private def roundStartedByMe: Boolean = roundData.facilitatorId.id == dao.id

  private[consensus] def receivedAllSelectedUnionBlocks(size: Int): Boolean =
    size == roundData.peers.size + 1

  private[consensus] def receivedAllCheckpointBlockProposals(size: Int): Boolean =
    size == roundData.peers.size + 1

  private[consensus] def receivedAllTransactionProposals(size: Int): Boolean = {
    val extraMessage = if (roundStartedByMe) 1 else 0
    size == roundData.peers.size + extraMessage
  }

  def validateReceivedProposals(
    proposals: Map[FacilitatorId, AnyRef],
    stage: String,
    minimumPercentage: Int = 51,
    countSelfAsPeer: Boolean = true
  ): F[Either[ConsensusException, Unit]] = {
    val peerSize = roundData.peers.size + (if (countSelfAsPeer) 1 else 0)
    val proposalPercentage: Float = proposals.size * 100 / peerSize
    (proposalPercentage, proposals.size) match {
      case (0, _) =>
        getOwnTransactionsToReturn.flatMap(
          txs => getOwnObservationsToReturn.map(exs => Left(EmptyProposals(roundData.roundId, stage, txs, exs)))
        )
      case (_, size) if size == 1 =>
        getOwnTransactionsToReturn.flatMap(
          txs => getOwnObservationsToReturn.map(exs => Left(EmptyProposals(roundData.roundId, stage, txs, exs)))
        )
      case (p, _) if p < minimumPercentage =>
        getOwnTransactionsToReturn.flatMap(
          txs =>
            getOwnObservationsToReturn.map(
              exs =>
                Left(
                  NotEnoughProposals(roundData.roundId, proposals.size, peerSize, stage, txs, exs)
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
    def transactionsToReturn: Seq[String]
    def observationsToReturn: Seq[String]
  }

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
  case class RoundId(id: String) extends AnyVal

  case class UnionProposals(state: StageState)
  case class ArbitraryDataProposals(distance: Int)

  case class ResolveMajorityCheckpointBlock(roundId: RoundId, stageState: StageState)

  case class AcceptMajorityCheckpointBlock(roundId: RoundId)

  case class StartTransactionProposal(roundId: RoundId)

  case class LightTransactionsProposal(
    roundId: RoundId,
    facilitatorId: FacilitatorId,
    txHashes: Seq[String],
    messages: Seq[String] = Seq(),
    notifications: Seq[PeerNotification] = Seq(),
    exHashes: Seq[String] = Seq()
  ) extends ConsensusProposal

  case class UnionBlockProposal(
    roundId: RoundId,
    facilitatorId: FacilitatorId,
    checkpointBlock: CheckpointBlock
  ) extends ConsensusProposal

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
    transactionsToReturn: Seq[String],
    observationsToReturn: Seq[String]
  )

  case class EmptyProposals(
    roundId: RoundId,
    stage: String,
    transactionsToReturn: Seq[String],
    observationsToReturn: Seq[String]
  ) extends ConsensusException(s"Proposals for stage: $stage and round: $roundId are empty.")

  case class PreviousStage(
    roundId: RoundId,
    stage: ConsensusStage,
    transactionsToReturn: Seq[String],
    observationsToReturn: Seq[String]
  ) extends ConsensusException(s"Received message from previous round stage. Current round stage is $stage")

  case class NotEnoughProposals(
    roundId: RoundId,
    proposals: Int,
    facilitators: Int,
    stage: String,
    transactionsToReturn: Seq[String],
    observationsToReturn: Seq[String]
  ) extends ConsensusException(
        s"Proposals number: $proposals for stage: $stage and round: $roundId are below given percentage. Number of facilitators: $facilitators"
      )

  case class SelectedUnionBlock(
    roundId: RoundId,
    facilitatorId: FacilitatorId,
    checkpointBlock: CheckpointBlock
  ) extends ConsensusProposal

}
