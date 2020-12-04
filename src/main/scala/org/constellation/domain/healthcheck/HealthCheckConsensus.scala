package org.constellation.domain.healthcheck

import cats.Order
import cats.data.NonEmptySet
import cats.effect.{Blocker, Clock, Concurrent, ContextShift, Fiber}
import cats.effect.concurrent.Ref
import cats.syntax.all._
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.circe.{Codec, Decoder, Encoder}
import io.circe.generic.semiauto.deriveCodec
import io.circe.syntax.EncoderOps
import org.constellation.ConstellationExecutionContext.createSemaphore
import org.constellation.domain.healthcheck.HealthCheckConsensusManager._
import org.constellation.domain.p2p.PeerHealthCheck.{
  PeerAvailable,
  PeerHealthCheckStatus,
  PeerUnresponsive,
  UnknownPeer
}
import org.constellation.infrastructure.p2p.{ClientInterpreter, PeerResponse}
import org.constellation.p2p.PeerData
import org.constellation.schema.{Id, NodeState}
import org.constellation.schema.consensus.RoundId

import scala.concurrent.duration.{DurationInt, DurationLong, FiniteDuration, SECONDS}
import scala.util.{Random, Try}

class HealthCheckConsensus[F[_]](
  checkedId: Id,
  ownId: Id,
  roundId: RoundId,
  initialRoundPeers: Map[Id, PeerData],
  healthCheckStatus: PeerHealthCheckStatus,
  startedAtSecond: FiniteDuration,
  val isKeepUpRound: Boolean,
  apiClient: ClientInterpreter[F],
  unboundedHealthBlocker: Blocker,
  healthHttpPort: String
)(implicit F: Concurrent[F], CS: ContextShift[F], C: Clock[F]) {
  import HealthCheckConsensus._

  val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]
  val fetchProposalAfter: FiniteDuration = 120.seconds
  val removeUnresponsivePeersWithParallelRoundAfter: FiniteDuration = 60.seconds

  private[healthcheck] val roundIds: Ref[F, NonEmptySet[RoundId]] = Ref.unsafe(NonEmptySet.one(roundId))
  private[healthcheck] val roundPeers: Ref[F, Map[Id, RoundData]] =
    Ref.unsafe(
      initialRoundPeers.mapValues(pd => RoundData(pd.asRight[ProxyPeers]))
    )
  private[healthcheck] val leftDuringConsensus: Ref[F, Set[Id]] = Ref.unsafe(Set.empty)
  private[healthcheck] val removedUnresponsivePeersWithParallelRound: Ref[F, Set[Id]] = Ref.unsafe(Set.empty)
  private[healthcheck] val ownConsensusHealthStatus: Ref[F, ConsensusHealthStatus] = Ref.unsafe(
    ConsensusHealthStatus(
      checkedId,
      ownId,
      roundId,
      healthCheckStatus,
      initialRoundPeers.mapValues(_.peerMetadata.nodeState)
    )
  )
  private[healthcheck] val parallelRounds: Ref[F, Map[Id, Set[RoundId]]] = Ref.unsafe(Map.empty)

  private val sendingProposalLock = createSemaphore()
  private val fetchingProposalLock = createSemaphore()

  def getTimeInSeconds(): F[FiniteDuration] = C.monotonic(SECONDS).map(_.seconds)

  def start(): F[Fiber[F, Unit]] = F.start(sendProposal())

  def getOwnPerceivedHealthStatus(): F[ConsensusHealthStatus] = ownConsensusHealthStatus.get

  def getRemovedUnresponsivePeersWithParallelRound(): F[Set[Id]] = removedUnresponsivePeersWithParallelRound.get

  def addRoundIds(ids: Set[RoundId]): F[Unit] =
    roundIds.modify(rounds => (NonEmptySet(rounds.head, rounds.tail ++ ids), ()))

  def getRoundIds(): F[NonEmptySet[RoundId]] = roundIds.get

  def getParallelRounds(): F[Map[Id, Set[RoundId]]] = parallelRounds.get

  def addParallelRounds(id: Id, roundIds: Set[RoundId]): F[Unit] = parallelRounds.modify { rounds =>
    val updatedIdRounds = rounds.get(id).map(r => r ++ roundIds).getOrElse(roundIds)

    (rounds + (id -> updatedIdRounds), ())
  }

  def getRoundPeers(): F[Map[Id, RoundData]] = roundPeers.get

  def addRoundPeer(id: Id, peerData: PeerData): F[Unit] =
    roundPeers.modify(peers => (peers + (id -> RoundData(peerData.asRight[ProxyPeers])), ()))

  def removeRoundPeer(id: Id): F[Unit] = roundPeers.modify(peers => (peers - id, ()))

  def manageAbsentPeers(absentPeers: Set[Id]): F[Unit] =
    for {
      peers <- getRoundPeers()
      // should we only remove peers that didn't finish or all of them
      unsuccessfulPeers = peers.filter {
        case (_, roundData) => !roundData.receivedProposal || roundData.consensusData.isEmpty
      }.keySet
      toRemove = unsuccessfulPeers.intersect(absentPeers)
      _ <- leftDuringConsensus.modify(alreadyLeft => (alreadyLeft ++ toRemove, ()))
      _ <- toRemove.toList.traverse(removeRoundPeer)
    } yield ()

  def manageProxiedPeers(peersThatNoLongerNeedProxying: Map[Id, PeerData]): F[Unit] =
    for {
      _ <- roundPeers.modify { roundPeers =>
        val updatedPeers = roundPeers.filterKeys(peersThatNoLongerNeedProxying.keySet.contains).map {
          case (id, roundData) => id -> roundData.copy(peerData = peersThatNoLongerNeedProxying(id).asRight[ProxyPeers])
        }

        (roundPeers ++ updatedPeers, ())
      }
    } yield ()

  def removeIdAsAProxy(proxyingId: Id, proxyToId: Id): F[Unit] =
    roundPeers.modify { peers =>
      val updatedPeer = peers.get(proxyToId).flatMap { roundData =>
        val newPeerData = roundData.peerData match {
          case Left(proxyPeers) =>
            val updatedProxyPeersData = proxyPeers.peersData.filterNot(_.peerMetadata.id == proxyingId)
            if (updatedProxyPeersData.isEmpty)
              None
            else
              ProxyPeers(NonEmptySet(updatedProxyPeersData.head, updatedProxyPeersData.tail)).asLeft[PeerData].some

          case Right(_) => roundData.peerData.some
        }

        newPeerData.map(pd => roundData.copy(peerData = pd))
      }

      updatedPeer match {
        case Some(roundData) => (peers + (proxyToId -> roundData), ())
        case None            => (peers - proxyToId, ())
      }
    }

  def removeUnresponsivePeersWithParallelRound(unresponsivePeers: Set[Id]): F[Unit] =
    for {
      parallelRounds <- getParallelRounds()
      toRemove = parallelRounds.keySet.intersect(unresponsivePeers)
      _ <- removedUnresponsivePeersWithParallelRound.modify(current => (current ++ toRemove, ()))
      _ <- toRemove.toList.traverse(removeRoundPeer)
    } yield ()

  def runManagementTasks(): F[Unit] =
    for {
      currentTime <- getTimeInSeconds()
      missingPeersData <- getRoundPeers().map(_.filter {
        case (id, data) => data.consensusData.isEmpty || !data.receivedProposal
      })
      _ <- logger.debug(s"Missing peers data for roundId=$roundId: ${missingPeersData.map {
        case (id, data) => id.medium -> (data.receivedProposal, data.peerData, data.consensusData)
      }}")
      _ <- if (currentTime - startedAtSecond > removeUnresponsivePeersWithParallelRoundAfter)
        removeUnresponsivePeersWithParallelRound(missingPeersData.keySet)
      else
        F.unit
      _ <- if (!isKeepUpRound) sendProposal() else F.unit // F.start(...)???
      _ <- if (currentTime - startedAtSecond > fetchProposalAfter || isKeepUpRound) fetchProposals() else F.unit // F.start(...)???
    } yield ()

  def generateHistoricalRoundData(healthcheckDecision: Option[HealthcheckConsensusDecision]): F[HistoricalRound] =
    for {
      finishedAtSecond <- getTimeInSeconds()
      roundIds <- getRoundIds()
      roundPeers <- getRoundPeers()
      parallelRounds <- getParallelRounds()
      leftDuringConsensus <- leftDuringConsensus.get
      removedUnresponsivePeersWithParallelRound <- removedUnresponsivePeersWithParallelRound.get
      ownConsensusHealthStatus <- getOwnPerceivedHealthStatus()
      historicalRound = HistoricalRound(
        checkedId = checkedId,
        startedAtSecond = startedAtSecond,
        finishedAtSecond = finishedAtSecond,
        roundIds = roundIds.toSortedSet,
        decision = healthcheckDecision,
        roundPeers = roundPeers,
        parallelRounds = parallelRounds,
        leftDuringConsensus = leftDuringConsensus,
        removedUnresponsivePeersWithParallelRound = removedUnresponsivePeersWithParallelRound,
        ownConsensusHealthStatus = ownConsensusHealthStatus,
        isKeepUpRound = isKeepUpRound
      )
    } yield historicalRound

  def processProposal(healthProposal: ConsensusHealthStatus): F[Unit] =
    for {
      _ <- roundIds.modify { roundIds =>
        val updated = if (roundIds.contains(healthProposal.roundId)) roundIds else roundIds.add(healthProposal.roundId)

        (updated, ())
      }
      initiallyUnknownPeers = healthProposal.clusterState.keySet
        .diff(initialRoundPeers.keySet)
        .filterNot(_ == ownId) // I think we don't need to filter out peers that left because they will not be added here by mistake
      _ <- roundPeers.modify { roundPeers =>
        // observations are immutable
        val peerRoundData = roundPeers
          .get(healthProposal.checkingPeerId)
          .map { roundData =>
            val newConsensusData =
              roundData.consensusData.getOrElse(
                PeerConsensusData(healthProposal.status, healthProposal.clusterState, healthProposal.roundId)
              )

            roundData.copy(consensusData = newConsensusData.some)
          }

        val updated =
          peerRoundData.map { prd =>
            val updatedPeers = prd.peerData match {
              case Left(_) =>
                roundPeers //for now we don't allow proxy through more than one node - so we don't proxy p1 -> p2 -> p3 -> p4, because p1 sees p2 which sees p3 which sees p4
              case Right(peerData) =>
                val addedPeers =
                  (initiallyUnknownPeers -- roundPeers.keySet)
                    .map(
                      _ -> RoundData(ProxyPeers(NonEmptySet.one(peerData)).asLeft[PeerData])
                    )
                val updatedProxyPeers =
                  roundPeers
                    .filterKeys(initiallyUnknownPeers.contains)
                    .mapValues(
                      roundData =>
                        roundData.copy(
                          peerData = roundData.peerData
                            .leftMap(proxyPeers => proxyPeers.copy(proxyPeers.peersData.add(peerData)))
                        )
                    )

                roundPeers ++ updatedProxyPeers ++ addedPeers
            }

            updatedPeers + (healthProposal.checkingPeerId -> prd)
          }.getOrElse(roundPeers)
        (updated, updated)
      }
    } yield ()

  private def fetchProposals(): F[Unit] =
    for {
      isAcquired <- fetchingProposalLock.tryAcquire
      _ <- if (isAcquired)
        fetchHealthProposals().handleErrorWith { e =>
          logger.error(e)("Error fetching healthcheck proposals.")
        } >> fetchingProposalLock.release
      else
        F.unit
    } yield ()

  private def fetchHealthProposals(): F[Unit] =
    for {
      peers <- roundPeers.get
        .map(_.filter { case (_, rd) => rd.consensusData.isEmpty })
      roundIds <- roundIds.get
      responses <- peers.toList.traverse {
        case (id, roundData) => {
          {
            roundData.peerData match {
              case Left(proxyPeers) =>
                for {
                  proxyPeer <- randomlyPickProxyPeer(proxyPeers.peersData)
                  response <- PeerResponse.run(
                    apiClient.healthcheck.fetchPeerHealthStatus(FetchPeerHealthStatus(roundIds.toSortedSet, id.some)),
                    unboundedHealthBlocker
                  )(proxyPeer.peerMetadata.toPeerClientMetadata.copy(port = healthHttpPort))
                } yield response
              case Right(peerData) =>
                PeerResponse.run(
                  apiClient.healthcheck.fetchPeerHealthStatus(FetchPeerHealthStatus(roundIds.toSortedSet)),
                  unboundedHealthBlocker
                )(peerData.peerMetadata.toPeerClientMetadata.copy(port = healthHttpPort))
            }
          }.handleError(
            _ => FetchingFailed(id).asInstanceOf[FetchProposalError].asLeft[SendPerceivedHealthStatus]
          )
        }.map(_.bimap(id -> _, id -> _))
      }
      (failed, succeeded) = responses.separate
      _ <- succeeded.traverse {
        case (id, proposal) =>
          logger.debug(
            s"Successfully fetched proposal from peer with id=${id.medium} proposal=${proposal.consensusHealthStatus}"
          ) >>
            processProposal(proposal.consensusHealthStatus)
              .handleErrorWith(
                e =>
                  logger.debug(e)(
                    s"Error during processing of healthcheck proposal from peer with id=${id.medium} proposal=${proposal.consensusHealthStatus}"
                  )
              )
      }
      _ <- failed.traverse {
        case (id, error) =>
          logger.debug(error)(s"Error during fetching healthcheck proposal from peer with id=${id.medium}.") >> {
            error match {
              case UnknownProxyIdFetching(proxyToId, proxyingId) =>
                removeIdAsAProxy(proxyingId = proxyingId, proxyToId = proxyToId)
              case _ => F.unit
            }
          }
      }
    } yield ()

  private def randomlyPickProxyPeer(proxyPeers: NonEmptySet[PeerData]): F[PeerData] = F.delay(
    Random.shuffle(proxyPeers.toList).head
  )

  private def sendProposal(): F[Unit] =
    for {
      isAcquired <- sendingProposalLock.tryAcquire
      _ <- if (isAcquired)
        sendHealthStatus().handleErrorWith { e =>
          logger.error(e)("Error sending healthcheck proposals.")
        } >> sendingProposalLock.release
      else
        F.unit
    } yield ()

  private def sendHealthStatus(): F[Unit] =
    for {
      peers <- roundPeers.get
        .map(_.filterNot { case (_, rd) => rd.receivedProposal })
      healthStatus <- ownConsensusHealthStatus.get
      responses <- peers.toList.traverse {
        case (id, roundData) => {
          {
            roundData.peerData match {
              case Left(proxyPeers) =>
                for {
                  proxyPeer <- randomlyPickProxyPeer(proxyPeers.peersData)
                  response <- PeerResponse.run(
                    apiClient.healthcheck.sendPeerHealthStatus(SendPerceivedHealthStatus(healthStatus, id.some)),
                    unboundedHealthBlocker
                  )(proxyPeer.peerMetadata.toPeerClientMetadata.copy(port = healthHttpPort))
                } yield response
              case Right(peerData) =>
                PeerResponse.run(
                  apiClient.healthcheck.sendPeerHealthStatus(SendPerceivedHealthStatus(healthStatus)),
                  unboundedHealthBlocker
                )(peerData.peerMetadata.toPeerClientMetadata.copy(port = healthHttpPort))
            }
          }.handleErrorWith(_ => SendingFailed(id).asInstanceOf[SendProposalError].asLeft[Unit].pure[F])
        }.map(_.bimap(id -> _, _ => id))
      }
      (failed, succeeded) = responses.separate
      _ <- roundPeers.modify { peers =>
        val updatedPeers = peers
          .filterKeys(succeeded.contains)
          .mapValues(rd => rd.copy(receivedProposal = true))

        (peers ++ updatedPeers, ())
      }
      _ <- failed.traverse {
        case (id, error) =>
          logger.debug(error)(s"Error during sending healthcheck proposal to peer with id=${id.medium}.") >> {
            error match {
              case UnknownProxyIdSending(proxyToId, proxyingId) =>
                removeIdAsAProxy(proxyingId = proxyingId, proxyToId = proxyToId)
              case _ => F.unit
            }
          }
      }
    } yield ()

  def isStatusOnline(status: PeerHealthCheckStatus): Boolean =
    status match {
      case PeerAvailable(_) => true
      case _                => false
    }

  def calculateConsensusOutcome(): F[Option[HealthcheckConsensusDecision]] =
    for {
      peersData <- roundPeers.get
      roundIds <- roundIds.get
      parallelRounds <- parallelRounds.get
      ownHealthStatus <- getOwnPerceivedHealthStatus().map(_.status)
      peersThatLeft <- leftDuringConsensus.get
      removedPeersWithParallelRound <- removedUnresponsivePeersWithParallelRound.get
      peersRemaining = peersData.keySet ++ (if (isKeepUpRound) Set.empty else Set(ownId))
      allPeers = peersRemaining ++ parallelRounds.keySet ++ peersThatLeft ++ removedPeersWithParallelRound
      allReceivedStatuses = peersData.values.toList.flatMap(_.consensusData.map(_.status)) ++
        (if (isKeepUpRound) Nil else List(ownHealthStatus))
      available = allReceivedStatuses.collect { case a @ PeerAvailable(_)       => a }
      unresponsive = allReceivedStatuses.collect { case a @ PeerUnresponsive(_) => a }
      unknown = allReceivedStatuses.collect { case a @ UnknownPeer(_)           => a }
      availableSize = BigDecimal(available.size)
      unresponsiveSize = BigDecimal(unresponsive.size)
      unknownSize = BigDecimal(unknown.size)
      remainingPeersSize = BigDecimal(peersRemaining.size)
      onlinePercentage = Try(availableSize / remainingPeersSize).toOption // should we divide by all known peers, or only the ones that remain in the consensus round
        .getOrElse(if (isStatusOnline(ownHealthStatus)) BigDecimal(1) else BigDecimal(0))
      offlinePercentage = Try((unresponsiveSize + unknownSize) / remainingPeersSize).toOption
        .getOrElse(if (isStatusOnline(ownHealthStatus)) BigDecimal(0) else BigDecimal(1))
      _ <- logger.debug(
        s"onlinePercentage=$onlinePercentage offlinePercentange=$offlinePercentage availableSize=$availableSize unresponsiveSize=$unresponsiveSize unknownSize=$unknownSize peersRemaining=${peersRemaining
          .map(_.short)} allPeers=${allPeers.map(_.short)}"
      )
      result = {
        if (allPeers.isEmpty)
          None
        else if (onlinePercentage >= BigDecimal(0.5d))
          PeerOnline(checkedId, allPeers, peersRemaining, onlinePercentage, parallelRounds, roundIds, isKeepUpRound)
            .asInstanceOf[HealthcheckConsensusDecision]
            .some
        else if (offlinePercentage > BigDecimal(0.5d))
          PeerOffline(checkedId, allPeers, peersRemaining, offlinePercentage, parallelRounds, roundIds, isKeepUpRound)
            .asInstanceOf[HealthcheckConsensusDecision]
            .some
        else None
      }
    } yield result

  def generateMissedConsensusNotification(): F[NotifyAboutMissedConsensus] =
    for {
      roundIds <- getRoundIds()
      ownHealthStatus <- getOwnPerceivedHealthStatus()
      notification = NotifyAboutMissedConsensus(checkedId, roundIds.toSortedSet, ownHealthStatus)
    } yield notification

  def isProposalSent(rd: List[RoundData]): Boolean = rd.forall(_.receivedProposal)
  def areProposalsReceived(rd: List[RoundData]): Boolean = rd.forall(_.consensusData.nonEmpty)

  def isReadyToCalculateOutcome(): F[Boolean] =
    for {
      peers <- roundPeers.get
      peersData = peers.values.toList
      result = (isProposalSent(peersData) || isKeepUpRound) && areProposalsReceived(peersData)
    } yield result
}

object HealthCheckConsensus {

  def apply[F[_]: Concurrent: ContextShift: Clock](
    checkedId: Id,
    ownId: Id,
    roundId: RoundId,
    initialRoundPeers: Map[Id, PeerData],
    healthCheckStatus: PeerHealthCheckStatus,
    startedAtSecond: FiniteDuration,
    isKeepUpRound: Boolean,
    apiClient: ClientInterpreter[F],
    unboundedHealthBlocker: Blocker,
    healthHttpPort: String
  ): HealthCheckConsensus[F] =
    new HealthCheckConsensus(
      checkedId,
      ownId,
      roundId,
      initialRoundPeers,
      healthCheckStatus,
      startedAtSecond,
      isKeepUpRound,
      apiClient,
      unboundedHealthBlocker,
      healthHttpPort = healthHttpPort
    )

  implicit val roundIdOrdering: Ordering[RoundId] = Ordering.by[RoundId, String](_.id)
  implicit val roundIdOrder: Order[RoundId] = Order.fromOrdering
  implicit val peerDataOrdering: Ordering[PeerData] = Ordering.by[PeerData, String](_.peerMetadata.id.hex)
  implicit val peerDataOrder: Order[PeerData] = Order.fromOrdering

  case class ConsensusHealthStatus(
    checkedId: Id,
    checkingPeerId: Id,
    roundId: RoundId,
    status: PeerHealthCheckStatus,
    clusterState: Map[Id, NodeState]
  )

  object ConsensusHealthStatus {
    implicit val consensusHealthStatusCodec: Codec[ConsensusHealthStatus] = deriveCodec
  }

  sealed trait HealthConsensusCommand

  object HealthConsensusCommand {
    implicit val encodeHealthConsensusCommand: Encoder[HealthConsensusCommand] = Encoder.instance {
      case a @ SendPerceivedHealthStatus(_, _)     => a.asJson
      case a @ NotifyAboutMissedConsensus(_, _, _) => a.asJson
    }

    implicit val decodeHealthConsensusCommand: Decoder[HealthConsensusCommand] =
      List[Decoder[HealthConsensusCommand]](
        Decoder[SendPerceivedHealthStatus].widen,
        Decoder[NotifyAboutMissedConsensus].widen
      ).reduceLeft(_.or(_))
  }

  case class SendPerceivedHealthStatus(
    consensusHealthStatus: ConsensusHealthStatus,
    asProxyForId: Option[Id] = None
  ) extends HealthConsensusCommand

  object SendPerceivedHealthStatus {
    implicit val sendPerceivedHealthStatusCodec: Codec[SendPerceivedHealthStatus] = deriveCodec
  }

  case class NotifyAboutMissedConsensus(
    checkedId: Id,
    roundIds: Set[RoundId],
    consensusHealthStatus: ConsensusHealthStatus
  ) extends HealthConsensusCommand

  object NotifyAboutMissedConsensus {
    implicit val notifyAboutMissedConsensusCodec: Codec[NotifyAboutMissedConsensus] = deriveCodec
  }

  case class FetchPeerHealthStatus(roundIds: Set[RoundId], asProxyForId: Option[Id] = None)

  object FetchPeerHealthStatus {
    implicit val fetchPeerHealthStatusCodec: Codec[FetchPeerHealthStatus] = deriveCodec
  }

  case class ProxyPeers(peersData: NonEmptySet[PeerData])

  case class PeerConsensusData(status: PeerHealthCheckStatus, clusterState: Map[Id, NodeState], roundId: RoundId)

  case class RoundData(
    peerData: Either[ProxyPeers, PeerData],
    consensusData: Option[PeerConsensusData] = None,
    receivedProposal: Boolean = false
  )

  case class RoundOutcome(roundIds: Set[RoundId], outcome: HealthcheckConsensusDecision)

  sealed trait HealthcheckConsensusDecision {
    val id: Id
    val allPeers: Set[Id]
    val remainingPeers: Set[Id]
    val percentage: BigDecimal
    val parallelRounds: Map[Id, Set[RoundId]]
    val roundIds: NonEmptySet[RoundId]
    val isKeepUpRound: Boolean
  }

  case class PeerOffline(
    id: Id,
    allPeers: Set[Id],
    remainingPeers: Set[Id],
    percentage: BigDecimal,
    parallelRounds: Map[Id, Set[RoundId]],
    roundIds: NonEmptySet[RoundId],
    isKeepUpRound: Boolean
  ) extends HealthcheckConsensusDecision

  case class PeerOnline(
    id: Id,
    allPeers: Set[Id],
    remainingPeers: Set[Id],
    percentage: BigDecimal,
    parallelRounds: Map[Id, Set[RoundId]],
    roundIds: NonEmptySet[RoundId],
    isKeepUpRound: Boolean
  ) extends HealthcheckConsensusDecision

  // Maybe we could introduce the one below to handle abnormal cases if any exist, for the case where node is processing a round that the node wasn't a part of - e.g. a parallel round -
  // the node shouldn't make it's own observation and it should rely only on the observations from other peers, there is a slight possibility remainingPeers will be empty
  // because we are not adding own proposal
//  case class NotConclusive(
//    id: Id,
//    allPeers: Set[Id],
//    remainingPeers: Set[Id],
//    percentage: BigDecimal,
//    parallelRounds: Map[Id, Set[RoundId]],
//    roundIds: NonEmptySet[RoundId],
//    isKeepUpRound: Boolean
//  ) extends HealthcheckConsensusDecision
}
