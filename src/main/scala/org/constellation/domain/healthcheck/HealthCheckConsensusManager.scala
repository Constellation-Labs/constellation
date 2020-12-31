package org.constellation.domain.healthcheck

import cats.Parallel
import cats.data.NonEmptyList
import cats.effect.{Blocker, Clock, Concurrent, ContextShift, Fiber, Timer}
import cats.effect.concurrent.Ref
import cats.syntax.all._
import io.chrisdavenport.fuuid.FUUID
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.circe.{Codec, Decoder, Encoder}
import io.circe.generic.semiauto.deriveCodec
import io.circe.syntax.EncoderOps
import org.constellation.ConfigUtil
import org.constellation.ConstellationExecutionContext.createSemaphore
import org.constellation.domain.healthcheck.HealthCheckConsensus._
import org.constellation.domain.healthcheck.HealthCheckLoggingHelper._
import org.constellation.domain.healthcheck.ReconciliationRound._
import org.constellation.domain.p2p.PeerHealthCheck
import org.constellation.domain.p2p.PeerHealthCheck.{PeerHealthCheckStatus, PeerUnresponsive, UnknownPeer}
import org.constellation.infrastructure.p2p.PeerResponse.PeerClientMetadata
import org.constellation.infrastructure.p2p.{ClientInterpreter, PeerResponse}
import org.constellation.p2p.{Cluster, PeerData}
import org.constellation.schema.consensus.RoundId
import org.constellation.schema.Id
import org.constellation.schema.NodeState._
import org.constellation.util.Metrics

import scala.concurrent.duration.{DurationInt, DurationLong, FiniteDuration, SECONDS}
import scala.util.Random

class HealthCheckConsensusManager[F[_]](
  ownId: Id,
  cluster: Cluster[F],
  peerHealthCheck: PeerHealthCheck[F],
  metrics: Metrics,
  apiClient: ClientInterpreter[F],
  unboundedHealthBlocker: Blocker,
  healthHttpPort: Int,
  peerHttpPort: Int
)(implicit F: Concurrent[F], CS: ContextShift[F], P: Parallel[F], C: Clock[F], T: Timer[F]) {
  import HealthCheckConsensusManager._

  val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]
  val healthCheckTimeout: FiniteDuration = 5.seconds

  // maybe this could be dynamic and dependent on cluster size, the bigger the cluster size the longer timespan
  val maxTimespanWithoutOwnReconciliationRound: FiniteDuration =
    ConfigUtil.getDurationFromConfig(
      "constellation.health-check.max-timespan-without-own-reconciliation-round",
      6.hours
    )

  private val consensusRounds: Ref[F, ConsensusRounds[F]] =
    Ref.unsafe(ConsensusRounds(List.empty, Map.empty, Map.empty))
  private val reconciliationRound: Ref[F, Option[ReconciliationRound[F]]] = Ref.unsafe(None)
  private val peersToRunHealthcheckFor: Ref[F, Set[Id]] = Ref.unsafe(Set.empty) // use THIS to start healthchecks
  private val peersToRunConsensusFor: Ref[F, Set[Id]] = Ref.unsafe(Set.empty) // use THIS to start consensuses
  private val peersThatNeedReconciliation: Ref[F, Set[Id]] = Ref.unsafe(Set.empty)
  private val timeOfLastOwnReconciliationRound: Ref[F, Option[FiniteDuration]] = Ref.unsafe(None)
  private val runReconciliationRoundAfter: Ref[F, Option[FiniteDuration]] = Ref.unsafe(None)

  private val checkingPeersHealthLock = createSemaphore()

  def getTimeInSeconds(): F[FiniteDuration] = C.monotonic(SECONDS).map(_.seconds)

  private def getInProgressRounds(): F[Map[Id, HealthCheckConsensus[F]]] = consensusRounds.get.map(_.inProgress)

  private def getKeepUpRounds(): F[Map[Id, HealthCheckConsensus[F]]] = consensusRounds.get.map(_.keepUpRounds)

  private[healthcheck] def createRoundId(): F[RoundId] =
    FUUID.randomFUUID
      .map(fuuid => RoundId(fuuid.toString()))

  private[healthcheck] def createHealthcheckRoundId(): F[HealthcheckRoundId] =
    for {
      roundId <- createRoundId()
      healthcheckRoundId = HealthcheckRoundId(roundId, ownId)
    } yield healthcheckRoundId

  private def pickPeersForConsensus(peers: Map[Id, PeerData], peersToExclude: Set[Id]): Map[Id, PeerData] =
    peers.filter {
      case (peerId, peerData) =>
        !peersToExclude.contains(peerId) && canActAsJoiningSource(peerData.peerMetadata.nodeState)
    }

  def startNewRoundForId(
    allPeers: Map[Id, PeerData],
    checkedId: Id,
    roundId: HealthcheckRoundId,
    keepUpRoundIds: Option[Set[HealthcheckRoundId]] = None,
    delayedHealthCheckStatus: Fiber[F, PeerHealthCheckStatus],
    roundType: HealthcheckActiveRoundType
  ): F[Either[HistoricalRoundData, HealthCheckConsensus[F]]] =
    for {
      startedAt <- getTimeInSeconds()
      peersIp = allPeers.find { case (id, _) => id == checkedId }.map {
        case (_, peerData) => peerData.peerMetadata.host
      }
      checkedPeer = CheckedPeer(checkedId, peersIp)
      allRoundIds = keepUpRoundIds.getOrElse(Set.empty) ++ Set(roundId)
      response <- consensusRounds.modify {
        case ConsensusRounds(historical, inProgress, keepUpRounds) =>
          lazy val roundPeers = pickPeersForConsensus(allPeers, Set(checkedId))
          lazy val consensusRound =
            HealthCheckConsensus[F](
              checkedPeer,
              ownId,
              roundId,
              keepUpRoundIds,
              roundPeers,
              delayedHealthCheckStatus,
              startedAt,
              roundType,
              metrics,
              apiClient,
              unboundedHealthBlocker,
              healthHttpPort = healthHttpPort
            )
          // when starting own consensus we also check keepUpRounds to not start new round when keepUpRound is running
          val historicalRoundForRoundIds = historical.find(_.roundIds.intersect(allRoundIds).nonEmpty)
          val roundForId =
            ((if (List(OwnRound, KeepUpRound).contains(roundType)) keepUpRounds else Map.empty) ++ inProgress)
              .get(checkedId)

          val (updatedInProgress, updatedKeepUp, consensusRoundResponse): (
            Map[Id, HealthCheckConsensus[F]],
            Map[Id, HealthCheckConsensus[F]],
            ConsensusRoundResponse
          ) =
            (roundForId, historicalRoundForRoundIds) match {
              case (_, Some(historical)) =>
                (inProgress, keepUpRounds, HistoricalRoundForGivenRoundsIdsAlreadyHappened(checkedId, historical))
              case (Some(existingRound), _) =>
                (inProgress, keepUpRounds, RoundForGivenIdAlreadyTakingPlace(checkedId, existingRound))
              case (None, _) =>
                roundType match {
                  case OwnRound =>
                    (inProgress + (checkedId -> consensusRound), keepUpRounds, RoundStarted(checkedId, consensusRound))
                  case PeerRound =>
                    (
                      inProgress + (checkedId -> consensusRound),
                      keepUpRounds - checkedId,
                      RoundStarted(checkedId, consensusRound)
                    )
                  case KeepUpRound =>
                    (inProgress, keepUpRounds + (checkedId -> consensusRound), RoundStarted(checkedId, consensusRound))
                }
            }

          (
            ConsensusRounds(historical = historical, inProgress = updatedInProgress, keepUpRounds = updatedKeepUp),
            consensusRoundResponse
          )
      }
      result <- {
        response match {
          case RoundStarted(id, consensusRound) =>
            for {
              _ <- logger.debug(
                s"Started $roundType for id=${logId(id)} with ${logRoundIdWithOwner(roundId)} and peers=${logIds(
                  allPeers.keySet
                )} at $startedAt! allRoundIds=${logRoundIds(allRoundIds)}"
              )
              inProgressRounds <- getInProgressRounds()
              keepUpRounds <- getKeepUpRounds()
              parallelRounds = (inProgressRounds ++ keepUpRounds).filterKeys(_ != id)
              _ <- if (parallelRounds.isEmpty)
                logger.debug(
                  s"No parallel rounds to add to $roundType with roundId=${logRoundId(roundId)} or vice versa."
                )
              else
                for {
                  _ <- parallelRounds.toList.traverse {
                    case (id, consensus) =>
                      consensus.getRoundIds() >>= { roundIds =>
                        consensusRound.addParallelRounds(id, roundIds.toSortedSet)
                      }
                  }
                  _ <- logger.debug(s"Added parallel rounds to $roundType with roundIds=${logRoundIds(allRoundIds)}")
                  _ <- parallelRounds.toList.traverse {
                    // should we refetch the rounds ids from the new starting round instead of taking allRoundIds?
                    case (_, consensus) =>
                      consensus.addParallelRounds(checkedId, allRoundIds)
                  }
                  _ <- logger.debug(s"Added roundsIds=${logRoundIds(allRoundIds)} to parallelRounds.")
                } yield ()
              _ <- consensusRound.start().map(_ => ())
              _ <- logger.debug(s"Triggered start $roundType with roundIds=${logRoundIds(allRoundIds)}")
              _ <- reconciliationRound.modify(_ => (None, ()))
            } yield consensusRound.asRight[HistoricalRoundData]

          case RoundForGivenIdAlreadyTakingPlace(id, consensusRound) => {
            consensusRound.roundType match {
              case OwnRound | PeerRound =>
                logger.debug(
                  s"$roundType for ${logId(id)} didn't start because ${consensusRound.roundType} for that id is in progress."
                )
              case KeepUpRound if roundType == OwnRound =>
                logger.debug(
                  s"$roundType for ${logId(id)} didn't start because $KeepUpRound for that id is already running."
                )
              case KeepUpRound if roundType == PeerRound =>
                logger.error(
                  s"$roundType for ${logId(id)} didn't start because $KeepUpRound for that id is already running. But $PeerRound should always delete $KeepUpRound!"
                )
              case KeepUpRound =>
                for {
                  _ <- logger.debug(
                    s"${consensusRound.roundType} for given id=${logId(id)} is already running. Attempted to start $roundType."
                  )
                  roundIds <- consensusRound.getRoundIds()
                  _ <- if (roundIds.toSortedSet.intersect(allRoundIds).nonEmpty)
                    consensusRound.addRoundIds(allRoundIds)
                  else
                    F.unit
                } yield ()
            }
          } >>
            consensusRound.asRight[HistoricalRoundData].pure[F]

          case HistoricalRoundForGivenRoundsIdsAlreadyHappened(id, historicalRoundData) =>
            logger.debug(
              s"Round for given $roundType with roundIds=${logRoundIds(historicalRoundData.roundIds)} already happened and is Historical!"
            ) >>
              historicalRoundData.asLeft[HealthCheckConsensus[F]].pure[F]
        }
      }
    } yield result

  def startOwnConsensusForId(checkedId: Id): F[Unit] =
    for {
      allPeers <- cluster.getPeerInfo
      roundId <- createHealthcheckRoundId()
      delayedHealthCheckStatus <- F.start(PeerUnresponsive(checkedId).asInstanceOf[PeerHealthCheckStatus].pure[F])
      _ <- startNewRoundForId(
        allPeers,
        checkedId,
        roundId,
        None,
        delayedHealthCheckStatus,
        OwnRound
      )
    } yield ()

  def participateInNewPeerConsensus(
    checkedId: Id,
    roundId: HealthcheckRoundId
  ): F[Either[HistoricalRoundData, HealthCheckConsensus[F]]] =
    for {
      allPeers <- cluster.getPeerInfo
      checkedPeerData = allPeers.get(checkedId) // should I check the status if it's not offline or leaving at the moment
      delayedHealthCheckStatus <- F.start {
        checkedPeerData
          .fold(UnknownPeer(checkedId).asInstanceOf[PeerHealthCheckStatus].pure[F])(
            pd => peerHealthCheck.checkPeer(pd.peerMetadata.toPeerClientMetadata, healthCheckTimeout)
          )
      }
      result <- startNewRoundForId(
        allPeers,
        checkedId,
        roundId,
        None,
        delayedHealthCheckStatus,
        PeerRound
      )
    } yield result

  def startKeepUpRound(notification: NotifyAboutMissedConsensus): F[Unit] =
    for {
      allPeers <- cluster.getPeerInfo
      NotifyAboutMissedConsensus(checkedId, roundIds, ConsensusHealthStatus(_, _, roundId, _, _)) = notification
      checkedPeerData = allPeers.get(checkedId) // should I check the status if it's not offline or leaving at the moment
      delayedHealthCheckStatus <- F.start {
        checkedPeerData
          .fold(UnknownPeer(checkedId).asInstanceOf[PeerHealthCheckStatus].pure[F])(
            pd => peerHealthCheck.checkPeer(pd.peerMetadata.toPeerClientMetadata, healthCheckTimeout)
          )
      }
      _ <- startNewRoundForId(
        allPeers,
        checkedId,
        roundId,
        roundIds.some,
        delayedHealthCheckStatus,
        KeepUpRound
      )
    } yield ()

  def handleProposalForHistoricalRound(
    historical: HistoricalRoundData,
    healthProposal: ConsensusHealthStatus
  ): F[Either[SendProposalError, Unit]] =
    for {
      _ <- logger.debug(
        s"Attempted to process a proposal for a historical round! checkingId=${logId(healthProposal.checkingPeerId)} roundId=${logRoundId(healthProposal.roundId)}"
      )
      ConsensusHealthStatus(checkedId, checkingPeerId, roundId, status, clusterState) = healthProposal
      _ <- metrics.incrementMetricAsync("healthcheck_attemptedToProcessHistoricalRoundProposal_total")
      historicalHealthProposal = historical.roundPeers.get(checkingPeerId).map(_.healthStatus)
      result <- historicalHealthProposal match {
        case Some(Some(proposal)) if proposal == healthProposal =>
          ProposalAlreadyProcessed(roundId, historical.roundType)
            .asInstanceOf[SendProposalError]
            .asLeft[Unit]
            .pure[F]
        case Some(Some(_)) =>
          DifferentProposalForRoundIdAlreadyProcessed(
            checkingPeerId,
            Set(roundId),
            historical.roundType
          ).asInstanceOf[SendProposalError].asLeft[Unit].pure[F]
        case Some(None) =>
          ProposalNotProcessedForHistoricalRound(historical.roundIds)
            .asInstanceOf[SendProposalError]
            .asLeft[Unit]
            .pure[F]
        case None =>
          IdNotPartOfTheConsensus(checkingPeerId, historical.roundIds, historical.roundType)
            .asInstanceOf[SendProposalError]
            .asLeft[Unit]
            .pure[F]
      }
    } yield result

  def handleConsensusHealthProposal(healthProposal: ConsensusHealthStatus): F[Either[SendProposalError, Unit]] =
    for {
      allRounds <- consensusRounds.get
      ConsensusHealthStatus(checkedPeer, checkingPeerId, roundId, _, _) = healthProposal
      ConsensusRounds(historical, inProgress, keepUp) = allRounds // should we add keepUp rounds?
      roundForId = inProgress.get(checkedPeer.id)
      historicalRoundForRoundId = historical.find(_.roundIds.contains(roundId))
      _ <- logger.debug(s"Received proposal: ${logConsensusHealthStatus(healthProposal)}")
      result <- (roundForId, historicalRoundForRoundId) match {
        case (_, Some(historical)) => // do we need to check if the in progress round has the same roundId as found historical?
          handleProposalForHistoricalRound(historical, healthProposal)
        case (Some(round), _) =>
          logger.debug(
            s"Round for ${logId(checkedPeer)} exists, processing proposal! Proposal's ${logRoundIdWithOwner(roundId)} checkingId=${logId(checkingPeerId)}"
          ) >>
            round.processProposal(healthProposal)
        case (None, _) =>
          for {
            _ <- logger.debug(
              s"Round for ${logId(checkedPeer)} doesn't exist participating in new consensus round! Proposal's ${logRoundIdWithOwner(roundId)} checkingId=${logId(checkingPeerId)}"
            )
            historicalOrRunning <- participateInNewPeerConsensus(checkedPeer.id, roundId)
            result <- historicalOrRunning match {
              case Left(historical) =>
                handleProposalForHistoricalRound(historical, healthProposal)
              case Right(runningRound) if runningRound.roundType == KeepUpRound =>
                InternalErrorStartingRound(roundId).asInstanceOf[SendProposalError].asLeft[Unit].pure[F]
              case Right(runningRound) =>
                runningRound.processProposal(healthProposal)
            }
          } yield result
      }
      _ <- result match {
        case Left(error) => logSendProposalError(error)(metrics, logger)
        case Right(_)    => F.unit
      }
    } yield result

  def handleNotificationAboutMissedConsensus(notification: NotifyAboutMissedConsensus): F[Unit] =
    for {
      allRounds <- consensusRounds.get
      NotifyAboutMissedConsensus(checkedId, roundIds, _) = notification
      ConsensusRounds(historical, inProgress, keepUpRounds) = allRounds
      keepUpRoundsWithMathchingRoundIds <- keepUpRounds.toList.traverse {
        case (id, consensus) => consensus.getRoundIds().map(id -> (_, consensus))
      }.map(_.filter { case (_, (currentRoundIds, _)) => currentRoundIds.toSortedSet.intersect(roundIds).nonEmpty })
        .map(_.toMap)

      // should I only do these checks in startKeepUpRound?
      isRoundInProgressForId = inProgress.keySet.contains(checkedId)
      isKeepUpRoundForId = keepUpRounds.keySet.contains(checkedId)
      isKeepUpRoundForRoundIds = keepUpRoundsWithMathchingRoundIds.nonEmpty
      isHistoricalRoundForRoundIds = historical.exists(_.roundIds.intersect(roundIds).nonEmpty)

      _ <- if (isKeepUpRoundForRoundIds)
        keepUpRoundsWithMathchingRoundIds.values.toList.traverse {
          case (_, consensus) =>
            consensus.addRoundIds(roundIds)
        } else F.unit
      _ <- if (isRoundInProgressForId || isKeepUpRoundForRoundIds || isHistoricalRoundForRoundIds || isKeepUpRoundForId)
        F.unit
      else
        startKeepUpRound(notification)
    } yield ()

  def getClusterState(): F[ClusterState] =
    for {
      allRounds <- consensusRounds.get
      ConsensusRounds(_, inProgress, keepUpRounds) = allRounds
      rounds <- (keepUpRounds ++ inProgress).toList.traverse {
        case (id, consensus) => consensus.getRoundIds().map(id -> _.toSortedSet)
      }.map(_.toMap)
      ownState <- cluster.getNodeState
      ownJoinedHeight <- cluster.getOwnJoinedHeight()
      ownNodeReconciliationData = NodeReconciliationData(
        id = ownId,
        nodeState = ownState.some,
        roundInProgress = None,
        joiningHeight = ownJoinedHeight
      )
      allPeers <- cluster.getPeerInfo
      peersWeRunConsensusForButDontHave = (rounds -- allPeers.keySet).map {
        case (id, roundIds) =>
          id -> NodeReconciliationData(id = id, nodeState = None, roundInProgress = roundIds.some, joiningHeight = None)
      }
      clusterState = allPeers.map {
        case (id, peerData) =>
          id -> NodeReconciliationData(
            id = id,
            nodeState = peerData.peerMetadata.nodeState.some,
            roundInProgress = rounds.get(id),
            joiningHeight = peerData.majorityHeight.head.joined
          )
      } ++ peersWeRunConsensusForButDontHave + (ownId -> ownNodeReconciliationData)
      _ <- F.start(manageSchedulingReconciliationAfterSendingClusterState()).map(_ => ())
    } yield clusterState

  def proxySendHealthProposal(
    consensusHealthStatus: ConsensusHealthStatus,
    proxyToPeer: Id
  ): F[Either[SendProposalError, Unit]] =
    for {
      maybePeerData <- cluster.getPeerInfo.map(_.get(proxyToPeer))
      result <- maybePeerData match {
        case Some(peerData) =>
          logger.debug(
            s"Proxying healthProposal from ${logId(consensusHealthStatus.checkingPeerId)} to ${logId(proxyToPeer)}"
          ) >>
            PeerResponse
              .run(
                apiClient.healthcheck.sendPeerHealthStatus(SendPerceivedHealthStatus(consensusHealthStatus)),
                unboundedHealthBlocker
              )(peerData.peerMetadata.toPeerClientMetadata.copy(port = "9003"))
              .handleErrorWith(_ => ProxySendFailed(proxyToPeer).asInstanceOf[SendProposalError].asLeft[Unit].pure[F])
        case None =>
          logger.debug(
            s"Unknown peer=${logId(proxyToPeer)} to proxy healthProposal from ${logId(consensusHealthStatus.checkingPeerId)} to."
          ) >>
            UnknownProxyIdSending(proxyToId = proxyToPeer, proxyingId = ownId).asLeft[Unit].pure[F]
      }
    } yield result

  def proxyGetHealthStatusForRound(
    roundIds: Set[HealthcheckRoundId],
    proxyToPeer: Id,
    originId: Id
  ): F[Either[FetchProposalError, SendPerceivedHealthStatus]] =
    for {
      maybePeerData <- cluster.getPeerInfo.map(_.get(proxyToPeer))
      result <- maybePeerData match {
        case Some(peerData) =>
          logger.debug(s"Proxying getting healthProposal from ${logId(proxyToPeer)} for ${logId(originId)}") >>
            PeerResponse
              .run(
                apiClient.healthcheck.fetchPeerHealthStatus(FetchPeerHealthStatus(roundIds, originId)),
                unboundedHealthBlocker
              )(peerData.peerMetadata.toPeerClientMetadata.copy(port = "9003"))
              .handleErrorWith(
                _ =>
                  ProxyGetFailed(proxyToPeer).asInstanceOf[FetchProposalError].asLeft[SendPerceivedHealthStatus].pure[F]
              )
        case None =>
          logger.debug(
            s"Unknown peer=${logId(proxyToPeer)} to proxy getting healthProposal from for ${logId(originId)}."
          ) >>
            UnknownProxyIdFetching(proxyToId = proxyToPeer, proxyingId = ownId)
              .asLeft[SendPerceivedHealthStatus]
              .pure[F]
      }
    } yield result

  def getHealthStatusForRound(
    roundIds: Set[HealthcheckRoundId],
    originId: Id
  ): F[Either[FetchProposalError, SendPerceivedHealthStatus]] =
    for {
      allRounds <- consensusRounds.get
      ConsensusRounds(historical, inProgress, _) = allRounds
      matchingConsensuses <- inProgress.values.toList
        .traverse(
          consensus =>
            consensus
              .getRoundIds()
              .map((consensus, _))
        )
        .map(_.filter { case (_, consensusRoundIds) => consensusRoundIds.toSortedSet.intersect(roundIds).nonEmpty })
        .map(_.map { case (consensus, _) => consensus })
      matchingHistoricalConsensuses = historical.filter(_.roundIds.intersect(roundIds).nonEmpty)
      result <- (matchingConsensuses, matchingHistoricalConsensuses) match {
        case (Nil, Nil) =>
          NoConsensusForGivenRounds(roundIds)
            .asInstanceOf[FetchProposalError]
            .asLeft[SendPerceivedHealthStatus]
            .pure[F]
        case (List(_, _, _*), _) | (_, List(_, _, _*)) =>
          MoreThanOneConsensusForGivenRounds(roundIds)
            .asInstanceOf[FetchProposalError]
            .asLeft[SendPerceivedHealthStatus]
            .pure[F]
        case (_ :: Nil, _ :: Nil) =>
          HistoricalAndInProgressRoundMatchedForGivenRounds(roundIds)
            .asInstanceOf[FetchProposalError]
            .asLeft[SendPerceivedHealthStatus]
            .pure[F]
        case (consensus :: Nil, Nil) =>
          consensus.getOwnProposalAndMarkAsSent(originId).map(SendPerceivedHealthStatus(_).asRight[FetchProposalError])
        case (Nil, historicalRound :: Nil) =>
          SendPerceivedHealthStatus(historicalRound.ownConsensusHealthStatus).asRight[FetchProposalError].pure[F]
      }
    } yield result

  private def partitionBasedOnReadyness(
    consensuses: Map[Id, HealthCheckConsensus[F]]
  ): F[(Map[Id, HealthCheckConsensus[F]], Map[Id, HealthCheckConsensus[F]])] =
    for {
      withStatus <- consensuses.toList.traverse {
        case (id, consensus) => consensus.isReadyToCalculateOutcome().map((id, consensus, _))
      }
      (readyStatus, notReadyStatus) = withStatus.partition { case (_, _, isReady) => isReady }
      ready = Map(readyStatus.map { case (id, consensus, _)       => (id, consensus) }: _*)
      notReady = Map(notReadyStatus.map { case (id, consensus, _) => (id, consensus) }: _*)
    } yield (ready, notReady)

  // TODO: Consider if checking peer's PeerData and token isn't a good idea. If something changed the peer
  //       may be removed or updated.
  private def inspectConsensusPeers(consensuses: List[HealthCheckConsensus[F]]): F[Unit] =
    for {
      _ <- logger.debug("Started inspection of consensus peers!")
      peers <- cluster.getPeerInfo
      leavingOrOfflinePeers = peers.filter {
        case (_, peerData) => isInvalidForJoining(peerData.peerMetadata.nodeState)
      }.keySet
      _ <- consensuses.traverse { consensus =>
        for {
          roundPeers <- consensus.getRoundPeers()
          notProxiedPeers = roundPeers.filterNot { case (_, rd) => rd.peerData.isLeft }.keySet
          absentPeers = (notProxiedPeers -- peers.keySet) ++ notProxiedPeers.intersect(leavingOrOfflinePeers)
          _ <- if (absentPeers.nonEmpty) consensus.manageAbsentPeers(absentPeers) else F.unit
          proxiedPeers = roundPeers.filter { case (_, roundData) => roundData.peerData.isLeft }
          peersThanNoLongerNeedProxying = peers.filter {
            case (id, peerData) =>
              canActAsJoiningSource(peerData.peerMetadata.nodeState) && proxiedPeers.keySet.contains(id)
          }
          _ <- if (peersThanNoLongerNeedProxying.nonEmpty) consensus.manageProxiedPeers(peersThanNoLongerNeedProxying)
          else F.unit
        } yield ()
      }
    } yield ()

  private def calculateConsensusOutcome(
    consensuses: Map[Id, HealthCheckConsensus[F]]
  ): F[Map[Id, (Option[HealthcheckConsensusDecision], HealthCheckConsensus[F])]] =
    if (consensuses.isEmpty) Map.empty[Id, (Option[HealthcheckConsensusDecision], HealthCheckConsensus[F])].pure[F]
    else
      logger.debug("Started calculation of consensus outcome.") >>
        consensuses.toList.traverse {
          case (id, consensus) =>
            consensus
              .calculateConsensusOutcome()
              .map(id -> (_, consensus))
        }.map(_.toMap)

  private def fetchAbsentPeers(consensuses: Map[Id, HealthCheckConsensus[F]]): F[Set[Id]] =
    if (consensuses.isEmpty) Set.empty[Id].pure[F]
    else
      logger.debug("Started fetching absent peers.") >>
        consensuses.toList.traverse {
          case (_, consensus) =>
            consensus
              .getRoundPeers()
              .map(_.filter { case (_, roundData) => roundData.peerData.isLeft }.keySet)
        }.map(_.flatten.toSet)

  private def findNewPeers(currentPeers: Map[Id, PeerData], consensuses: Map[Id, HealthCheckConsensus[F]]): F[Set[Id]] =
    if (consensuses.isEmpty) Set.empty[Id].pure[F]
    else
      logger.debug("Finding new peers.") >>
        consensuses.toList.traverse {
          case (_, consensus) =>
            consensus
              .getRoundPeers()
              .map(_.keySet)
        }.map(_.flatten.toSet)
          .map(currentPeers.keySet -- consensuses.keySet -- _)

  def notifyRemainingPeersAboutMissedConsensuses(remainingPeers: Set[Id]) =
    for {
      peers <- cluster.getPeerInfo
      peersWeHave = peers -- peers.keySet.diff(remainingPeers)
      inProgress <- getInProgressRounds()
      missedInProgress <- inProgress.values.toList.traverse { consensus =>
        for {
          removed <- consensus.getRemovedUnresponsivePeersWithParallelRound()
          notification <- consensus.generateMissedConsensusNotification()
        } yield (removed, notification)
      }
      peersWithMissedConsensus = peersWeHave.map {
        case (id, peerData) =>
          (peerData, missedInProgress.filter { case (removed, _) => removed.contains(id) }.map(_._2))
      }.map { case (peerData, notifications) => notifications.map((peerData, _)) }.flatten.toList
      _ <- peersWithMissedConsensus.traverse {
        case (peerData, notification) =>
          PeerResponse
            .run(
              apiClient.healthcheck.notifyAboutMissedConsensus(notification),
              unboundedHealthBlocker
            )(peerData.peerMetadata.toPeerClientMetadata)
            .handleErrorWith(e => logger.debug(e)(s"Failed to notify peer about missed consensus."))
      }
    } yield ()

  def addMissingPeer(id: Id, consensus: HealthCheckConsensus[F]): F[Unit] = {
    for {
      roundIds <- consensus.getRoundIds()
      _ <- logger.debug(s"Adding peer ${logId(id)} after decision for consensus with roundIds=${logRoundIds(roundIds)}")
      peersIps <- consensus.getCheckedPeersIps()
      ip <- peersIps match {
        case Some(NonEmptyList(ip, Nil)) =>
          logger.debug(s"Found ip=$ip for id ${logId(id)} that's about to be added!") >> ip.pure[F]
        case Some(NonEmptyList(ip, otherIps)) =>
          logger.warn(
            s"Found multiple ip's for peer ${logId(id)} that's being added. Will use most frequent one -> ip=$ip other ips=$otherIps"
          ) >>
            ip.pure[F]
        case None =>
          logger.error(s"Couldn't find any ip address for peer ${logId(id)} that can be used to attempt registration!") >>
            F.raiseError(new Throwable(s"No ip found for peer during adding missing peer process!"))
      }
      peerClientMetadata = PeerClientMetadata(ip, peerHttpPort, id)
      _ <- logger.debug(s"Adding the missing peer ${logId(id)} with peerClientMetadata=$peerClientMetadata")
      _ <- cluster.attemptRegisterPeer(peerClientMetadata, isReconciliationJoin = true)
    } yield ()
  }.handleErrorWith { error =>
    logger.warn(error)(s"Adding missing peer with id=${logId(id)} failed!")
  }

  def monitorAndManageConsensuses(consensuses: Map[Id, HealthCheckConsensus[F]]): F[Unit] =
    for {
      peers <- cluster.getPeerInfo
      notOfflinePeers = peers.filter {
        case (_, peerData) => canActAsJoiningSource(peerData.peerMetadata.nodeState)
      }.keySet
      partitioned <- partitionBasedOnReadyness(consensuses)
      (ready, notReady) = partitioned
      _ <- if (ready.nonEmpty) logger.debug(s"Ready consensuses ${logIds(ready.keySet)}") else F.unit
      readyAfterInspection <- if (notReady.nonEmpty) {
        for {
          _ <- logger.debug(s"NotReady consensuses ${logIds(notReady.keySet)}")
          _ <- inspectConsensusPeers(notReady.values.toList)
          result <- partitionBasedOnReadyness(notReady).map { case (ready, _) => ready }
          _ <- if (result.nonEmpty) logger.debug(s"Ready after inspection consensuses ${logIds(result.keySet)}")
          else F.unit
        } yield result
      } else
        Map.empty[Id, HealthCheckConsensus[F]].pure[F]
      toManage = notReady -- readyAfterInspection.keySet
      _ <- toManage.values.toList.traverse(c => F.start(c.runManagementTasks()))
      readyToCalculateOutcome = ready ++ readyAfterInspection
      peersWithDecision <- calculateConsensusOutcome(readyToCalculateOutcome) // <- I guess it shouldn't return optional value, if it's ready the decision should be made
      _ <- if (peersWithDecision.nonEmpty) logger.debug(s"peersWithDecision: ${logIds(peersWithDecision.keySet)}")
      else F.unit
      peersWeDidntHave <- fetchAbsentPeers(readyToCalculateOutcome)
      peersWeStillDontHave = peersWeDidntHave -- notOfflinePeers
      _ <- if (peersWeDidntHave.nonEmpty) logger.debug(s"peersWeDidntHave: ${logIds(peersWeDidntHave)}") else F.unit
      _ <- if (peersWeStillDontHave.nonEmpty) logger.debug(s"peersWeStillDontHave: ${logIds(peersWeStillDontHave)}")
      else F.unit
      // should peers here also be filtered base on offline/online states?
      // Also shouldn't we only find newPeers for finished rounds?
      newPeers <- findNewPeers(peers, consensuses)
      _ <- if (newPeers.nonEmpty) logger.debug(s"newPeers: ${logIds(newPeers)}") else F.unit
      toRemove = peersWithDecision.filter {
        case (_, (maybeDecision, _)) => maybeDecision.exists(_.isInstanceOf[PeerOffline])
      }
      toRemain = peersWithDecision.filter {
        case (_, (maybeDecision, _)) => maybeDecision.exists(_.isInstanceOf[PeerOnline])
      }
      unknown = peersWithDecision.filter { case (_, (maybeDecision, _)) => maybeDecision.isEmpty }
      _ <- if (toRemove.nonEmpty) logger.debug(s"toRemove: ${logIds(toRemove.keySet)}") else F.unit
      _ <- if (toRemain.nonEmpty) logger.debug(s"toRemain: ${logIds(toRemain.keySet)}") else F.unit
      _ <- if (unknown.nonEmpty)
        logger.debug(s"unknown: ${logIds(unknown.keySet)}") >>
          incrementFatalIntegrityHealthcheckError(metrics)
      else F.unit
      // mark offline those peers that we do have in our list
      toMarkOfflinePeers = peers.filterKeys(toRemove.keySet.contains).values.toList
      _ <- peerHealthCheck.markOffline(toMarkOfflinePeers)
      // should I check the status??? I think yes as it may be that it's offline but stuck so not removed from the list and we will not add because of that
      peersToAdd = toRemain -- notOfflinePeers
      _ <- if (peersToAdd.nonEmpty)
        logger.warn(s"Peers that we don't have online but we should: ${logIds(peersToAdd.keySet)}")
      else F.unit
      _ <- peersToAdd.toList.traverse { case (id, (_, consensus)) => addMissingPeer(id, consensus) }
      _ <- peersToRunConsensusFor.modify(peers => (peers ++ peersWeStillDontHave, ()))
      _ <- peersThatNeedReconciliation.modify(peers => (peers ++ newPeers, ()))
      _ <- notifyRemainingPeersAboutMissedConsensuses(toRemain.keySet)
      finishedRounds = toRemove ++ toRemain
      _ <- finishedRounds.toList.traverse {
        case (id, (decision, _)) =>
          decision match {
            case Some(decision) => logHealthcheckConsensusDecicion(decision, logger)
            case None           => logger.debug(s"HealthcheckConsensus decision for id=${logId(id)} COULDN'T BE CALCULATED")
          }
      }
      finishedWithHistoricalData <- finishedRounds.toList.traverse {
        case (_, (decision, consensus)) =>
          consensus.generateHistoricalRoundData(decision)
      }
      _ <- consensusRounds.modify {
        case ConsensusRounds(historical, inProgress, keepUpRounds) =>
          // theoretically we could just remove all finished from inProgress an keepUp as it shouldn't happen that we have rounds for the same id in both
          val (finishedOwnOrPeerRounds, finishedKeepUpRounds) =
            finishedWithHistoricalData.partition(_.roundType.previousType match {
              case OwnRound | PeerRound => true
              case KeepUpRound          => false
            })
          val updatedInProgress = inProgress -- finishedOwnOrPeerRounds.map(_.checkedPeer.id).toSet
          val updatedKeepUp = keepUpRounds -- finishedKeepUpRounds.map(_.checkedPeer.id).toSet
          val updatedHistorical = historical ++ finishedWithHistoricalData

          (ConsensusRounds(updatedHistorical, updatedInProgress, updatedKeepUp), ())
      }
    } yield ()

  def manageReconciliationRound(round: ReconciliationRound[F]): F[Unit] =
    for {
      result <- round.getReconciliationResult()
      _ <- result match {
        case Some(rr @ ReconciliationResult(peersToRunHealthCheckFor, misalignedPeers, _)) =>
          for {
            currentTime <- getTimeInSeconds()
            roundId = round.roundId
            _ <- peersToRunHealthcheckFor.modify(current => (current ++ peersToRunHealthCheckFor, ()))
            _ <- peersToRunConsensusFor.modify(current => (current ++ misalignedPeers, ()))
            _ <- timeOfLastOwnReconciliationRound.modify(_ => (currentTime.some, ()))
            _ <- runReconciliationRoundAfter.modify(_ => (None, ()))
            _ <- reconciliationRound.modify(_ => (None, ()))
            _ <- logger.debug(
              s"Own[${logId(ownId)}] reconciliation round with id=$roundId result: ${logReconciliationResult(rr)}"
            )
            _ <- metrics.incrementMetricAsync("healthcheck_ownReconciliationRounds_total")
          } yield ()
        case None => F.unit
      }
    } yield ()

  // Schedule between 2 and (2 minutes + 15 seconds * peersCount) from now.
  // So the range gets bigger along with peers list getting bigger.
  def scheduleEpoch(peersCount: Int): F[FiniteDuration] =
    for {
      currentEpoch <- getTimeInSeconds()
      delayTime <- F.delay(Random.nextInt(15 * peersCount.max(1)).seconds)
      result = currentEpoch + 2.minutes + delayTime
    } yield result

  def manageSchedulingReconciliation(): F[Unit] =
    for {
      peersCount <- cluster.getPeerInfo.map(_.size)
      currentEpoch <- getTimeInSeconds()
      peersThatNeedReconciliation <- peersThatNeedReconciliation.get
      runAfter <- runReconciliationRoundAfter.get
      _ <- runAfter match {
        case Some(roundEpoch) if roundEpoch < currentEpoch =>
          for {
            roundId <- createRoundId()
            round <- reconciliationRound.modify { _ =>
              val round = ReconciliationRound[F](
                ownId,
                cluster,
                currentEpoch,
                roundId,
                apiClient,
                unboundedHealthBlocker,
                healthHttpPort = healthHttpPort
              )
              (round.some, round)
            }
            _ <- round.start().map(_ => ())
          } yield ()
        case Some(_) => F.unit
        case None =>
          for {
            scheduleTime <- scheduleEpoch(peersCount)
            _ <- runReconciliationRoundAfter.modify(_ => (scheduleTime.some, scheduleTime)) >>=
              (time => logger.debug(s"Scheduled reconciliation round at $time epoch"))
          } yield ()
      }
    } yield ()

  def manageSchedulingReconciliationAfterSendingClusterState(): F[Unit] =
    for {
      currentEpoch <- getTimeInSeconds()
      _ <- peersThatNeedReconciliation.modify(_ => (Set.empty, ()))
      lastOwnReconciliation <- timeOfLastOwnReconciliationRound.get
      shouldCancel = lastOwnReconciliation
        .map(_ + maxTimespanWithoutOwnReconciliationRound)
        .exists(_ > currentEpoch)
      _ <- if (shouldCancel)
        runReconciliationRoundAfter.modify(_ => (None, ()))
      else
        F.unit
    } yield ()

  def manageReconciliation(): F[Unit] =
    for {
      maybeRound <- reconciliationRound.get
      _ <- maybeRound match {
        case Some(round) => manageReconciliationRound(round)
        case None        => manageSchedulingReconciliation()
      }
    } yield ()

  def setConsensusesMetrics(consensusRounds: ConsensusRounds[F]) =
    for {
      allRounds <- consensusRounds.pure[F]
      ConsensusRounds(historical, inProgress, keepUp) = allRounds
      historicalOwnCount = historical.count(_.roundType.previousType == OwnRound)
      historicalPeerCount = historical.count(_.roundType.previousType == PeerRound)
      historicalKeepUpCount = historical.count(_.roundType.previousType == KeepUpRound)
      _ <- metrics.updateMetricAsync("healthcheck_inProgressRoundsCount", inProgress.size)
      _ <- metrics.updateMetricAsync("healthcheck_keepUpRoundsCount", keepUp.size)
      _ <- metrics.updateMetricAsync("healthcheck_historicalRoundsCount", historical.size)
      _ <- metrics.updateMetricAsync("healthcheck_historicalOwnRoundsCount", historicalOwnCount)
      _ <- metrics.updateMetricAsync("healthcheck_historicalPeerRoundsCount", historicalPeerCount)
      _ <- metrics.updateMetricAsync("healthcheck_historicalKeepUpRoundsCount", historicalKeepUpCount)
    } yield ()

  def triggerMonitorAndManageConsensuses(): F[Unit] =
    for {
      allRounds <- consensusRounds.get
      ConsensusRounds(_, inProgress, keepUp) = allRounds
      _ <- F.start(setConsensusesMetrics(allRounds))
      consensuses = keepUp ++ inProgress
      _ <- if (consensuses.nonEmpty) monitorAndManageConsensuses(consensuses) else manageReconciliation()
    } yield ()

  def triggerHealthcheckManagement(): F[Unit] =
    for {
      _ <- triggerMonitorAndManageConsensuses()
      _ <- F.start(checkPeersHealth())
      peersToRunConsensusFor <- peersToRunConsensusFor.modify(peers => (Set.empty, peers))
      /*
      TODO: consider adding a logic checking if not too many consensuses are started at the same time
       e.g. when reconciliation detects one peer that misses a lot of other peers - this would
       trigger consensus for every single one of these peers
       */
      _ <- peersToRunConsensusFor.toList.traverse(id => startOwnConsensusForId(id))
    } yield ()

  def checkPeersHealth(): F[Unit] = {
    def check(): F[Unit] =
      for {
        peersUnderConsensus <- getPeersUnderConsensus()
        peersToRunHealthcheckFor <- peersToRunHealthcheckFor.get
        unresponsivePeers <- peerHealthCheck
          .check(peersUnderConsensus, peersToRunHealthcheckFor)
          .map(_.map(_.peerMetadata.id).toSet)
        notOfflinePeers <- cluster.getPeerInfo.map(_.filter {
          case (_, peerData) => canActAsJoiningSource(peerData.peerMetadata.nodeState)
        }.keySet)
        unresponsiveNotOfflinePeers = unresponsivePeers.intersect(notOfflinePeers)
        _ <- unresponsiveNotOfflinePeers.toList.traverse(id => startOwnConsensusForId(id))
      } yield ()

    for {
      isAcquired <- checkingPeersHealthLock.tryAcquire
      _ <- if (isAcquired)
        check().handleErrorWith { e =>
          logger.error(e)("Error checking peers health.")
        } >> checkingPeersHealthLock.release
      else
        F.unit
    } yield ()
  }

  def areThereConsensusRoundsInProgress: F[Boolean] =
    getInProgressRounds().map(_.nonEmpty)

  def getPeersUnderConsensus(): F[Set[Id]] =
    getInProgressRounds().map(_.keySet)

  sealed trait ConsensusRoundResponse
  case class RoundStarted(id: Id, consensus: HealthCheckConsensus[F]) extends ConsensusRoundResponse
  case class RoundForGivenIdAlreadyTakingPlace(id: Id, consensus: HealthCheckConsensus[F])
      extends ConsensusRoundResponse
  case class HistoricalRoundForGivenRoundsIdsAlreadyHappened(id: Id, historicalRoundData: HistoricalRoundData)
      extends ConsensusRoundResponse
}

object HealthCheckConsensusManager {

  def apply[F[_]: Concurrent: ContextShift: Parallel: Timer](
    ownId: Id,
    cluster: Cluster[F],
    peerHealthCheck: PeerHealthCheck[F],
    metrics: Metrics,
    apiClient: ClientInterpreter[F],
    unboundedHealthBlocker: Blocker,
    healthHttpPort: Int,
    peerHttpPort: Int
  ): HealthCheckConsensusManager[F] =
    new HealthCheckConsensusManager(
      ownId,
      cluster,
      peerHealthCheck,
      metrics,
      apiClient,
      unboundedHealthBlocker,
      healthHttpPort = healthHttpPort,
      peerHttpPort = peerHttpPort
    )

  //Error
  sealed trait HealthCheckManagerError extends Exception
  sealed trait FetchProposalError extends HealthCheckManagerError

  object FetchProposalError {
    implicit val fetchProposalErrorCodec: Codec[FetchProposalError] = deriveCodec
  }

  case class NoConsensusForGivenRounds(roundIds: Set[HealthcheckRoundId]) extends FetchProposalError

  case class MoreThanOneConsensusForGivenRounds(roundIds: Set[HealthcheckRoundId]) extends FetchProposalError

  case class HistoricalAndInProgressRoundMatchedForGivenRounds(roundIds: Set[HealthcheckRoundId])
      extends FetchProposalError

  case class FetchingFailed(id: Id) extends FetchProposalError

  case class UnknownProxyIdFetching(proxyToId: Id, proxyingId: Id) extends FetchProposalError

  case class ProxyGetFailed(id: Id) extends FetchProposalError

  sealed trait SendProposalError extends HealthCheckManagerError

  object SendProposalError {
    implicit val sendProposalErrorCodec: Codec[SendProposalError] = deriveCodec
  }

  case class SendingFailed(id: Id) extends SendProposalError

  case class ProposalAlreadyProcessed(roundId: HealthcheckRoundId, roundType: HealthcheckRoundType)
      extends SendProposalError

  case class DifferentProposalForRoundIdAlreadyProcessed(
    checkingPeerId: Id,
    roundIds: Set[HealthcheckRoundId],
    roundType: HealthcheckRoundType
  ) extends SendProposalError

  case class DifferentProposalAlreadyProcessedForCheckedId(
    checkedId: Id,
    checkingPeerId: Id,
    roundIds: Set[HealthcheckRoundId],
    roundType: HealthcheckRoundType
  ) extends SendProposalError

  case class IdNotPartOfTheConsensus(id: Id, roundIds: Set[HealthcheckRoundId], roundType: HealthcheckRoundType)
      extends SendProposalError

  case class ProposalNotProcessedForHistoricalRound(roundIds: Set[HealthcheckRoundId]) extends SendProposalError

  case class InternalErrorStartingRound(roundId: HealthcheckRoundId) extends SendProposalError

  case class UnknownProxyIdSending(proxyToId: Id, proxyingId: Id) extends SendProposalError

  case class ProxySendFailed(id: Id) extends SendProposalError

  case class HistoricalRoundData(
    checkedPeer: CheckedPeer,
    startedAtSecond: FiniteDuration,
    finishedAtSecond: FiniteDuration,
    roundIds: Set[HealthcheckRoundId],
    decision: Option[HealthcheckConsensusDecision],
    roundPeers: Map[Id, RoundData],
    parallelRounds: Map[Id, Set[HealthcheckRoundId]],
    removedPeers: Map[Id, NonEmptyList[PeerRemovalReason]],
    ownConsensusHealthStatus: ConsensusHealthStatus,
    roundType: HistoricalRound
  )

  case class ConsensusRounds[F[_]](
    historical: List[HistoricalRoundData],
    inProgress: Map[Id, HealthCheckConsensus[F]],
    keepUpRounds: Map[Id, HealthCheckConsensus[F]]
  )

  sealed trait HealthcheckRoundType

  object HealthcheckRoundType {
    implicit val healthcheckRoundTypeCodec: Codec[HealthcheckRoundType] = deriveCodec
  }
  sealed trait HealthcheckInactiveRoundType extends HealthcheckRoundType

  object HealthcheckInactiveRoundType {
    implicit val healthcheckInactiveRoundTypeCodec: Codec[HealthcheckInactiveRoundType] = deriveCodec
  }

  case class HistoricalRound(previousType: HealthcheckActiveRoundType) extends HealthcheckInactiveRoundType

  sealed trait HealthcheckActiveRoundType extends HealthcheckRoundType

  object HealthcheckActiveRoundType {
    implicit val healthcheckActiveRoundTypeCodec: Codec[HealthcheckActiveRoundType] = deriveCodec
  }

  case object OwnRound extends HealthcheckActiveRoundType
  case object PeerRound extends HealthcheckActiveRoundType
  case object KeepUpRound extends HealthcheckActiveRoundType

  object EitherCodec {
    implicit def eitherEncoder[A, B](implicit a: Encoder[A], b: Encoder[B]): Encoder[Either[A, B]] =
      Encoder.encodeEither("left", "right")
    implicit def eitherDecoder[A, B](implicit a: Decoder[A], b: Decoder[B]): Decoder[Either[A, B]] =
      Decoder.decodeEither("left", "right")
  }
}
