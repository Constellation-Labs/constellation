package org.constellation.domain.healthcheck

import cats.Parallel
import cats.data.NonEmptyList
import cats.effect.concurrent.{Ref, Semaphore}
import cats.effect.{Blocker, Clock, Concurrent, ContextShift, Fiber, Timer}
import cats.syntax.all._
import io.chrisdavenport.fuuid.FUUID
import io.circe.generic.semiauto.deriveCodec
import io.circe.{Codec, Decoder, Encoder}
import org.constellation.ConstellationExecutionContext.createSemaphore
import org.constellation.domain.cluster.ClusterStorageAlgebra
import org.constellation.domain.healthcheck.HealthCheckConsensus._
import org.constellation.domain.healthcheck.HealthCheckConsensusManagerBase._
import org.constellation.domain.healthcheck.HealthCheckLoggingHelper._
import org.constellation.infrastructure.p2p.{ClientInterpreter, PeerResponse}
import org.constellation.p2p.{Cluster, PeerData}
import org.constellation.schema.Id
import org.constellation.schema.NodeState.{canActAsJoiningSource, isInvalidForJoining}
import org.constellation.schema.consensus.RoundId

import scala.concurrent.duration.{DurationLong, FiniteDuration, SECONDS}

abstract class HealthCheckConsensusManagerBase[
  F[_],
  K <: HealthCheckKey: Encoder: Decoder,
  A <: HealthCheckStatus: Encoder: Decoder,
  B <: ConsensusHealthStatus[K, A]: Encoder: Decoder,
  C <: SendConsensusHealthStatus[K, A, B]: Encoder: Decoder
](
  ownId: Id,
  cluster: Cluster[F],
  clusterStorage: ClusterStorageAlgebra[F],
  metrics: PrefixedHealthCheckMetrics[F],
  apiClient: ClientInterpreter[F],
  unboundedHealthBlocker: Blocker,
  healthHttpPort: Int,
  peerHttpPort: Int,
  driver: HealthCheckConsensusTypeDriver[K, A, B],
  healthCheckType: HealthCheckType
)(implicit F: Concurrent[F], CS: ContextShift[F], P: Parallel[F], C: Clock[F], T: Timer[F]) {

  val logger: PrefixedHealthCheckLogger[F] = new PrefixedHealthCheckLogger[F](healthCheckType)

  // TO IMPLEMENT
  def checkHealthForPeer(key: K): F[Fiber[F, A]]

  def periodicOperationWhenNoConsensusesInProgress(): F[Unit]

  def periodicOperation(): F[Unit]

  def periodicPeersHealthCheck(): F[Unit]

  def readyConsensusesAction(consensuses: Map[K, HealthCheckConsensus[F, K, A, B, C]]): F[Unit]

  def onSuccessfullRoundStart(): F[Unit]

  def positiveOutcomeAction(
    positiveOutcomePeers: Map[K, (HealthcheckConsensusDecision[K], HealthCheckConsensus[F, K, A, B, C])]
  ): F[Unit]

  def negativeOutcomeAction(
    negativeOutcomePeers: Map[K, (HealthcheckConsensusDecision[K], HealthCheckConsensus[F, K, A, B, C])]
  ): F[Unit] =
    for {
      peers <- clusterStorage.getPeers
      toMarkOfflinePeers = peers.filterKeys(negativeOutcomePeers.keySet.map(_.id).contains).values.toList
      _ <- markOffline(toMarkOfflinePeers)
    } yield ()

  val consensusRounds: Ref[F, ConsensusRounds[F, K, A, B, C]] =
    Ref.unsafe(ConsensusRounds(List.empty, Map.empty))

  val checkingPeersHealthLock: Semaphore[F] = createSemaphore()

  def getTimeInSeconds(): F[FiniteDuration] = C.monotonic(SECONDS).map(_.seconds)

  def getHistoricalRounds(): F[List[HistoricalRoundData[K, A, B]]] = consensusRounds.get.map(_.historical)

  private def getInProgressRounds(): F[Map[K, HealthCheckConsensus[F, K, A, B, C]]] =
    consensusRounds.get.map(_.inProgress)

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

  def markOffline(peers: List[PeerData]): F[Unit] =
    peers.traverse { pd =>
      (for {
        _ <- logger.info(s"Marking dead peer: ${pd.peerMetadata.id.short} (${pd.peerMetadata.host}) as offline")
        _ <- unboundedHealthBlocker.blockOn(cluster.markOfflinePeer(pd.peerMetadata.id))
        _ <- metrics.updateMetricAsync("deadPeer", pd.peerMetadata.host)
      } yield ())
        .handleErrorWith(_ => logger.error("Cannot mark peer as offline - error / skipping to next peer if available"))
    }.void

  private def runPeriodicPeersHealthCheck(): F[Unit] =
    for {
      isAcquired <- checkingPeersHealthLock.tryAcquire
      _ <- if (isAcquired)
        periodicPeersHealthCheck().handleErrorWith { e =>
          logger.error(e)("Error checking peers health.")
        } >> checkingPeersHealthLock.release
      else
        F.unit
    } yield ()

  private def startNewRoundForId(
    key: K,
    allPeers: Map[Id, PeerData],
    roundId: HealthcheckRoundId,
    delayedHealthCheckStatus: Fiber[F, A],
    roundType: HealthcheckActiveRoundType
  ): F[Either[HistoricalRoundData[K, A, B], HealthCheckConsensus[F, K, A, B, C]]] =
    for {
      startedAt <- getTimeInSeconds()
      checkedId = key.id
      peersIp = allPeers.find { case (id, _) => id == checkedId }.map {
        case (_, peerData) => peerData.peerMetadata.host
      }
      checkedPeer = CheckedPeer(checkedId, peersIp)
      allRoundIds = Set(roundId)
      response <- consensusRounds.modify {
        case ConsensusRounds(historical, inProgress) =>
          lazy val roundPeers = pickPeersForConsensus(allPeers, Set(checkedId))
          lazy val consensusRound: HealthCheckConsensus[F, K, A, B, C] =
            HealthCheckConsensus[F, K, A, B, C](
              key,
              checkedPeer,
              ownId,
              roundId,
              roundPeers,
              delayedHealthCheckStatus,
              startedAt,
              roundType,
              metrics,
              apiClient,
              unboundedHealthBlocker,
              healthHttpPort = healthHttpPort,
              driver,
              healthCheckType
            )
          val historicalRoundForRoundIds = historical.find(_.roundIds.intersect(allRoundIds).nonEmpty)
          val roundForKey = inProgress.get(key)

          val (updatedInProgress, consensusRoundResponse): (
            Map[K, HealthCheckConsensus[F, K, A, B, C]],
            ConsensusRoundResponse
          ) =
            (roundForKey, historicalRoundForRoundIds) match {
              case (_, Some(historical)) =>
                (inProgress, HistoricalRoundForGivenRoundsIdsAlreadyHappened(key, historical))
              case (Some(existingRound), _) =>
                (inProgress, RoundForGivenKeyAlreadyTakingPlace(key, existingRound))
              case (None, _) =>
                (inProgress + (key -> consensusRound), RoundStarted(key, consensusRound))
            }

          (
            ConsensusRounds(historical = historical, inProgress = updatedInProgress),
            consensusRoundResponse
          )
      }
      result <- {
        response match {
          case RoundStarted(key, consensusRound) =>
            for {
              _ <- logger.debug(
                s"Started $roundType for key=${logHealthCheckKey(key)} with ${logRoundIdWithOwner(roundId)} and peers=${logIds(
                  allPeers.keySet
                )} at $startedAt! allRoundIds=${logRoundIds(allRoundIds)}"
              )
              inProgressRounds <- getInProgressRounds()
              parallelRounds = inProgressRounds.filterKeys(_ != key)
              _ <- if (parallelRounds.isEmpty)
                logger.debug(
                  s"No parallel rounds to add to $roundType with roundId=${logRoundId(roundId)} or vice versa."
                )
              else
                for {
                  _ <- parallelRounds.toList.traverse {
                    case (key, consensus) =>
                      consensus.getRoundIds() >>= { roundIds =>
                        consensusRound.addParallelRounds(key, roundIds.toSortedSet)
                      }
                  }
                  _ <- logger.debug(s"Added parallel rounds to $roundType with roundIds=${logRoundIds(allRoundIds)}")
                  _ <- parallelRounds.toList.traverse {
                    // should we refetch the rounds ids from the new starting round instead of taking allRoundIds?
                    case (_, consensus) =>
                      consensus.addParallelRounds(key, allRoundIds)
                  }
                  _ <- logger.debug(s"Added roundsIds=${logRoundIds(allRoundIds)} to parallelRounds.")
                } yield ()
              _ <- consensusRound.start().map(_ => ())
              _ <- logger.debug(s"Triggered start $roundType with roundIds=${logRoundIds(allRoundIds)}")
              _ <- onSuccessfullRoundStart()
            } yield consensusRound.asRight[HistoricalRoundData[K, A, B]]

          case RoundForGivenKeyAlreadyTakingPlace(key, consensusRound) => {
            logger.debug(
              s"$roundType for ${logHealthCheckKey(key)} didn't start because ${consensusRound.roundType} for that id is in progress."
            )
          } >>
            consensusRound.asRight[HistoricalRoundData[K, A, B]].pure[F]

          case HistoricalRoundForGivenRoundsIdsAlreadyHappened(_, historicalRoundData) =>
            logger.debug(
              s"Round for given $roundType with roundIds=${logRoundIds(historicalRoundData.roundIds)} already happened and is Historical!"
            ) >>
              historicalRoundData.asLeft[HealthCheckConsensus[F, K, A, B, C]].pure[F]
        }
      }
    } yield result

  def startOwnConsensusForId(key: K, delayedHealthCheckStatus: Fiber[F, A]): F[Unit] =
    for {
      allPeers <- clusterStorage.getPeers
      roundId <- createHealthcheckRoundId()
      _ <- startNewRoundForId(
        key,
        allPeers,
        roundId,
        delayedHealthCheckStatus,
        OwnRound
      )
    } yield ()

  def participateInNewPeerConsensus(
    key: K,
    roundId: HealthcheckRoundId
  ): F[Either[HistoricalRoundData[K, A, B], HealthCheckConsensus[F, K, A, B, C]]] =
    for {
      allPeers <- clusterStorage.getPeers
      delayedHealthCheckStatus <- checkHealthForPeer(key)
      result <- startNewRoundForId(
        key,
        allPeers,
        roundId,
        delayedHealthCheckStatus,
        PeerRound
      )
    } yield result

  def handleProposalForHistoricalRound(
    historical: HistoricalRoundData[K, A, B],
    healthProposal: B
  ): F[Either[SendProposalError, Unit]] =
    for {
      _ <- logger.debug(
        s"Attempted to process a proposal for a historical round! checkingId=${logId(healthProposal.checkingPeerId)} key=${logHealthCheckKey(
          healthProposal.key
        )} roundId=${logRoundId(healthProposal.roundId)}"
      )
      checkingPeerId = healthProposal.checkingPeerId
      roundId = healthProposal.roundId
      _ <- metrics.incrementMetricAsync("attemptedToProcessHistoricalRoundProposal_total")
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

  def handleConsensusHealthProposal(
    healthProposal: B
  ): F[Either[SendProposalError, Unit]] =
    for {
      allRounds <- consensusRounds.get
      key = healthProposal.key
      checkingPeerId = healthProposal.checkingPeerId
      roundId = healthProposal.roundId
      ConsensusRounds(historical, inProgress) = allRounds
      roundForId = inProgress.get(key)
      historicalRoundForRoundId = historical.find(_.roundIds.contains(roundId))
      _ <- logger.debug(s"Received proposal: ${logConsensusHealthStatus(healthProposal)}")
      result <- (roundForId, historicalRoundForRoundId) match {
        case (_, Some(historical)) => // do we need to check if the in progress round has the same roundId as found historical?
          handleProposalForHistoricalRound(historical, healthProposal)
        case (Some(round), _) =>
          logger.debug(
            s"Round for key ${logHealthCheckKey(key)} exists, processing proposal! Proposal's ${logRoundIdWithOwner(roundId)} checkingId=${logId(checkingPeerId)}"
          ) >>
            round.processProposal(healthProposal)
        case (None, _) =>
          for {
            _ <- logger.debug(
              s"Round for key ${logHealthCheckKey(key)} doesn't exist participating in new consensus round! Proposal's ${logRoundIdWithOwner(roundId)} checkingId=${logId(checkingPeerId)}"
            )
            //foo: Fiber[F, A] = ???
            historicalOrRunning <- participateInNewPeerConsensus(key, roundId)
            result <- historicalOrRunning match {
              case Left(historical) =>
                handleProposalForHistoricalRound(historical, healthProposal)
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

  def proxySendHealthProposal(
    consensusHealthStatus: B,
    proxyToPeer: Id
  ): F[Either[SendProposalError, Unit]] =
    for {
      maybePeerData <- clusterStorage.getPeers.map(_.get(proxyToPeer))
      result <- maybePeerData match {
        case Some(peerData) =>
          logger.debug(
            s"Proxying healthProposal from ${logId(consensusHealthStatus.checkingPeerId)} to ${logId(proxyToPeer)} for roundId ${logRoundId(consensusHealthStatus.roundId)}"
          ) >>
            PeerResponse
              .run(
                apiClient.healthcheck.sendPeerHealthStatus(SendConsensusHealthStatus[K, A, B](consensusHealthStatus)),
                unboundedHealthBlocker
              )(peerData.peerMetadata.toPeerClientMetadata.copy(port = healthHttpPort.toString))
              .handleErrorWith(_ => ProxySendFailed(proxyToPeer).asInstanceOf[SendProposalError].asLeft[Unit].pure[F])
        case None =>
          logger.debug(
            s"Unknown peer=${logId(proxyToPeer)} to proxy healthProposal from ${logId(consensusHealthStatus.checkingPeerId)} to for roundId ${logRoundId(consensusHealthStatus.roundId)}."
          ) >>
            UnknownProxyIdSending(proxyToId = proxyToPeer, proxyingId = ownId).asLeft[Unit].pure[F]
      }
    } yield result

  def proxyGetHealthStatusForRound(
    roundIds: Set[HealthcheckRoundId],
    proxyToPeer: Id,
    originId: Id
  ): F[Either[FetchProposalError, C]] =
    for {
      maybePeerData <- clusterStorage.getPeers.map(_.get(proxyToPeer))
      result <- maybePeerData match {
        case Some(peerData) =>
          logger.debug(
            s"Proxying getting healthProposal from ${logId(proxyToPeer)} for ${logId(originId)} for roundIds ${logRoundIds(roundIds)}"
          ) >>
            PeerResponse
              .run(
                apiClient.healthcheck
                  .fetchPeerHealthStatus[C](FetchPeerHealthStatus(healthCheckType, roundIds, originId)),
                unboundedHealthBlocker
              )(peerData.peerMetadata.toPeerClientMetadata.copy(port = healthHttpPort.toString))
              .handleErrorWith(
                _ =>
                  ProxyGetFailed(proxyToPeer)
                    .asInstanceOf[FetchProposalError]
                    .asLeft[C]
                    .pure[F]
              )
        case None =>
          logger.debug(
            s"Unknown peer=${logId(proxyToPeer)} to proxy getting healthProposal from for ${logId(originId)} for roundIds ${logRoundIds(roundIds)}."
          ) >>
            UnknownProxyIdFetching(proxyToId = proxyToPeer, proxyingId = ownId)
              .asLeft[C]
              .pure[F]
      }
    } yield result

  def getHealthStatusForRound(
    roundIds: Set[HealthcheckRoundId],
    originId: Id
  ): F[Either[FetchProposalError, C]] =
    for {
      allRounds <- consensusRounds.get
      ConsensusRounds(historical, inProgress) = allRounds
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
            .asLeft[C]
            .pure[F]
        case (List(_, _, _*), _) | (_, List(_, _, _*)) =>
          MoreThanOneConsensusForGivenRounds(roundIds)
            .asInstanceOf[FetchProposalError]
            .asLeft[C]
            .pure[F]
        case (List(_, _*), List(_, _*)) =>
          HistoricalAndInProgressRoundMatchedForGivenRounds(roundIds)
            .asInstanceOf[FetchProposalError]
            .asLeft[C]
            .pure[F]
        case (List(consensus, _*), Nil) =>
          consensus
            .getOwnProposalAndMarkAsSent(originId)
            .map(SendConsensusHealthStatus[K, A, B](_).asInstanceOf[C].asRight[FetchProposalError])
        case (Nil, List(historicalRound, _*)) =>
          SendConsensusHealthStatus[K, A, B](historicalRound.ownConsensusHealthStatus)
            .asInstanceOf[C]
            .asRight[FetchProposalError]
            .pure[F]
      }
    } yield result

  private def partitionBasedOnReadyness(
    consensuses: Map[K, HealthCheckConsensus[F, K, A, B, C]]
  ): F[(Map[K, HealthCheckConsensus[F, K, A, B, C]], Map[K, HealthCheckConsensus[F, K, A, B, C]])] =
    for {
      withStatus <- consensuses.toList.traverse {
        case (key, consensus) => consensus.isReadyToCalculateOutcome().map((key, consensus, _))
      }
      (readyStatus, notReadyStatus) = withStatus.partition { case (_, _, isReady) => isReady }
      ready = Map(readyStatus.map { case (key, consensus, _)       => (key, consensus) }: _*)
      notReady = Map(notReadyStatus.map { case (key, consensus, _) => (key, consensus) }: _*)
    } yield (ready, notReady)

  // TODO: Consider if checking peer's PeerData and token isn't a good idea. If something changed the peer
  //       may be removed or updated.
  private def inspectConsensusPeers(consensuses: List[HealthCheckConsensus[F, K, A, B, C]]): F[Unit] =
    for {
      _ <- logger.debug("Started inspection of consensus peers!")
      peers <- clusterStorage.getPeers
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
    consensuses: Map[K, HealthCheckConsensus[F, K, A, B, C]]
  ): F[Map[K, (HealthcheckConsensusDecision[K], HealthCheckConsensus[F, K, A, B, C])]] =
    if (consensuses.isEmpty)
      Map.empty[K, (HealthcheckConsensusDecision[K], HealthCheckConsensus[F, K, A, B, C])].pure[F]
    else
      logger.debug("Started calculation of consensus outcome.") >>
        consensuses.toList.traverse {
          case (key, consensus) =>
            consensus
              .calculateConsensusOutcome()
              .map(key -> (_, consensus))
        }.map(_.toMap)

  def monitorAndManageConsensuses(consensuses: Map[K, HealthCheckConsensus[F, K, A, B, C]]): F[Unit] =
    for {
      partitioned <- partitionBasedOnReadyness(consensuses)
      (ready, notReady) = partitioned
      _ <- if (ready.nonEmpty) logger.debug(s"Ready consensuses ${logHealthCheckKeys(ready.keySet)}") else F.unit
      readyAfterInspection <- if (notReady.nonEmpty) {
        for {
          _ <- logger.debug(s"NotReady consensuses ${logHealthCheckKeys(notReady.keySet)}")
          _ <- inspectConsensusPeers(notReady.values.toList)
          result <- partitionBasedOnReadyness(notReady).map { case (ready, _) => ready }
          _ <- if (result.nonEmpty)
            logger.debug(s"Ready after inspection consensuses ${logHealthCheckKeys(result.keySet)}")
          else F.unit
        } yield result
      } else
        Map.empty[K, HealthCheckConsensus[F, K, A, B, C]].pure[F]
      toManage = notReady -- readyAfterInspection.keySet
      _ <- toManage.values.toList.traverse(c => F.start(c.runManagementTasks()))
      readyToCalculateOutcome = ready ++ readyAfterInspection
      peersWithDecision <- calculateConsensusOutcome(readyToCalculateOutcome)
      _ <- if (peersWithDecision.nonEmpty)
        logger.debug(s"peersWithDecision: ${logHealthCheckKeys(peersWithDecision.keySet)}")
      else F.unit
      _ <- readyConsensusesAction(readyToCalculateOutcome)
      negativeOutcomePeers = peersWithDecision.filter {
        case (_, (decision, _)) => decision.isInstanceOf[NegativeOutcome[K]]
      }
      positiveOutcomePeers = peersWithDecision.filter {
        case (_, (decision, _)) => decision.isInstanceOf[PositiveOutcome[K]]
      }
      _ <- if (negativeOutcomePeers.nonEmpty)
        logger.debug(s"negativeOutcomePeers: ${logHealthCheckKeys(negativeOutcomePeers.keySet)}")
      else F.unit
      _ <- if (positiveOutcomePeers.nonEmpty)
        logger.debug(s"positiveOutcomePeers: ${logHealthCheckKeys(positiveOutcomePeers.keySet)}")
      else F.unit
      _ <- F.start(negativeOutcomeAction(negativeOutcomePeers))
      _ <- F.start(positiveOutcomeAction(positiveOutcomePeers))
      finishedRounds = negativeOutcomePeers ++ positiveOutcomePeers
      _ <- finishedRounds.toList.traverse {
        case (_, (decision, _)) => logHealthcheckConsensusDecision(decision)(metrics, logger)
      }
      finishedWithHistoricalData <- finishedRounds.toList.traverse {
        case (_, (decision, consensus)) =>
          consensus.generateHistoricalRoundData(decision)
      }
      _ <- consensusRounds.modify {
        case ConsensusRounds(historical, inProgress) =>
          val updatedInProgress = inProgress -- finishedWithHistoricalData.map(_.key).toSet
          val updatedHistorical = historical ++ finishedWithHistoricalData

          (ConsensusRounds(updatedHistorical, updatedInProgress), ())
      }
    } yield ()

  def setConsensusesMetrics(consensusRounds: ConsensusRounds[F, K, A, B, C]): F[Unit] =
    for {
      allRounds <- consensusRounds.pure[F]
      ConsensusRounds(historical, inProgress) = allRounds
      historicalOwnCount = historical.count(_.roundType.previousType == OwnRound)
      historicalPeerCount = historical.count(_.roundType.previousType == PeerRound)
      _ <- metrics.updateMetricAsync("inProgressRoundsCount", inProgress.size)
      _ <- metrics.updateMetricAsync("historicalRoundsCount", historical.size)
      _ <- metrics.updateMetricAsync("historicalOwnRoundsCount", historicalOwnCount)
      _ <- metrics.updateMetricAsync("historicalPeerRoundsCount", historicalPeerCount)
    } yield ()

  def triggerMonitorAndManageConsensuses(): F[Unit] =
    for {
      allRounds <- consensusRounds.get
      ConsensusRounds(_, inProgress) = allRounds
      _ <- F.start(setConsensusesMetrics(allRounds))
      _ <- if (inProgress.nonEmpty) monitorAndManageConsensuses(inProgress)
      else periodicOperationWhenNoConsensusesInProgress()
    } yield ()

  def triggerHealthcheckManagement(): F[Unit] =
    for {
      _ <- triggerMonitorAndManageConsensuses()
      _ <- F.start(runPeriodicPeersHealthCheck())
      _ <- periodicOperation()
    } yield ()

  def getPeersUnderConsensus(): F[Set[Id]] =
    getInProgressRounds().map(_.keySet.map(_.id))

  sealed trait ConsensusRoundResponse
  case class RoundStarted(key: K, consensus: HealthCheckConsensus[F, K, A, B, C]) extends ConsensusRoundResponse
  case class RoundForGivenKeyAlreadyTakingPlace(key: K, consensus: HealthCheckConsensus[F, K, A, B, C])
      extends ConsensusRoundResponse
  case class HistoricalRoundForGivenRoundsIdsAlreadyHappened(
    key: K,
    historicalRoundData: HistoricalRoundData[K, A, B]
  ) extends ConsensusRoundResponse

}

object HealthCheckConsensusManagerBase {
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

  case class HistoricalRoundData[K <: HealthCheckKey, A <: HealthCheckStatus, B <: ConsensusHealthStatus[K, A]](
    key: K,
    checkedPeer: CheckedPeer,
    startedAtSecond: FiniteDuration,
    finishedAtSecond: FiniteDuration,
    roundIds: Set[HealthcheckRoundId],
    decision: HealthcheckConsensusDecision[K],
    roundPeers: Map[Id, RoundData[K, A]],
    parallelRounds: Map[K, Set[HealthcheckRoundId]],
    removedPeers: Map[Id, NonEmptyList[PeerRemovalReason]],
    ownConsensusHealthStatus: B,
    roundType: HistoricalRound
  )

  case class ConsensusRounds[
    F[_],
    K <: HealthCheckKey,
    A <: HealthCheckStatus,
    B <: ConsensusHealthStatus[K, A],
    C <: SendConsensusHealthStatus[K, A, B]
  ](
    historical: List[HistoricalRoundData[K, A, B]],
    inProgress: Map[K, HealthCheckConsensus[F, K, A, B, C]]
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

  object EitherCodec {
    implicit def eitherEncoder[A, B](implicit a: Encoder[A], b: Encoder[B]): Encoder[Either[A, B]] =
      Encoder.encodeEither("left", "right")
    implicit def eitherDecoder[A, B](implicit a: Decoder[A], b: Decoder[B]): Decoder[Either[A, B]] =
      Decoder.decodeEither("left", "right")
  }
}
