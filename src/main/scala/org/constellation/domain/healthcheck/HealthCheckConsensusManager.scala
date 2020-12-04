package org.constellation.domain.healthcheck

import cats.effect.{Blocker, Clock, Concurrent, ContextShift, Timer}
import cats.effect.concurrent.Ref
import cats.syntax.all._
import io.chrisdavenport.fuuid.FUUID
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.circe.{Codec, Decoder, Encoder}
import io.circe.generic.semiauto.deriveCodec
import io.circe.syntax.EncoderOps
import org.constellation.domain.healthcheck.HealthCheckConsensus._
import org.constellation.domain.healthcheck.ReconciliationRound._
import org.constellation.domain.p2p.PeerHealthCheck
import org.constellation.domain.p2p.PeerHealthCheck.{PeerHealthCheckStatus, PeerUnresponsive, UnknownPeer}
import org.constellation.infrastructure.p2p.{ClientInterpreter, PeerResponse}
import org.constellation.p2p.{Cluster, PeerData}
import org.constellation.schema.consensus.RoundId
import org.constellation.schema.Id
import org.constellation.schema.NodeState._

import scala.concurrent.duration.{DurationInt, FiniteDuration, SECONDS}
import scala.util.Random

class HealthCheckConsensusManager[F[_]](
  ownId: Id,
  apiClient: ClientInterpreter[F],
  unboundedBlocker: Blocker,
  cluster: Cluster[F],
  peerHealthCheck: PeerHealthCheck[F]
)(implicit F: Concurrent[F], CS: ContextShift[F], C: Clock[F], T: Timer[F]) {
  import HealthCheckConsensusManager._

  val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]
  val healthCheckTimeout: FiniteDuration = 5.seconds
  val maxTimespanWithoutOwnReconciliationRound: FiniteDuration = 6.hours

  private[healthcheck] val consensusRounds: Ref[F, ConsensusRounds[F]] =
    Ref.unsafe(ConsensusRounds(List.empty, Map.empty, Map.empty))
  private[healthcheck] val reconciliationRound: Ref[F, Option[ReconciliationRound[F]]] = Ref.unsafe(None)
  private[healthcheck] val peersToRunHealthcheckFor: Ref[F, Set[Id]] = Ref.unsafe(Set.empty)
  private[healthcheck] val peersToRunConsensusFor: Ref[F, Set[Id]] = Ref.unsafe(Set.empty)
  private[healthcheck] val peersThatNeedReconciliation: Ref[F, Set[Id]] = Ref.unsafe(Set.empty)
  private[healthcheck] val timeOfLastOwnReconciliationRound: Ref[F, Option[Long]] = Ref.unsafe(None)
  private[healthcheck] val runReconciliationRoundAfter: Ref[F, Option[Long]] = Ref.unsafe(None)

  def getTimeInSeconds(): F[Long] = C.monotonic(SECONDS)

  private def getInProgressRounds(): F[Map[Id, HealthCheckConsensus[F]]] = consensusRounds.get.map(_.inProgress)

  private def getKeepUpRounds(): F[Map[Id, HealthCheckConsensus[F]]] = consensusRounds.get.map(_.keepUpRounds)

  private[healthcheck] def createRoundId(): F[RoundId] =
    FUUID.randomFUUID
      .map(fuuid => RoundId(fuuid.toString()))

  private def pickPeersForConsensus(peers: Map[Id, PeerData], peersToExclude: Set[Id]): Map[Id, PeerData] =
    peers.filter {
      case (peerId, peerData) =>
        !peersToExclude.contains(peerId) && canActAsJoiningSource(peerData.peerMetadata.nodeState)
    }

  def startOwnConsensusForId(checkedId: Id): F[ConsensusRoundResponse] =
    for {
      roundId <- createRoundId()
      allPeers <- cluster.getPeerInfo
      startedAt <- getTimeInSeconds()
      result <- consensusRounds.modify {
        case ConsensusRounds(historical, inProgress, keepUpRounds) =>
          lazy val roundPeers = pickPeersForConsensus(allPeers, /*inProgress.keySet +*/ Set(checkedId))
          lazy val consensusRound =
            HealthCheckConsensus[F](
              checkedId,
              ownId,
              roundId,
              roundPeers,
              PeerUnresponsive(checkedId),
              startedAt,
              isKeepUpRound = false
            )(
              apiClient,
              unboundedBlocker
            )
          val parallelRounds = (inProgress ++ keepUpRounds).filterKeys(_ != checkedId)
          val roundForId = (keepUpRounds ++ inProgress).get(checkedId) // should we allow starting own round for id if there is a keepUpRound for that id taking place?

          val (updatedInProgress, result): (
            Map[Id, HealthCheckConsensus[F]],
            (ConsensusRoundResponse, HealthCheckConsensus[F], Map[Id, HealthCheckConsensus[F]])
          ) = roundForId match {
            case Some(existingRound) =>
              (inProgress, (RoundForGivenIdAlreadyTakingPlace(checkedId), existingRound, parallelRounds))
            case None =>
              (inProgress + (checkedId -> consensusRound), (RoundStarted(roundId), consensusRound, parallelRounds))
          }

          (ConsensusRounds(historical, updatedInProgress, keepUpRounds), result) // mabe we shouldn't separate keep up rounds from in progress, though we should remove keep up round for id if we get a proposal for a new consensus for that id
      }
      (response, consensusRound, parallelRounds) = result
      _ <- {
        response match {
          case RoundStarted(_) =>
            logger.debug(
              s"Started own round for id=${checkedId.medium} with roundId=$roundId and peers=${allPeers.keySet
                .map(_.medium)} at $startedAt!"
            ) >>
              parallelRounds.toList.traverse {
                case (id, consensus) =>
                  consensus.getRoundIds() >>= { roundIds =>
                    consensusRound.addParallelRounds(id, roundIds.toSortedSet)
                  }
              } >>
              logger.debug(s"Added parallel rounds to own roundId=$roundId") >>
              consensusRound.start().map(_ => ()) >> //is that correct? will the fiber still run in the background?
              logger.debug(s"Triggered start on own consensus round with roundId=$roundId") >>
              getInProgressRounds() >>= (
              _.toList.filter(_._1 != checkedId).traverse {
                case (_, consensus) =>
                  consensus.addParallelRounds(checkedId, Set(roundId))
              }
            ) //logger.debug....
          case RoundForGivenIdAlreadyTakingPlace(id) =>
            logger.debug(s"Round for given id=${id.medium} is already taking place")
        }
      }
    } yield response

  def participateInNewPeerConsensus(healthProposal: ConsensusHealthStatus): F[Unit] =
    for {
      allPeers <- cluster.getPeerInfo
      ConsensusHealthStatus(checkedId, _, roundId, _, _) = healthProposal
      checkedPeer = allPeers.get(checkedId)
      // if we don't have such peer in our list of peers we say that it's unresponsive from our point of view, to let the network converge
      // we can't add missing peers currently
      healthCheckStatus <- checkedPeer
        .fold(UnknownPeer(checkedId).asInstanceOf[PeerHealthCheckStatus].pure[F])(
          pd => peerHealthCheck.checkPeer(pd.peerMetadata.toPeerClientMetadata, healthCheckTimeout)
        )
      startedAt <- getTimeInSeconds()
      result <- consensusRounds.modify {
        case ConsensusRounds(historical, inProgress, keepUpRounds) => // are you sure it will always work - it impacts whether we can respond to a peer on an endpoint that we processed the proposal before actually processing it see: HealthCheckEndpoints
          // should we get roundPeers always, or only if there is no other consensus in progress currently - for this case take round peers from that consensus???
          // for sure we need to filter out the peer that is the subject of other concurrently running consensuses
          // UPDATE: though we are not sure if they will be removed or not so I would rather add them, try to send/receive and if after some timeout they are still unresponsive
          // we then remove the unresponsive peers with parallel rounds, we maximize our chance of treating online peers as online and offline as offline
          // for many concurrent rounds the amount of deciding peers would decrease significantly
          lazy val roundPeers = pickPeersForConsensus(allPeers, /*inProgress.keySet +*/ Set(checkedId))
          lazy val consensusRound =
            HealthCheckConsensus(
              checkedId,
              ownId,
              roundId,
              roundPeers,
              healthCheckStatus,
              startedAt,
              isKeepUpRound = false
            )(
              apiClient,
              unboundedBlocker
            )
          val parallelRounds = (inProgress ++ keepUpRounds).filterKeys(_ != checkedId) // shouldn't happen? //should I add keepUpRounds as parallel ones???
          val roundForId = inProgress.get(checkedId)
          val (updatedInProgress, result): (
            Map[Id, HealthCheckConsensus[F]],
            (ConsensusRoundResponse, HealthCheckConsensus[F], Map[Id, HealthCheckConsensus[F]])
          ) = roundForId match {
            case Some(existingRound) =>
              (inProgress, (RoundForGivenIdAlreadyTakingPlace(checkedId), existingRound, parallelRounds))
            case None =>
              (inProgress + (checkedId -> consensusRound), (RoundStarted(roundId), consensusRound, parallelRounds))
          }

          (ConsensusRounds(historical, updatedInProgress, keepUpRounds - checkedId), result)
      }
      (response, consensusRound, parallelRounds) = result
      _ <- response match {
        case RoundStarted(_) =>
          logger.debug(
            s"Joined new round for id=${checkedId.medium} with status=$healthCheckStatus with roundId=$roundId and peers=${allPeers.keySet
              .map(_.medium)} at $startedAt!"
          ) >>
            parallelRounds.toList.traverse {
              case (id, consensus) =>
                consensus.getRoundIds() >>= (roundIds => consensusRound.addParallelRounds(id, roundIds.toSortedSet))
            } >>
            logger.debug(s"Added parallel rounds to joined roundId=$roundId") >>
            consensusRound.start().map(_ => ()) >> //is that correct? will the fiber still run in the background?
            logger.debug(s"Triggered start on joined consensus round with roundId=$roundId") >>
            getInProgressRounds() >>= (
            _.toList.filter(_._1 != checkedId).traverse {
              case (_, consensus) =>
                consensus
                  .addParallelRounds(checkedId, Set(roundId)) //should we add also if we got all the proposals already? also should we remove the peer if we got proposals and other stuff from that peer?
            }
          )
        case RoundForGivenIdAlreadyTakingPlace(id) =>
          logger.debug(s"Round for given id=${id.medium} is already taking place") // >> F.unit
      }
      _ <- handleConsensusHealthProposal(healthProposal)
    } yield ()

  def startKeepUpRound(notification: NotifyAboutMissedConsensus): F[Unit] =
    for {
      allPeers <- cluster.getPeerInfo
      checkedId = notification.checkedId
      roundIds = notification.roundIds
      checkedPeer = allPeers.get(checkedId)
      // if we don't have such peer in our list of peers we say that it's unresponsive from our point of view, to let the network converge
      // we can't add missing peers currently
      healthCheckStatus <- checkedPeer
        .fold(UnknownPeer(notification.checkedId).asInstanceOf[PeerHealthCheckStatus].pure[F])(
          pd => peerHealthCheck.checkPeer(pd.peerMetadata.toPeerClientMetadata, healthCheckTimeout)
        )
      startedAt <- getTimeInSeconds()
      result <- consensusRounds.modify {
        case ConsensusRounds(historical, inProgress, keepUpRounds) =>
          lazy val roundPeers = pickPeersForConsensus(allPeers, /*inProgress.keySet +*/ Set(notification.checkedId))
          lazy val consensusRound =
            HealthCheckConsensus(
              notification.checkedId,
              ownId,
              notification.consensusHealthStatus.roundId,
              roundPeers,
              healthCheckStatus,
              startedAt,
              isKeepUpRound = true
            )(
              apiClient,
              unboundedBlocker
            )
          lazy val parallelRounds = (inProgress ++ keepUpRounds).filterKeys(_ != checkedId) //should I add keepUpRounds as parallel ones???

          val isRoundInProgressForId = inProgress.keySet.contains(checkedId)
          val isKeepUpRoundForId = keepUpRounds.keySet.contains(checkedId)
          val isHistoricalRoundForRoundIds = historical.exists(_.roundIds.intersect(notification.roundIds).nonEmpty)

          val (updatedKeepUpRounds, result): (
            Map[Id, HealthCheckConsensus[F]],
              (KeepUpRoundResponse, Option[HealthCheckConsensus[F]], Map[Id, HealthCheckConsensus[F]])
            ) =
            if (isRoundInProgressForId)
              (keepUpRounds, (RoundForGivenIdIsInProgress(checkedId), None, Map.empty))
            else if (isHistoricalRoundForRoundIds)
              (keepUpRounds, (HistoricalRoundForGivenRoundsIdsAlreadyHappened(notification.checkedId), None, Map.empty))
            else if (isKeepUpRoundForId)
              (keepUpRounds, (KeepUpRoundForGivenIdAlreadyTakingPlace(checkedId), keepUpRounds.get(checkedId), Map.empty))
            else
              (keepUpRounds + (checkedId -> consensusRound), (KeepUpRoundStarted(roundIds), consensusRound.some, parallelRounds))

          (ConsensusRounds(historical, inProgress, updatedKeepUpRounds), result)
      }
      (response, consensusRound, parallelRounds) = result
      _ <- (response, consensusRound) match {
        case (KeepUpRoundStarted(roundIds), Some(consensus)) =>
          logger.debug(s"Started keepUpRound for roundIds=$roundIds") >>
            parallelRounds.toList.traverse { case(id, consensus) =>
              consensus.getRoundIds() >>= (roundIds => consensus.addParallelRounds(id, roundIds.toSortedSet))
            } >>
            logger.debug(s"Added parallel rounds to keepUpRound with roundIds=$roundIds") >>
            F.start(consensus.runManagementTasks()).map(_ => ()) >>
            logger.debug(s"Triggered runManagementTaks on keepUpRound with roundIds=$roundIds") >>
            // should I add keepUpRound to parallel inProgress rounds also???
            getInProgressRounds() >>= (
            _.toList.filter(_._1 != checkedId).traverse {
              case (_, consensus) =>
                consensus.addParallelRounds(checkedId, roundIds)
            }
          )
        case (KeepUpRoundForGivenIdAlreadyTakingPlace(id), Some(consensus)) =>
          logger.debug(s"KeepUpRound for $id already taking place.") >>
          consensus.getRoundIds() >>=
            (ids => if (ids.toSortedSet.intersect(roundIds).nonEmpty) consensus.addRoundIds(roundIds) else F.unit)
        case (RoundForGivenIdIsInProgress(id), None) =>
          logger.debug(s"Round for given id=$id while processing keepUpRound for roundIds=$roundIds turned out to be in progress.")
        case (HistoricalRoundForGivenRoundsIdsAlreadyHappened(id), None) =>
          logger.debug(s"Round for given keepUpRound with roundIds=$roundIds already happened and is Historical!")
        case _ =>
          logger.warn(s"Unexpected case when processing keepUpRound for roundIds=$roundIds response=$response consensus=$consensusRound parallelRounds=$parallelRounds")
      }
    } yield ()

  def handleConsensusHealthProposal(healthProposal: ConsensusHealthStatus): F[Unit] =
    for {
      rounds <- getInProgressRounds()
      roundForId = rounds.get(healthProposal.checkedId)
      _ <- logger.debug(
        s"Received proposal: checkedId=${healthProposal.checkedId.short} checkingId=${healthProposal.checkingPeerId.short} roundId=${healthProposal.roundId} status=${healthProposal.status} clusterState=${healthProposal.clusterState.map {
          case (id, state) => id.short -> state
        }}"
      )
      _ <- roundForId match {
        case Some(round) =>
          logger.debug(s"Round exists, processing proposal!") >>
            round.processProposal(healthProposal)
        case None =>
          logger.debug(s"Round deesn't exist participating in new consensus round!") >>
            participateInNewPeerConsensus(healthProposal)
      }
    } yield ()

  def handleNotificationAboutMissedConsensus(notification: NotifyAboutMissedConsensus): F[Unit] =
    for {
      allRounds <- consensusRounds.get
      ConsensusRounds(historical, inProgress, keepUpRounds) = allRounds
      keepUpRoundsWithMathchingRoundIds <-
        keepUpRounds.toList.traverse { case (id, consensus) => consensus.getRoundIds().map(id -> (_, consensus)) }
          .map(_.filter { case (_, (roundIds, _)) => roundIds.toSortedSet.intersect(notification.roundIds).nonEmpty })
          .map(_.toMap)

      // should I only do these checks in startKeepUpRound?
      isRoundInProgressForId = inProgress.keySet.contains(notification.checkedId)
      isKeepUpRoundForId = keepUpRounds.keySet.contains(notification.checkedId)
      isKeepUpRoundForRoundIds = keepUpRoundsWithMathchingRoundIds.nonEmpty
      isHistoricalRoundForRoundIds = historical.exists(_.roundIds.intersect(notification.roundIds).nonEmpty)

      _ <- if (isKeepUpRoundForRoundIds)
        keepUpRoundsWithMathchingRoundIds.values.toList.traverse { case(_, consensus) =>
          consensus.addRoundIds(notification.roundIds)
        }
      else F.unit
      _ <-
        if (isRoundInProgressForId || isKeepUpRoundForRoundIds || isHistoricalRoundForRoundIds || isKeepUpRoundForId)
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
      ownNodeReconciliationData = NodeReconciliationData(ownId, ownState, none[Set[RoundId]], ownJoinedHeight)
      clusterState <- cluster.getPeerInfo
        .map(
          _.map {
            case (id, peerData) =>
              id -> NodeReconciliationData(
                id,
                peerData.peerMetadata.nodeState,
                rounds.get(id),
                peerData.majorityHeight.head.joined
              )
          } + (ownId -> ownNodeReconciliationData)
        )
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
          logger.debug(s"Proxying healthProposal from ${consensusHealthStatus.checkingPeerId.medium} to $proxyToPeer") >>
            PeerResponse
              .run(
                apiClient.healthcheck.sendPeerHealthStatus(SendPerceivedHealthStatus(consensusHealthStatus)),
                unboundedBlocker
              )(peerData.peerMetadata.toPeerClientMetadata.copy(port = "9003"))
              .handleErrorWith(_ => ProxySendFailed(proxyToPeer).asInstanceOf[SendProposalError].asLeft[Unit].pure[F])
        case None =>
          logger.debug(s"Unknown peer=${proxyToPeer.medium} to proxy healthProposal to.") >>
            UnknownProxyIdSending(proxyToPeer).asLeft[Unit].pure[F]
      }
    } yield result

  def proxyGetHealthStatusForRound(
    roundIds: Set[RoundId],
    proxyToPeer: Id
  ): F[Either[FetchProposalError, SendPerceivedHealthStatus]] =
    for {
      maybePeerData <- cluster.getPeerInfo.map(_.get(proxyToPeer))
      result <- maybePeerData match {
        case Some(peerData) =>
          logger.debug(s"Proxying getting healthProposal from $proxyToPeer") >>
            PeerResponse.run(
              apiClient.healthcheck.fetchPeerHealthStatus(FetchPeerHealthStatus(roundIds)),
              unboundedBlocker
            )(peerData.peerMetadata.toPeerClientMetadata.copy(port = "9003"))
              .handleErrorWith(_ => ProxyGetFailed(proxyToPeer).asInstanceOf[FetchProposalError].asLeft[SendPerceivedHealthStatus].pure[F])
        case None =>
          logger.debug(s"Unknown peer=$proxyToPeer to proxy getting healthProposal from.") >> UnknownProxyIdFetching(
            proxyToPeer,
            ownId
          ).asLeft[SendPerceivedHealthStatus].pure[F]
      }
    } yield result

  def getHealthStatusForRound(roundIds: Set[RoundId]): F[Either[FetchProposalError, SendPerceivedHealthStatus]] =
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
          consensus.getOwnPerceivedHealthStatus().map(SendPerceivedHealthStatus(_).asRight[FetchProposalError])
        case (Nil, historicalRound :: Nil) =>
          F.delay(SendPerceivedHealthStatus(historicalRound.ownConsensusHealthStatus).asRight[FetchProposalError])
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

  private def inspectConsensusPeers(consensuses: List[HealthCheckConsensus[F]]): F[Unit] =
    for {
      _ <- logger.debug("inspectConsensusPeers STARTED")
      peers <- cluster.getPeerInfo
      _ <- consensuses.traverse { consensus =>
        for {
          roundPeers <- consensus.getRoundPeers()
          leavingOrOfflinePeers = peers.filter {
            case (_, peerData) => isInvalidForJoining(peerData.peerMetadata.nodeState)
          }.keySet
          absentPeers = (roundPeers.keySet -- peers.keySet) ++ roundPeers.keySet.intersect(leavingOrOfflinePeers)
          _ <- consensus.manageAbsentPeers(absentPeers)
          proxiedPeers = roundPeers.filter { case (_, roundData) => roundData.peerData.isLeft }
          peersThanNoLongerNeedProxying = peers.filter {
            case (id, peerData) =>
              canActAsJoiningSource(peerData.peerMetadata.nodeState) && proxiedPeers.keySet.contains(id)
          }
          _ <- consensus.manageProxiedPeers(peersThanNoLongerNeedProxying)
        } yield ()
      }
    } yield ()

  private def calculateConsensusOutcome(
    consensuses: Map[Id, HealthCheckConsensus[F]]
  ): F[Map[Id, (Option[HealthcheckConsensusDecision], HealthCheckConsensus[F])]] =
    logger.debug("calculateConsensusOutcome STARTED") >>
      consensuses.toList.traverse {
        case (id, consensus) =>
          consensus
            .calculateConsensusOutcome()
            .map(id -> (_, consensus))
      }.map(_.toMap)

  private def fetchAbsentPeers(consensuses: Map[Id, HealthCheckConsensus[F]]): F[Set[Id]] =
    logger.debug("fetchAbsentPeers STARTED") >>
      consensuses.toList.traverse {
        case (id, consensus) =>
          consensus
            .getRoundPeers()
            .map(_.filter { case (_, roundData) => roundData.peerData.isLeft }.keySet)
      }.map(_.flatten.toSet)

  private def findNewPeers(currentPeers: Map[Id, PeerData], consensuses: Map[Id, HealthCheckConsensus[F]]) =
    consensuses.toList.traverse {
      case (id, consensus) =>
        consensus
          .getRoundPeers()
          .map(_.keySet)
    }.map(_.flatten.toSet)
      .map(currentPeers.keySet -- _)

//  def notifyRemainingPeersAboutMissedConsensuses(remainingPeers: Set[Id]) =
//    for {
//      peers <- cluster.getPeerInfo
//      peersWeHave = peers -- peers.keySet.diff(remainingPeers)
//      inProgress <- getInProgressRounds()
//      missedInProgress <- inProgress.values.toList.traverse { consensus =>
//        for {
//          removed <- consensus.getRemovedUnresponsivePeersWithParallelRound()
//          notification <- consensus.generateMissedConsensusNotification()
//        } yield (removed, notification)
//      }
//      peersWithMissedConsensus = peersWeHave.map { case(id, peerData) => (peerData, missedInProgress.filter { case (removed, _) => removed.contains(id) }.map(_._2))}
//      foo <- peersWithMissedConsensus.toList.map {
//        case(peerData, notifications) =>
//          notifications.traverse { notification =>
//            PeerResponse.run(
//              apiClient.healthcheck.notifyAboutMissedConsensus(notification),
//              unboundedBlocker
//            )(peerData.peerMetadata.toPeerClientMetadata)
//          }
//      }
//    } yield ()

  // Maybe we should keep consensuses in progress until all our peers confirm that they made a decision?
  // Only after that we are moving the consensus to historical ones.
  // And of course after our decision is made we will be still removing the peers that stopped responding, so we shouldn't keep the consensus forever.
  // With HistoricalRound class there is no need to wait I think
  // Another thing I would add is to respond with failed if someone sends as the proposal for ID that they already sent proposal for - that means that either someone isn't aware that we received the proposal, or it already processed the round for this ID and started a new one

  // We need:
  // logic for adding nodes we don't have but should have, or decision that we shouldn't add, but then maybe the node should leave the cluster
  // logic for fetching peers that we didn't have and we need to run new consensus for - with check that we still don't have those
  // logic for checking if any new peers appeared in the meantime, if yes we need to run reconciliation to check if they aligned with the cluster
  def monitorAndManageConsensuses(consensuses: Map[Id, HealthCheckConsensus[F]]): F[Unit] =
    for {
      peers <- cluster.getPeerInfo
      partitioned <- partitionBasedOnReadyness(consensuses)
      (ready, notReady) = partitioned
      _ <- logger.debug(s"Ready consensuses ${ready.keySet.map(_.medium)}")
      _ <- logger.debug(s"NotReady consensuses ${notReady.keySet.map(_.medium)}")
      _ <- inspectConsensusPeers(notReady.values.toList)
      readyAfterInspect <- partitionBasedOnReadyness(notReady).map { case (ready, _) => ready }
      toManage = notReady -- readyAfterInspect.keySet
      _ <- F.start(toManage.values.toList.traverse(_.runManagementTasks())) // F.start(...)???
      peersWithDecision <- calculateConsensusOutcome(ready ++ readyAfterInspect) // I guess it shouldn't return optional value, if it's ready the decision should be made
      _ <- logger.debug(s"peersWithDecision: ${peersWithDecision.keySet.map(_.medium)}")
      peersWeDidntHave <- fetchAbsentPeers(ready ++ readyAfterInspect)
      _ <- if (peersWeDidntHave.nonEmpty) logger.debug(s"peersWeDidntHave: $peersWeDidntHave") else F.unit
      peersWeStillDontHave = peersWeDidntHave -- peers.keySet //should I check the status if it's not offline/leaving???
      _ <- if (peersWeStillDontHave.nonEmpty) logger.debug(s"peersWeStillDontHave: $peersWeStillDontHave") else F.unit
      newPeers <- findNewPeers(peers, consensuses)
      _ <- if (newPeers.nonEmpty) logger.debug(s"newPeers: ${newPeers.map(_.medium)}") else F.unit
      toRemove = peersWithDecision.filter {
        case (id, (maybeDecision, _)) => maybeDecision.exists(_.isInstanceOf[PeerOffline]) /*maybeDecision.contains(PeerOffline(id, _, _, _, _, _))*/
      }
      _ <- logger.debug(s"toRemove: ${toRemove.keySet.map(_.medium)}")
      toRemain = peersWithDecision.filter {
        case (id, (maybeDecision, _)) => maybeDecision.exists(_.isInstanceOf[PeerOnline]) /*maybeDecision.contains(PeerOnline(id, _, _, _, _, _))*/
      }
      _ <- logger.debug(s"toRemain: ${toRemain.keySet.map(_.medium)}")
      unknown = peersWithDecision.filter {
        case (id, (maybeDecision, _)) => maybeDecision.isEmpty /*maybeDecision.contains(PeerOnline(id, _, _, _, _, _))*/
      }
      _ <- logger.debug(s"unknown: ${unknown.keySet.map(_.medium)}")
      _ <- peerHealthCheck.markOffline(peers.filterKeys(toRemove.keySet.contains).values.toList) // mark offline those peers that we do have in our list
      // if toRemain peer is not on our list we need to add it
      toRemainNotPresentPeers = toRemain.keySet -- peers.keySet //should I check the status???
      _ <- if (toRemainNotPresentPeers.nonEmpty) logger.warn(s"Peers that we don't have but we should: ${toRemainNotPresentPeers.map(_.medium)}") else F.unit
      _ <- peersToRunConsensusFor.modify(peers => (peers ++ peersWeStillDontHave, ()))
      _ <- peersThatNeedReconciliation.modify(peers => (peers ++ newPeers, ()))
      //_ <- notifyRemainingPeersAboutMissedConsensuses(toRemain)
      finishedRounds = toRemove ++ toRemain
      _ <- finishedRounds.toList.traverse {
        case (id, (decision, _)) =>
          decision match {
            case Some(PeerOffline(id, allPeers, remainingPeers, percentage, parallelRounds, roundIds, isKeepUpRound)) =>
              logger.debug(
                s"HealtcheckConsensus decision for id=${id.medium} is: PeerOffline allPeers=${allPeers
                  .map(_.medium)} remainingPeers=${remainingPeers.map(_.medium)} percentage=$percentage parallelRounds=${parallelRounds
                  .map(r => r._1.medium -> r._2)} roundIds=$roundIds isKeepUpRound=$isKeepUpRound"
              )
            case Some(PeerOnline(id, allPeers, remainingPeers, percentage, parallelRounds, roundIds, isKeepUpRound)) =>
              logger.debug(
                s"HealtcheckConsensus decision for id=${id.medium} is: PeerOnline allPeers=${allPeers
                  .map(_.medium)} remainingPeers=${remainingPeers.map(_.medium)} percentage=$percentage parallelRounds=${parallelRounds
                  .map(r => r._1.medium -> r._2)} roundIds=$roundIds isKeepUpRound=$isKeepUpRound"
              )
            case None => logger.debug(s"HealthcheckConsensus decision for id=${id.medium} COULDN'T BE CALCULATED")
          }
      }
      finishedWithHistoricalData <- finishedRounds.toList.traverse {
        case (_, (decision, consensus)) =>
          consensus.generateHistoricalRoundData(decision)
      }
      _ <- consensusRounds.modify {
        case ConsensusRounds(historical, inProgress, keepUpRounds) =>
          val updatedInProgress = inProgress -- finishedWithHistoricalData.map(_.checkedId).toSet
          val updatedHistorical = historical ++ finishedWithHistoricalData

          (ConsensusRounds(updatedHistorical, updatedInProgress, keepUpRounds), ())
      }
    } yield ()

  def manageReconciliationRound(round: ReconciliationRound[F]): F[Unit] =
    for {
      result <- round.getReconciliationResult()
      _ <- result match {
        case Some(ReconciliationResult(peersToRunHealthCheckFor, misalignedPeers, clusterAlignment)) =>
          for {
            _ <- peersToRunHealthcheckFor.modify(current => (current ++ peersToRunHealthCheckFor, ()))
            _ <- peersToRunConsensusFor.modify(current => (current ++ misalignedPeers, ()))
            _ <- reconciliationRound.modify(_ => (None, ()))
            _ <- logger.debug(s"Own reconciliation round result: peersToRunHealthcheckFor: ${peersToRunHealthCheckFor.map(_.medium)} misalignedPeers: ${misalignedPeers.map(_.medium)}, clusterAlignment: ${clusterAlignment.map(_.toList.map{ case (id, result) => id.short -> result })}")
          } yield ()
        case None => F.unit
      }
    } yield ()

  // Schedule between 1 and 10 minutes from now
  def scheduleSoonEpoch(): F[Long] =
    for {
      currentEpoch <- getTimeInSeconds()
      delayTime <- F.delay(Random.nextInt(540))
      result = currentEpoch + 60 + delayTime
    } yield result

  // Schedule between 10 and 60 minutes from now
  def scheduleNormalEpoch(): F[Long] =
    for {
      currentEpoch <- getTimeInSeconds()
      delayTime <- F.delay(Random.nextInt(3000))
      result = currentEpoch + 600 + delayTime
    } yield result

  def manageSchedulingReconciliation(): F[Unit] =
    for {
      currentEpoch <- getTimeInSeconds()
      peersThatNeedReconciliation <- peersThatNeedReconciliation.get
      runAfter <- runReconciliationRoundAfter.get
      _ <- runAfter match {
        case Some(roundEpoch) if roundEpoch < currentEpoch =>
          for {
            round <- reconciliationRound.modify { _ =>
              val round = new ReconciliationRound[F](ownId, cluster, currentEpoch)(apiClient, unboundedBlocker)
              (round.some, round)
            }
            _ <- round.start().map(_ => ())
          } yield ()
        case Some(_) => F.unit
        case None =>
          for {
            scheduleTime <- if (peersThatNeedReconciliation.nonEmpty) scheduleSoonEpoch() else scheduleNormalEpoch()
            _ <- runReconciliationRoundAfter.modify(_ => (scheduleTime.some, ()))
          } yield ()
      }
    } yield ()

  def manageSchedulingReconciliationAfterSendingClusterState(): F[Unit] =
    for {
      currentEpoch <- getTimeInSeconds()
      _ <- peersThatNeedReconciliation.modify(_ => (Set.empty, ()))
      lastOwnReconciliation <- timeOfLastOwnReconciliationRound.get
      shouldCancel = lastOwnReconciliation.map(_ + maxTimespanWithoutOwnReconciliationRound.toSeconds).exists(_ > currentEpoch)
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
        case None => manageSchedulingReconciliation()
      }
    } yield ()

  def triggerMonitorAndManageConsensuses(): F[Unit] =
    for {
      inProgress <- getInProgressRounds()
      keepUp <- getKeepUpRounds()
      consensuses = keepUp ++ inProgress
      _ <- if (consensuses.nonEmpty) monitorAndManageConsensuses(consensuses) else manageReconciliation()
    } yield ()

  def checkPeersHealth(): F[Unit] =
    for {
      _ <- triggerMonitorAndManageConsensuses() //monitorAndManageConsensuses()
      peersUnderConsensus <- getPeersUnderConsensus
      unresponsivePeers <- peerHealthCheck.check(peersUnderConsensus)
      _ <- unresponsivePeers.traverse(pd => startOwnConsensusForId(pd.peerMetadata.id))
    } yield ()

  def areThereConsensusRoundsInProgress: F[Boolean] =
    getInProgressRounds().map(_.nonEmpty)

  def getPeersUnderConsensus: F[Set[Id]] =
    getInProgressRounds().map(_.keySet)
}

object HealthCheckConsensusManager {
  //Response
  sealed trait ConsensusRoundResponse
  case class RoundStarted(roundId: RoundId) extends ConsensusRoundResponse
  case class RoundForGivenIdAlreadyTakingPlace(id: Id) extends ConsensusRoundResponse

  sealed trait KeepUpRoundResponse
  case class KeepUpRoundStarted(roundIds: Set[RoundId]) extends KeepUpRoundResponse
  case class KeepUpRoundForGivenIdAlreadyTakingPlace(id: Id) extends KeepUpRoundResponse
  case class RoundForGivenIdIsInProgress(id: Id) extends KeepUpRoundResponse
  case class HistoricalRoundForGivenRoundsIdsAlreadyHappened(id: Id) extends KeepUpRoundResponse

  //Error
  sealed trait HealthCheckManagerError extends Exception
  sealed trait FetchProposalError extends HealthCheckManagerError

  object FetchProposalError {
    implicit val fetchProposalErrorEncoder: Encoder[FetchProposalError] = Encoder.instance {
      case a @ NoConsensusForGivenRounds(_)                         => a.asJson
      case a @ MoreThanOneConsensusForGivenRounds(_)                => a.asJson
      case a @ HistoricalAndInProgressRoundMatchedForGivenRounds(_) => a.asJson
      case a @ UnknownProxyIdFetching(_, _)                         => a.asJson
      case a @ ProxyGetFailed(_)                                    => a.asJson
    }

    implicit val fetchProposalErrorDecoder: Decoder[FetchProposalError] =
      List[Decoder[FetchProposalError]](
        Decoder[NoConsensusForGivenRounds].widen,
        Decoder[MoreThanOneConsensusForGivenRounds].widen,
        Decoder[HistoricalAndInProgressRoundMatchedForGivenRounds].widen,
        Decoder[UnknownProxyIdFetching].widen,
        Decoder[ProxyGetFailed].widen
      ).reduceLeft(_.or(_))
  }
  case class NoConsensusForGivenRounds(roundIds: Set[RoundId]) extends FetchProposalError

  object NoConsensusForGivenRounds {
    implicit val noConsensusForGivenRoundsCodec: Codec[NoConsensusForGivenRounds] = deriveCodec
  }
  case class MoreThanOneConsensusForGivenRounds(roundIds: Set[RoundId]) extends FetchProposalError

  object MoreThanOneConsensusForGivenRounds {
    implicit val moreThanOneConsensusForGivenRoundsCodec: Codec[MoreThanOneConsensusForGivenRounds] = deriveCodec
  }

  case class HistoricalAndInProgressRoundMatchedForGivenRounds(roundIds: Set[RoundId]) extends FetchProposalError

  object HistoricalAndInProgressRoundMatchedForGivenRounds {
    implicit val historicalAndInProgressRoundMatchedForGivenRoundsCodec
      : Codec[HistoricalAndInProgressRoundMatchedForGivenRounds] = deriveCodec
  }

  case class UnknownProxyIdFetching(proxyToId: Id, proxyingId: Id) extends FetchProposalError

  object UnknownProxyIdFetching {
    implicit val unknownProxyId: Codec[UnknownProxyIdFetching] = deriveCodec
  }

  case class ProxyGetFailed(id: Id) extends FetchProposalError
  object ProxyGetFailed {
    implicit val proxyGetFailedCodec: Codec[ProxyGetFailed] = deriveCodec
  }

  sealed trait SendProposalError extends HealthCheckManagerError

  object SendProposalError {
    implicit val sendProposalErrorEncoder: Encoder[SendProposalError] = Encoder.instance {
      case a @ UnknownProxyIdSending(_) => a.asJson
      case a @ ProxySendFailed(_)       => a.asJson
    }

    implicit val sendProposalErrorDecoder: Decoder[SendProposalError] =
      List[Decoder[SendProposalError]](
        Decoder[UnknownProxyIdSending].widen,
        Decoder[ProxySendFailed].widen
      ).reduceLeft(_.or(_))
  }
  case class UnknownProxyIdSending(id: Id) extends SendProposalError

  object UnknownProxyIdSending {
    implicit val unknownProxyIdSendingCodec: Codec[UnknownProxyIdSending] = deriveCodec
  }

  case class ProxySendFailed(id: Id) extends SendProposalError

  object ProxySendFailed {
    implicit val proxySendFailedCodec: Codec[ProxySendFailed] = deriveCodec
  }

  case class HistoricalRound(
    checkedId: Id,
    startedAtSecond: Long,
    finishedAtSecond: Long,
    roundIds: Set[RoundId],
    decision: Option[HealthcheckConsensusDecision],
    roundPeers: Map[Id, RoundData],
    parallelRounds: Map[Id, Set[RoundId]],
    leftDuringConsensus: Set[Id],
    removedUnresponsivePeersWithParallelRound: Set[Id],
    ownConsensusHealthStatus: ConsensusHealthStatus,
    isKeepUpRound: Boolean
  )

  case class ConsensusRounds[F[_]](
    historical: List[HistoricalRound],
    inProgress: Map[Id, HealthCheckConsensus[F]],
    keepUpRounds: Map[Id, HealthCheckConsensus[F]]
  )
}
