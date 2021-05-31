package org.constellation.domain.healthcheck.ping

import cats.Parallel
import cats.data.NonEmptyList
import cats.effect.concurrent.Ref
import cats.effect.{Blocker, Clock, Concurrent, ContextShift, Fiber, Timer}
import cats.syntax.all._
import org.constellation.domain.cluster.{ClusterStorageAlgebra, NodeStorageAlgebra}
import org.constellation.domain.healthcheck.HealthCheckConsensus.{
  HealthcheckConsensusDecision,
  PingHealthStatus,
  SendConsensusHealthStatus
}
import org.constellation.domain.healthcheck.HealthCheckConsensusManagerBase.ConsensusRounds
import org.constellation.domain.healthcheck.HealthCheckKey.PingHealthCheckKey
import org.constellation.domain.healthcheck.HealthCheckLoggingHelper.{
  logId,
  logIds,
  logReconciliationResult,
  logRoundIds
}
import org.constellation.domain.healthcheck.HealthCheckStatus.{PeerPingHealthCheckStatus, PeerUnresponsive, UnknownPeer}
import org.constellation.domain.healthcheck.HealthCheckType.PingHealthCheck
import org.constellation.domain.healthcheck._
import org.constellation.domain.healthcheck.ping.ReconciliationRound.{
  ClusterState,
  NodeReconciliationData,
  ReconciliationResult
}
import org.constellation.infrastructure.p2p.ClientInterpreter
import org.constellation.infrastructure.p2p.PeerResponse.PeerClientMetadata
import org.constellation.p2p.{Cluster, PeerData}
import org.constellation.schema.Id
import org.constellation.schema.NodeState.canActAsJoiningSource
import org.constellation.util.Metrics

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.Random

class PingHealthCheckConsensusManager[F[_]](
  ownId: Id,
  cluster: Cluster[F],
  clusterStorage: ClusterStorageAlgebra[F],
  nodeStorage: NodeStorageAlgebra[F],
  peerHealthCheck: PeerHealthCheck[F],
  metrics: PrefixedHealthCheckMetrics[F],
  apiClient: ClientInterpreter[F],
  unboundedHealthBlocker: Blocker,
  healthHttpPort: Int,
  peerHttpPort: Int
)(implicit F: Concurrent[F], CS: ContextShift[F], P: Parallel[F], C: Clock[F], T: Timer[F])
    extends HealthCheckConsensusManagerBase[
      F,
      PingHealthCheckKey,
      PeerPingHealthCheckStatus,
      PingHealthStatus,
      SendConsensusHealthStatus[PingHealthCheckKey, PeerPingHealthCheckStatus, PingHealthStatus]
    ](
      ownId,
      cluster,
      clusterStorage,
      metrics,
      apiClient,
      unboundedHealthBlocker,
      healthHttpPort,
      peerHttpPort,
      PingHealthCheckConsensusDriver,
      PingHealthCheck
    ) {

  val healthCheckTimeout: FiniteDuration = 5.seconds

  // maybe this could be dynamic and dependent on cluster size, the bigger the cluster size the longer timespan
  val maxTimespanWithoutOwnReconciliationRound: FiniteDuration = 6.hours

  private val reconciliationRound: Ref[F, Option[ReconciliationRound[F]]] = Ref.unsafe(None)
  private val peersToRunHealthcheckFor: Ref[F, Set[Id]] = Ref.unsafe(Set.empty) // use THIS to start healthchecks
  private val peersToRunConsensusFor: Ref[F, Set[Id]] = Ref.unsafe(Set.empty) // use THIS to start consensuses
  private val peersThatNeedReconciliation: Ref[F, Set[Id]] = Ref.unsafe(Set.empty)
  private val timeOfLastOwnReconciliationRound: Ref[F, Option[FiniteDuration]] = Ref.unsafe(None)
  private val runReconciliationRoundAfter: Ref[F, Option[FiniteDuration]] = Ref.unsafe(None)

  def getNotOfflinePeers(peers: Map[Id, PeerData]): Set[Id] =
    peers.filter {
      case (_, peerData) => canActAsJoiningSource(peerData.peerMetadata.nodeState)
    }.keySet

  override def periodicPeersHealthCheck(): F[Unit] =
    for {
      peersUnderConsensus <- getPeersUnderConsensus()
      peersToRunHealthcheckFor <- peersToRunHealthcheckFor.modify(current => (Set.empty, current))
      healthcheckStartTime <- getTimeInSeconds()
      unresponsivePeers <- peerHealthCheck
        .check(peersUnderConsensus, peersToRunHealthcheckFor)
        .map(_.map(_.peerMetadata.id).toSet)
      peers <- clusterStorage.getPeers
      notOfflinePeers = getNotOfflinePeers(peers)
      historical <- getHistoricalRounds()
      peerToRunConsensusFor = unresponsivePeers
        .intersect(notOfflinePeers)
        .filterNot(
          id => historical.exists(hrd => hrd.checkedPeer.id == id && hrd.finishedAtSecond > healthcheckStartTime)
        )
      _ <- peerToRunConsensusFor.toList.traverse { id =>
        for {
          delayedHealthCheckStatus <- F.start(PeerUnresponsive(id).asInstanceOf[PeerPingHealthCheckStatus].pure[F])
          _ <- startOwnConsensusForId(PingHealthCheckKey(id), delayedHealthCheckStatus)
        } yield ()
      }
    } yield ()

  override def periodicOperation(): F[Unit] =
    for {
      peersToRunConsensusFor <- peersToRunConsensusFor.modify(peers => (Set.empty, peers))
      /*
    TODO: consider adding a logic checking if not too many consensuses are started at the same time
     e.g. when reconciliation detects one peer that misses a lot of other peers - this would
     trigger consensus for every single one of these peers
       */
      _ <- peersToRunConsensusFor.toList.traverse { id =>
        for {
          delayedHealthCheckStatus <- F.start(PeerUnresponsive(id).asInstanceOf[PeerPingHealthCheckStatus].pure[F])
          _ <- startOwnConsensusForId(PingHealthCheckKey(id), delayedHealthCheckStatus)
        } yield ()
      }
    } yield ()

  override def checkHealthForPeer(key: PingHealthCheckKey): F[Fiber[F, PeerPingHealthCheckStatus]] =
    for {
      allPeers <- clusterStorage.getPeers
      checkedPeerData = allPeers.get(key.id) // should I check the status if it's not offline or leaving at the moment
      delayedHealthCheckStatus <- F.start {
        checkedPeerData
          .fold(UnknownPeer(key.id).asInstanceOf[PeerPingHealthCheckStatus].pure[F])(
            pd => peerHealthCheck.checkPeer(pd.peerMetadata.toPeerClientMetadata, healthCheckTimeout)
          )
      }
    } yield delayedHealthCheckStatus

  override def periodicOperationWhenNoConsensusesInProgress(): F[Unit] = manageReconciliation()

  override def onSuccessfullRoundStart(): F[Unit] =
    reconciliationRound.modify(_ => (None, ()))

  override def readyConsensusesAction(
    consensuses: Map[PingHealthCheckKey, HealthCheckConsensus[
      F,
      PingHealthCheckKey,
      PeerPingHealthCheckStatus,
      PingHealthStatus,
      SendConsensusHealthStatus[PingHealthCheckKey, PeerPingHealthCheckStatus, PingHealthStatus]
    ]]
  ): F[Unit] =
    for {
      peers <- clusterStorage.getPeers
      notOfflinePeers = getNotOfflinePeers(peers)
      peersWeDidntHave <- fetchAbsentPeers(consensuses)
      peersWeStillDontHave = peersWeDidntHave -- notOfflinePeers
      _ <- if (peersWeDidntHave.nonEmpty) logger.debug(s"peersWeDidntHave: ${logIds(peersWeDidntHave)}") else F.unit
      _ <- if (peersWeStillDontHave.nonEmpty) logger.debug(s"peersWeStillDontHave: ${logIds(peersWeStillDontHave)}")
      else F.unit
      // should peers here also be filtered base on offline/online states?
      // Also shouldn't we only find newPeers for finished rounds?
      newPeers <- findNewPeers(peers, consensuses)
      // previously find new peers fetched them from all consensuses, now we get them from ready consensuses
      _ <- if (newPeers.nonEmpty) logger.debug(s"newPeers: ${logIds(newPeers)}") else F.unit
      _ <- peersToRunConsensusFor.modify(peers => (peers ++ peersWeStillDontHave, ()))
      _ <- peersThatNeedReconciliation.modify(peers => (peers ++ newPeers, ()))
    } yield ()

  override def positiveOutcomeAction(
    positiveOutcomePeers: Map[
      PingHealthCheckKey,
      (
        HealthcheckConsensusDecision[PingHealthCheckKey],
        HealthCheckConsensus[
          F,
          PingHealthCheckKey,
          PeerPingHealthCheckStatus,
          PingHealthStatus,
          SendConsensusHealthStatus[PingHealthCheckKey, PeerPingHealthCheckStatus, PingHealthStatus]
        ]
      )
    ]
  ): F[Unit] =
    for {
      peers <- clusterStorage.getPeers
      notOfflinePeers = getNotOfflinePeers(peers)
      peersToAdd = positiveOutcomePeers -- notOfflinePeers.map(PingHealthCheckKey(_))
      _ <- if (peersToAdd.nonEmpty)
        logger.warn(s"Peers that we don't have online but we should: ${(peersToAdd.keySet)}")
      else F.unit
      _ <- F.start(
        peersToAdd.toList.traverse { case (key, (_, consensus)) => addMissingPeer(key.id, consensus) }
      )
    } yield ()

  def getClusterState(): F[ClusterState] =
    for {
      allRounds <- consensusRounds.get
      ConsensusRounds(_, inProgress) = allRounds
      rounds <- inProgress.toList.traverse {
        case (key, consensus) => consensus.getRoundIds().map(key.id -> _.toSortedSet)
      }.map(_.toMap)
      ownState <- nodeStorage.getNodeState
      ownJoinedHeight <- nodeStorage.getOwnJoinedHeight
      ownNodeReconciliationData = NodeReconciliationData(
        id = ownId,
        nodeState = ownState.some,
        roundInProgress = None,
        joiningHeight = ownJoinedHeight
      )
      allPeers <- clusterStorage.getPeers
      peersWeRunConsensusForButDontHave = (rounds -- allPeers.keySet).map {
        case (id, roundIds) =>
          id -> NodeReconciliationData(
            id = id,
            nodeState = None,
            roundInProgress = roundIds.some,
            joiningHeight = None
          )
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

  private def fetchAbsentPeers(
    consensuses: Map[PingHealthCheckKey, HealthCheckConsensus[
      F,
      PingHealthCheckKey,
      PeerPingHealthCheckStatus,
      PingHealthStatus,
      SendConsensusHealthStatus[PingHealthCheckKey, PeerPingHealthCheckStatus, PingHealthStatus]
    ]]
  ): F[Set[Id]] =
    if (consensuses.isEmpty) Set.empty[Id].pure[F]
    else
      logger.debug("Started fetching absent peers.") >>
        consensuses.toList.traverse {
          case (_, consensus) =>
            consensus
              .getRoundPeers()
              .map(_.filter { case (_, roundData) => roundData.peerData.isLeft }.keySet)
        }.map(_.flatten.toSet)

  private def findNewPeers(
    currentPeers: Map[Id, PeerData],
    consensuses: Map[PingHealthCheckKey, HealthCheckConsensus[
      F,
      PingHealthCheckKey,
      PeerPingHealthCheckStatus,
      PingHealthStatus,
      SendConsensusHealthStatus[PingHealthCheckKey, PeerPingHealthCheckStatus, PingHealthStatus]
    ]]
  ): F[Set[Id]] =
    if (consensuses.isEmpty) Set.empty[Id].pure[F]
    else
      logger.debug("Finding new peers.") >>
        consensuses.toList.traverse {
          case (_, consensus) =>
            consensus
              .getRoundPeers()
              .map(_.keySet)
        }.map(_.flatten.toSet)
          .map(currentPeers.keySet -- consensuses.keySet.map(_.id) -- _)

  def addMissingPeer(
    id: Id,
    consensus: HealthCheckConsensus[
      F,
      PingHealthCheckKey,
      PeerPingHealthCheckStatus,
      PingHealthStatus,
      SendConsensusHealthStatus[PingHealthCheckKey, PeerPingHealthCheckStatus, PingHealthStatus]
    ]
  ): F[Unit] = {
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
            _ <- metrics.incrementMetricAsync("ownReconciliationRounds_total")
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
      peersCount <- clusterStorage.getPeers.map(_.size)
      currentEpoch <- getTimeInSeconds()
      runAfter <- runReconciliationRoundAfter.get
      _ <- runAfter match {
        case Some(roundEpoch) if roundEpoch < currentEpoch =>
          for {
            roundId <- createRoundId()
            round <- reconciliationRound.modify { _ =>
              val round = ReconciliationRound[F](
                ownId,
                clusterStorage,
                nodeStorage,
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
}

object PingHealthCheckConsensusManager {

  def apply[F[_]: Concurrent: ContextShift: Parallel: Timer](
    ownId: Id,
    cluster: Cluster[F],
    clusterStorage: ClusterStorageAlgebra[F],
    nodeStorage: NodeStorageAlgebra[F],
    peerHealthCheck: PeerHealthCheck[F],
    metrics: Metrics,
    apiClient: ClientInterpreter[F],
    unboundedHealthBlocker: Blocker,
    healthHttpPort: Int,
    peerHttpPort: Int
  ): PingHealthCheckConsensusManager[F] =
    new PingHealthCheckConsensusManager(
      ownId,
      cluster,
      clusterStorage,
      nodeStorage,
      peerHealthCheck,
      new PrefixedHealthCheckMetrics[F](metrics, PingHealthCheck),
      apiClient,
      unboundedHealthBlocker,
      healthHttpPort,
      peerHttpPort
    )
}
