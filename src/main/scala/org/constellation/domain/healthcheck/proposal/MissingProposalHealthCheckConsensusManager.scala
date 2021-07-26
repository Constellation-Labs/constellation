package org.constellation.domain.healthcheck.proposal

import cats.Parallel
import cats.data.NonEmptyList
import cats.effect.concurrent.Ref
import cats.effect.{Blocker, Clock, Concurrent, ContextShift, Fiber, Timer}
import cats.syntax.all._
import org.constellation.ConfigUtil
import org.constellation.domain.cluster.{ClusterStorageAlgebra, NodeStorageAlgebra}
import org.constellation.domain.healthcheck.HealthCheckConsensus.{
  HealthcheckConsensusDecision,
  MissingProposalHealthStatus,
  RoundData,
  SendConsensusHealthStatus
}
import org.constellation.domain.healthcheck.HealthCheckKey.MissingProposalHealthCheckKey
import org.constellation.domain.healthcheck.HealthCheckLoggingHelper.{
  logHealthCheckKey,
  logHealthCheckKeys,
  logIdShort,
  logIds
}
import org.constellation.domain.healthcheck.HealthCheckStatus.{
  GotPeerProposalAtHeight,
  MissingOwnProposalAtHeight,
  MissingPeerProposalAtHeight,
  MissingProposalHealthCheckStatus
}
import org.constellation.domain.healthcheck.HealthCheckType.MissingProposalHealthCheck
import org.constellation.domain.healthcheck.{
  HealthCheckConsensus,
  HealthCheckConsensusManagerBase,
  PrefixedHealthCheckMetrics
}
import org.constellation.domain.redownload.{MissingProposalFinder, RedownloadService, RedownloadStorageAlgebra}
import org.constellation.infrastructure.p2p.ClientInterpreter
import org.constellation.p2p.{Cluster, MajorityHeight, PeerData}
import org.constellation.schema.Id
import org.constellation.schema.snapshot.HeightRange
import org.constellation.util.Metrics

import scala.concurrent.duration.{DurationInt, FiniteDuration}

// TODO: peers selection needs to be reverified and the negative outcome action as well I think we may need to
//       not remove the peers as this means they will only be removed on full active peers and not all the nodes
//       so instead we may need to remove these nodes from active pool not from peers list, but in this case
//       light nodes also need to be notified
class MissingProposalHealthCheckConsensusManager[F[_]](
  ownId: Id,
  cluster: Cluster[F],
  clusterStorage: ClusterStorageAlgebra[F],
  nodeStorage: NodeStorageAlgebra[F],
  redownloadStorage: RedownloadStorageAlgebra[F],
  missingProposalFinder: MissingProposalFinder,
  metrics: PrefixedHealthCheckMetrics[F],
  apiClient: ClientInterpreter[F],
  unboundedHealthBlocker: Blocker,
  healthHttpPort: Int,
  peerHttpPort: Int
)(implicit F: Concurrent[F], CS: ContextShift[F], P: Parallel[F], C: Clock[F], T: Timer[F])
    extends HealthCheckConsensusManagerBase[
      F,
      MissingProposalHealthCheckKey,
      MissingProposalHealthCheckStatus,
      MissingProposalHealthStatus,
      SendConsensusHealthStatus[
        MissingProposalHealthCheckKey,
        MissingProposalHealthCheckStatus,
        MissingProposalHealthStatus
      ]
    ](
      ownId,
      cluster,
      clusterStorage,
      metrics,
      apiClient,
      unboundedHealthBlocker,
      healthHttpPort,
      peerHttpPort,
      MissingProposalHealthCheckConsensusDriver,
      MissingProposalHealthCheck
    ) {

  val snapshotHeightInterval: Int = ConfigUtil.constellation.getInt("snapshot.snapshotHeightInterval")

  val stuckMajorityPatience: FiniteDuration = 10.minutes

  case class MajorityMaxHeightWithTimestamp(height: Long, timestamp: FiniteDuration)
  private val lastMajorityHeightWithTimestamp: Ref[F, Option[MajorityMaxHeightWithTimestamp]] = Ref.unsafe(None)

  override def getHealthcheckPeers(): F[Map[Id, PeerData]] = clusterStorage.getActiveFullPeers()

  override def periodicOperation(): F[Unit] =
    for {
      currentTimestamp <- getTimeInSeconds()
      latestMajority <- redownloadStorage.getLatestMajorityHeight
      created <- redownloadStorage.getCreatedSnapshots
      _ <- lastMajorityHeightWithTimestamp.modify {
        case Some(majorityWithTimestamp) =>
          if (majorityWithTimestamp.height < latestMajority)
            (MajorityMaxHeightWithTimestamp(latestMajority, currentTimestamp).some, ())
          else
            (majorityWithTimestamp.some, ())
        case None
            if latestMajority > 0 || (latestMajority == 0 && created.contains(
              latestMajority + snapshotHeightInterval
            )) =>
          (MajorityMaxHeightWithTimestamp(latestMajority, currentTimestamp).some, ())
        case None =>
          (None, ())
      }
    } yield ()

  override def periodicPeersHealthCheck(): F[Unit] =
    for {
      currentTimestamp <- getTimeInSeconds()
      maybeLatestMajorityWithTimestamp <- lastMajorityHeightWithTimestamp.get
      peers <- getHealthcheckPeers()
      ownPeer <- nodeStorage.getOwnJoinedHeight
      peersCache = peers.map {
        case (id, peerData) => (id, peerData.majorityHeight)
      } ++ Map(ownId -> NonEmptyList.one(MajorityHeight(ownPeer, None)))
      proposals <- redownloadStorage.getPeersProposals
      created <- redownloadStorage.getCreatedSnapshots
      recentlyCheckedKeys <- getHistoricalRounds().map(_.mapFilter { historical =>
        if (currentTimestamp - historical.finishedAtSecond < 1.minute) historical.key.some
        else None
      }.toSet)
      keys = maybeLatestMajorityWithTimestamp match {
        case Some(majorityWithTimestamp)
            if (currentTimestamp - majorityWithTimestamp.timestamp > stuckMajorityPatience) && created.keySet.contains(
              majorityWithTimestamp.height + snapshotHeightInterval
            ) =>
          val nextHeight = majorityWithTimestamp.height + snapshotHeightInterval
          val heightRange = HeightRange(nextHeight, nextHeight)
          val missing = missingProposalFinder.findMissingPeerProposals(heightRange, proposals, peersCache)

          // TODO: maybe we need some logic to limit the number of started consensuses, maybe less then 90% of the cluster must
          val keys = missing.map(_._1).map(MissingProposalHealthCheckKey(_, nextHeight)) -- recentlyCheckedKeys

          keys
        case _ =>
          Set.empty[MissingProposalHealthCheckKey]
      }
      _ <- if (keys.nonEmpty)
        logger.warn(
          s"Found following missing proposal keys: ${logHealthCheckKeys(keys)}. Will attempt to run consensuses."
        )
      else
        F.unit

      _ <- keys.toList.traverse { k =>
        for {
          delayedHealthCheckStatus <- F.start(
            MissingPeerProposalAtHeight(k.id, k.height).asInstanceOf[MissingProposalHealthCheckStatus].pure[F]
          )
          _ <- startOwnConsensusForId(k, delayedHealthCheckStatus)
        } yield ()
      }
    } yield ()

  override def checkHealthForPeer(key: MissingProposalHealthCheckKey): F[Fiber[F, MissingProposalHealthCheckStatus]] =
    F.start {
      for {
        ownProposals <- redownloadStorage.getCreatedSnapshots
        peerProposals <- redownloadStorage.getPeersProposals
        notExistsOwnProposalAtHeight = !ownProposals.exists { case (height, _) => height == key.height }
        // TODO: different status for when we don't have any proposals for that peer???
        existsPeerProposalAtHeight = peerProposals
          .get(key.id)
          .exists(_.exists { case (height, _) => height == key.height })
        healthCheckStatus = if (existsPeerProposalAtHeight) GotPeerProposalAtHeight(key.id, key.height)
        else if (notExistsOwnProposalAtHeight)
          MissingOwnProposalAtHeight(ownId, key.height) // TODO: correct? should I put my own id here, or any id actually? Isn't height enough?
        else MissingPeerProposalAtHeight(key.id, key.height)
      } yield healthCheckStatus
    }

  override def periodicOperationWhenNoConsensusesInProgress(): F[Unit] = F.unit

  override def onSuccessfullRoundStart(): F[Unit] = F.unit

  override def readyConsensusesAction(
    consensuses: Map[MissingProposalHealthCheckKey, HealthCheckConsensus[
      F,
      MissingProposalHealthCheckKey,
      MissingProposalHealthCheckStatus,
      MissingProposalHealthStatus,
      SendConsensusHealthStatus[
        MissingProposalHealthCheckKey,
        MissingProposalHealthCheckStatus,
        MissingProposalHealthStatus
      ]
    ]]
  ): F[Unit] = F.unit

  override def positiveOutcomeAction(
    positiveOutcomePeers: Map[
      MissingProposalHealthCheckKey,
      (
        HealthcheckConsensusDecision[MissingProposalHealthCheckKey],
        HealthCheckConsensus[
          F,
          MissingProposalHealthCheckKey,
          MissingProposalHealthCheckStatus,
          MissingProposalHealthStatus,
          SendConsensusHealthStatus[
            MissingProposalHealthCheckKey,
            MissingProposalHealthCheckStatus,
            MissingProposalHealthStatus
          ]
        ]
      )
    ]
  ): F[Unit] =
    positiveOutcomePeers.toList.traverse {
      case (key, (_, consensus)) =>
        for {
          ownStatus <- consensus.getOwnConsensusHealthStatus().map(_.status)
          _ <- if (ownStatus.isInstanceOf[GotPeerProposalAtHeight])
            logger.debug(s"Already have the proposal for key=${logHealthCheckKey(key)}. Omitting proposal download!")
          else
            logger.warn(s"Missing proposal at height=${key.height} for id=${logIdShort(key.id)}")
        } yield ()
    }.void

}

object MissingProposalHealthCheckConsensusManager {

  def apply[F[_]: Concurrent: ContextShift: Parallel: Timer](
    ownId: Id,
    cluster: Cluster[F],
    clusterStorage: ClusterStorageAlgebra[F],
    nodeStorage: NodeStorageAlgebra[F],
    redownloadStorage: RedownloadStorageAlgebra[F],
    missingProposalFinder: MissingProposalFinder,
    metrics: Metrics,
    apiClient: ClientInterpreter[F],
    unboundedHealthBlocker: Blocker,
    healthHttpPort: Int,
    peerHttpPort: Int
  ): MissingProposalHealthCheckConsensusManager[F] =
    new MissingProposalHealthCheckConsensusManager(
      ownId,
      cluster,
      clusterStorage,
      nodeStorage,
      redownloadStorage,
      missingProposalFinder,
      new PrefixedHealthCheckMetrics[F](metrics, MissingProposalHealthCheck),
      apiClient,
      unboundedHealthBlocker,
      healthHttpPort,
      peerHttpPort
    )
}
