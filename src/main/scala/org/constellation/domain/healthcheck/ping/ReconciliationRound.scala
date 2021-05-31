package org.constellation.domain.healthcheck.ping

import cats.effect.concurrent.Ref
import cats.effect.{Blocker, Clock, Concurrent, ContextShift, Fiber, Timer}
import cats.syntax.all._
import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import org.constellation.domain.cluster.{ClusterStorageAlgebra, NodeStorageAlgebra}
import org.constellation.domain.healthcheck.HealthCheckConsensus.HealthcheckRoundId
import org.constellation.domain.healthcheck.HealthCheckLoggingHelper.logId
import org.constellation.domain.healthcheck.HealthCheckType.PingHealthCheck
import org.constellation.domain.healthcheck.PrefixedHealthCheckLogger
import org.constellation.infrastructure.p2p.{ClientInterpreter, PeerResponse}
import org.constellation.p2p.Cluster
import org.constellation.schema.NodeState.{canActAsJoiningSource, isInitiallyJoining, isInvalidForJoining}
import org.constellation.schema.consensus.RoundId
import org.constellation.schema.{Id, NodeState}

import scala.concurrent.duration.{DurationInt, FiniteDuration}

class ReconciliationRound[F[_]](
  ownId: Id,
  clusterStorage: ClusterStorageAlgebra[F],
  nodeStorage: NodeStorageAlgebra[F],
  val startedAtSecond: FiniteDuration,
  val roundId: RoundId,
  apiClient: ClientInterpreter[F],
  unboundedHealthBlocker: Blocker,
  healthHttpPort: String
)(implicit F: Concurrent[F], CS: ContextShift[F], C: Clock[F], T: Timer[F]) {
  import ReconciliationRound._

  val logger: PrefixedHealthCheckLogger[F] = new PrefixedHealthCheckLogger[F](PingHealthCheck)

  val retryAttempts = 2
  val retryAfter: FiniteDuration = 60.seconds

  private[healthcheck] val reconciliationResult: Ref[F, Option[ReconciliationResult]] =
    Ref.unsafe(None)

  def getReconciliationResult(): F[Option[ReconciliationResult]] = reconciliationResult.get

  def start(): F[Fiber[F, Unit]] = F.start(run())

  private def run(
    attemptsLeft: Int = retryAttempts,
    firstClusterAlignmentResult: Option[Map[Id, List[ClusterAlignmentResult]]] = None,
    lastClusterAlignmentResult: Option[Map[Id, List[ClusterAlignmentResult]]] = None
  ): F[Unit] =
    for {
      isFirstRun <- (attemptsLeft == retryAttempts).pure[F]
      isLastRun = attemptsLeft == 0
      _ <- logger.debug(
        s"Reconciliation round with id=$roundId, is running. isFirstRun=$isFirstRun isLastRun=$isLastRun attemptsLeft=$attemptsLeft."
      )
      _ <- if (isFirstRun) F.unit else T.sleep(retryAfter)
      result <- runReconciliation()
      _ <- result match {
        case Left(unresponsivePeers) =>
          reconciliationResult.modify(
            // shouldn't we only run healthcheck for nodes that didn't respond instead of running consensus right away?
            // in consequence I think we should also try to calculate cluster state from the responses we got and start consensuses for these
            _ => (ReconciliationResult(unresponsivePeers, Set.empty, lastClusterAlignmentResult).some, ())
          )

        case Right(clusterAlignmentResults) if areAllNodesAligned(clusterAlignmentResults) =>
          reconciliationResult.modify(
            _ => (ReconciliationResult(Set.empty, Set.empty, clusterAlignmentResults.some).some, ())
          )
        case Right(clusterAlignmentResults) if !isLastRun =>
          run(
            attemptsLeft - 1,
            if (isFirstRun) clusterAlignmentResults.some else firstClusterAlignmentResult,
            clusterAlignmentResults.some
          )
        case Right(clusterAlignmentResults) =>
          for {
            initiallyMisaligned <- firstClusterAlignmentResult.map(fetchMisalignedPeers).getOrElse(Set.empty).pure[F]

            currentlyMisaligned = fetchMisalignedPeers(clusterAlignmentResults)
            misaligned = initiallyMisaligned.intersect(currentlyMisaligned)
            _ <- reconciliationResult.modify(
              _ => (ReconciliationResult(Set.empty, misaligned, clusterAlignmentResults.some).some, ())
            )
          } yield ()
      }
    } yield ()

  def areAllNodesAligned(clusterAlignmentResults: Map[Id, List[ClusterAlignmentResult]]): Boolean =
    clusterAlignmentResults.forall { case (_, result) => result == List(NodeAligned) }

  def fetchMisalignedPeers(clusterAlignmentResults: Map[Id, List[ClusterAlignmentResult]]): Set[Id] =
    clusterAlignmentResults.filter { case (id, result) => result != List(NodeAligned) }.keySet

  // Should we filter out (not send) node's observation about itself?
  private def runReconciliation(): F[Either[Set[Id], Map[Id, List[ClusterAlignmentResult]]]] =
    for {
      allPeers <- clusterStorage.getPeers
      ownState <- nodeStorage.getNodeState
      ownJoinedHeight <- nodeStorage.getOwnJoinedHeight
      peers = allPeers.filter { case (_, peerData) => canActAsJoiningSource(peerData.peerMetadata.nodeState) }
      // assumption and check before starting reconciliation is that there should be no rounds in progress
      // if the round starts and reconciliation is in progress, reconciliation should be canceled and result discarded
      ownClusterState = allPeers.mapValues(
        pd =>
          NodeReconciliationData(
            pd.peerMetadata.id,
            pd.peerMetadata.nodeState.some,
            none[Set[HealthcheckRoundId]],
            pd.majorityHeight.head.joined // is that correct, head???
          )
      ) + (ownId -> NodeReconciliationData(
        ownId,
        ownState.some,
        none[Set[HealthcheckRoundId]],
        ownJoinedHeight
      ))
      responses <- peers.toList.traverse {
        case (id, peerData) =>
          PeerResponse
            .run(
              apiClient.healthcheck.getClusterState(),
              unboundedHealthBlocker
            )(peerData.peerMetadata.toPeerClientMetadata.copy(port = healthHttpPort))
            .map(_.some)
            .handleErrorWith { e =>
              logger.debug(e)(s"Error during fetching clusterState from id=${logId(id)}") >>
                none[ClusterState].pure[F]
            }
            .map(id -> _)
      }.map(_.toMap)
      (successfulPeers, peersThatFailed) = responses.partition {
        case (_, maybeClusterState) => maybeClusterState.nonEmpty
      }
      result = if (peersThatFailed.nonEmpty)
        peersThatFailed.keySet.asLeft[Map[Id, List[ClusterAlignmentResult]]]
      else {
        val peersClusterState = successfulPeers.collect {
          case (id, Some(clusterState)) => id -> clusterState
        } + (ownId -> ownClusterState)

        calculateClusterAlignment(peersClusterState).asRight[Set[Id]]
      }
    } yield result
}

object ReconciliationRound {

  def apply[F[_]: Concurrent: ContextShift: Clock: Timer](
    ownId: Id,
    clusterStorage: ClusterStorageAlgebra[F],
    nodeStorage: NodeStorageAlgebra[F],
    startedAtSecond: FiniteDuration,
    roundId: RoundId,
    apiClient: ClientInterpreter[F],
    unboundedHealthBlocker: Blocker,
    healthHttpPort: Int
  ): ReconciliationRound[F] =
    new ReconciliationRound(
      ownId,
      clusterStorage,
      nodeStorage,
      startedAtSecond,
      roundId,
      apiClient,
      unboundedHealthBlocker,
      healthHttpPort = healthHttpPort.toString
    )

  // should we have a check if the cluster inconsistency isn't coming from one node only? then run consensus for that node, and not all nodes it sees inconsistently (to avoid consensuses blast)?
  def calculateClusterAlignment(peersClusterState: Map[Id, ClusterState]): Map[Id, List[ClusterAlignmentResult]] = {
    val allPeers = peersClusterState.values.flatMap(_.keySet).toSet

    val peersWithObservations = allPeers.map { checkedId =>
      checkedId -> peersClusterState.mapValues(cs => cs.get(checkedId))
    }.toMap

    val peersWithAlignmentResult = peersWithObservations.map {
      // what about joining height, we can use it to assert on the status also
      case (checkedId, peersObservations) =>
        // should we maybe always ignore peer's observation about itself?, if yes we don't need to send it at all
        val notPresentOnAllNodes: Option[NodeNotPresentOnAllNodes] =
          notPresentOnAllNodesCheck(checkedId, peersObservations)
        val inconsistentStatus: Option[NodeInconsistentlySeenAsOnlineOrOffline] =
          inconsistentStatusCheck(checkedId, peersObservations)

        val alignmentResult: List[ClusterAlignmentResult] =
          Option(List(notPresentOnAllNodes, inconsistentStatus).flatten)
            .filter(_.nonEmpty)
            .getOrElse(List(NodeAligned))

        checkedId -> alignmentResult
    }

    peersWithAlignmentResult
  }

  /**
    * Checks if the node is visible on all other nodes. If it's not it asserts that's not caused by the node
    * still joining, still leaving or healthcheck consensus still running on some of the nodes.
    */
  def notPresentOnAllNodesCheck(
    checkedId: Id,
    peersObservations: Map[Id, Option[NodeReconciliationData]]
  ): Option[NodeNotPresentOnAllNodes] = {
    val (peersThatSeeId, peersThatDontSeeId) =
      peersObservations.partition { case (_, maybeData) => maybeData.exists(_.nodeState.nonEmpty) }

    if (peersThatDontSeeId.nonEmpty && !(
          peersThatSeeId.values.forall(_.exists(_.nodeState.exists(isInitiallyJoining))) ||
            peersThatSeeId.values.forall(_.exists(_.nodeState.exists(isInvalidForJoining))) ||
            (peersThatSeeId - checkedId).values.forall(_.exists(_.roundInProgress.nonEmpty)) ||
            (peersThatDontSeeId - checkedId).values.forall(_.exists(_.roundInProgress.nonEmpty))
        ))
      NodeNotPresentOnAllNodes(peersThatSeeId.keySet, peersThatDontSeeId.keySet).some
    else
      None
  }

  /**
    * Checks if the node is consistently perceived as online or offline. If it's not it asserts that's not
    * caused by healthcheck consensus still running on some of the nodes.
    */
  def inconsistentStatusCheck(
    checkedId: Id,
    peersObservations: Map[Id, Option[NodeReconciliationData]]
  ): Option[NodeInconsistentlySeenAsOnlineOrOffline] = {
    val (peersThatSeeCheckedIdAsOnline, peersThatSeeCheckedIdAsOffline) =
      peersObservations.collect { case (id, Some(a @ NodeReconciliationData(_, Some(_), _, _))) => id -> a } // I collect to not overlap the other cases
      .partition {
        case (_, nrd) => !nrd.nodeState.forall(isInvalidForJoining)
      }

    val areOnlyOfflineNodesWithRoundInProgress =
      (peersThatSeeCheckedIdAsOffline - checkedId).values.forall(_.roundInProgress.nonEmpty) &&
        (peersThatSeeCheckedIdAsOnline - checkedId).values.forall(_.roundInProgress.isEmpty)
    val areOnlyOnlineNodesWithRoundInProgress =
      (peersThatSeeCheckedIdAsOnline - checkedId).values.forall(_.roundInProgress.nonEmpty) &&
        (peersThatSeeCheckedIdAsOffline - checkedId).values.forall(_.roundInProgress.isEmpty)

    if (peersThatSeeCheckedIdAsOffline.nonEmpty &&
        peersThatSeeCheckedIdAsOnline.nonEmpty &&
        !(areOnlyOfflineNodesWithRoundInProgress || areOnlyOnlineNodesWithRoundInProgress))
      NodeInconsistentlySeenAsOnlineOrOffline(
        peersThatSeeCheckedIdAsOnline.keySet,
        peersThatSeeCheckedIdAsOffline.keySet
      ).some
    else
      None
  }

  case class ReconciliationResult(
    peersToRunHealthCheckFor: Set[Id],
    misalignedPeers: Set[Id],
    clusterAlignment: Option[Map[Id, List[ClusterAlignmentResult]]]
  )

  // maybe it would be better to have it as sealed trait and have two extending classes,
  // one when nodes has the node in peers list, another when node doesn't but runs consensus for that node
  // currently we know that by nodeState being empty or not and that's not too explicit
  case class NodeReconciliationData(
    id: Id,
    nodeState: Option[NodeState],
    roundInProgress: Option[Set[HealthcheckRoundId]],
    joiningHeight: Option[Long]
  )

  object NodeReconciliationData {
    implicit val nodeReconciliationDataCodec: Codec[NodeReconciliationData] = deriveCodec
  }

  type ClusterState = Map[Id, NodeReconciliationData]

  sealed trait ClusterAlignmentResult
  case object NodeAligned extends ClusterAlignmentResult
  case class NodeNotPresentOnAllNodes(presentOn: Set[Id], notPresentOn: Set[Id]) extends ClusterAlignmentResult
  case class NodeInconsistentlySeenAsOnlineOrOffline(onlineOn: Set[Id], offlineOn: Set[Id])
      extends ClusterAlignmentResult
}
