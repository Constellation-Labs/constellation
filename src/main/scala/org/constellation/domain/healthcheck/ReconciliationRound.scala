package org.constellation.domain.healthcheck

import cats.effect.concurrent.Ref
import cats.effect.{Blocker, Clock, Concurrent, ContextShift, Fiber, Timer}
import cats.syntax.all._
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import org.constellation.infrastructure.p2p.{ClientInterpreter, PeerResponse}
import org.constellation.p2p.Cluster
import org.constellation.schema.NodeState.{canActAsJoiningSource, isInitiallyJoining, isInvalidForJoining}
import org.constellation.schema.consensus.RoundId
import org.constellation.schema.{Id, NodeState}

import scala.concurrent.duration.{DurationInt, FiniteDuration}

class ReconciliationRound[F[_]](
  ownId: Id,
  cluster: Cluster[F],
  val startedAtSecond: Long
)(
  apiClient: ClientInterpreter[F],
  unboundedBlocker: Blocker
)(implicit F: Concurrent[F], CS: ContextShift[F], C: Clock[F], T: Timer[F]) {
  import ReconciliationRound._

  val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]

  val retryAttempts = 2
  val retryAfter: FiniteDuration = 60.seconds

  private[healthcheck] val reconciliationResult: Ref[F, Option[ReconciliationResult]] =
    Ref.unsafe(None)

//  private[healthcheck] val firstClusterAlignmentResults: Ref[F, Option[Map[Id, List[ClusterAlignmentResult]]]] =
//    Ref.unsafe(None)

//  private[healthcheck] val lastClusterAlignmentResults: Ref[F, Option[Map[Id, List[ClusterAlignmentResult]]]] =
//    Ref.unsafe(None)

  def getReconciliationResult(): F[Option[ReconciliationResult]] = reconciliationResult.get

  def start(): F[Fiber[F, Unit]] = F.start(run())

  private def run(
    attemptsLeft: Int = retryAttempts,
    firstClusterAlignmentResult: Option[Map[Id, List[ClusterAlignmentResult]]] = None,
    lastClusterAlignmentResult: Option[Map[Id, List[ClusterAlignmentResult]]] = None
  ): F[Unit] =
    for {
      isFirstRun <- F.delay(attemptsLeft == retryAttempts)
      isLastRun <- F.delay(attemptsLeft == 0)
      _ <- if (isFirstRun) F.unit else T.sleep(retryAfter)
      result <- runReconciliation()
      _ <- result match {
        case Left(unresponsivePeers) =>
          reconciliationResult.modify(
            // shouldn't we only run healthcheck for nodes that didn't respond instead of running consensus right away?
            // in consequence I think we should also try to calculate cluster state from the responses we got and start consensuses for these
            _ => (ReconciliationResult(unresponsivePeers, Set.empty, lastClusterAlignmentResult).some, ())
          )

        case Right(clusterAlignmentResults) =>
          if (areAllNodesAligned(clusterAlignmentResults))
            reconciliationResult.modify(
              _ => (ReconciliationResult(Set.empty, Set.empty, clusterAlignmentResults.some).some, ())
            )
          else if (!isLastRun)
            run(
              attemptsLeft - 1,
              if (isFirstRun) clusterAlignmentResults.some else firstClusterAlignmentResult,
              clusterAlignmentResults.some
            )
          else {
            for {
              initiallyMisaligned <- F.delay(
                firstClusterAlignmentResult.map(fetchMisalignedPeers).getOrElse(Set.empty)
              )
              currentlyMisaligned <- F.delay(fetchMisalignedPeers(clusterAlignmentResults))
              misaligned = initiallyMisaligned.intersect(currentlyMisaligned)
              _ <- reconciliationResult.modify(
                _ => (ReconciliationResult(Set.empty, misaligned, clusterAlignmentResults.some).some, ())
              )
            } yield ()
          }
      }
    } yield ()

  def areAllNodesAligned(clusterAlignmentResults: Map[Id, List[ClusterAlignmentResult]]): Boolean =
    clusterAlignmentResults.forall { case (_, result) => result == List(NodeAligned) }

  def fetchMisalignedPeers(clusterAlignmentResults: Map[Id, List[ClusterAlignmentResult]]): Set[Id] =
    clusterAlignmentResults.filter { case (id, result) => result != List(NodeAligned) }.keySet

  // Should we filter out (not send) node's observation about itself?
  private def runReconciliation(): F[Either[Set[Id], Map[Id, List[ClusterAlignmentResult]]]] =
    for {
      allPeers <- cluster.getPeerInfo
      peers = allPeers.filter { case (_, peerData) => canActAsJoiningSource(peerData.peerMetadata.nodeState) }
      // assumption and check before starting reconciliation is that there should be no rounds in progress
      // if the round starts and reconciliation is in progress, reconciliation should be canceled and result discarded
      ownClusterState = allPeers.mapValues(
        pd =>
          NodeReconciliationData(
            pd.peerMetadata.id,
            pd.peerMetadata.nodeState,
            none[Set[RoundId]],
            pd.majorityHeight.head.joined // is that correct, head???
          )
      )
      responses <- peers.toList.traverse {
        case (id, peerData) =>
          PeerResponse
            .run(
              apiClient.healthcheck.getClusterState(),
              unboundedBlocker
            )(peerData.peerMetadata.toPeerClientMetadata.copy(port = "9003"))
            .map(_.some)
            .handleErrorWith { e =>
              logger.debug(e)(s"Error during fetching clusterState from id=$id") >>
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

  def apply[F[_]: Concurrent: ContextShift: Clock: Timer](ownId: Id, cluster: Cluster[F], startedAtSecond: Long)(
    apiClient: ClientInterpreter[F],
    unboundedBlocker: Blocker
  ): ReconciliationRound[F] = new ReconciliationRound(ownId, cluster, startedAtSecond)(apiClient, unboundedBlocker)

  // should we have a check if the cluster inconsistency isn't coming from one node only? then run consensus for that node, and not all nodes it sees inconsistently (to avoid consensuses blast)?
  def calculateClusterAlignment(peersClusterState: Map[Id, ClusterState]): Map[Id, List[ClusterAlignmentResult]] = {
    val allPeers = peersClusterState.values.flatMap(_.keySet).toSet

    val peersWithObservations = allPeers.map { checkedId =>
      checkedId -> peersClusterState.mapValues(cs => cs.get(checkedId))
    }.toMap

    val peersWithAlignmentResult = peersWithObservations.map {
      // what about joining height, we can use it to assert on the status also
      case (checkedId, peersObservations) =>
        val notPresentOnAllNodes: Option[NodeNotPresentOnAllNodes] = notPresentOnAllNodesCheck(peersObservations)
        val inconsistentStatus: Option[NodeInconsistentlySeenAsOnlineOrOffline] =
          inconsistentStatusCheck(peersObservations)

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
    peersObservations: Map[Id, Option[NodeReconciliationData]]
  ): Option[NodeNotPresentOnAllNodes] = {
    val (peersThatSeeId, peersThatDontSeeId) =
      peersObservations.partition { case (_, maybeData) => maybeData.nonEmpty }

    if (peersThatDontSeeId.nonEmpty && (
          !peersThatSeeId.values.forall(_.exists(nrd => isInitiallyJoining(nrd.nodeState))) ||
          !peersThatSeeId.values.forall(_.exists(nrd => isInvalidForJoining(nrd.nodeState))) ||
          !peersThatSeeId.values.forall(_.exists(_.roundInProgress.nonEmpty)) ||
          !peersThatDontSeeId.values.forall(_.exists(_.roundInProgress.nonEmpty))
        ))
      NodeNotPresentOnAllNodes(peersThatSeeId.keySet, peersThatDontSeeId.keySet).some
    else
      None
  }

  /**
    * Checks if the node is consistently perceived as online or offline. If it's not it asserts that's not
    * caused by healtcheck consensus still running on some of the nodes.
    */
  def inconsistentStatusCheck(
    peersObservations: Map[Id, Option[NodeReconciliationData]]
  ): Option[NodeInconsistentlySeenAsOnlineOrOffline] = {
    val (peersThatSeeCheckedIdAsOnline, peersThatSeeCheckedIdAsOffline) = //should I collect the Option[ReconciliationData] and work on these only?
      peersObservations.partition {
        case (_, maybeData) => maybeData.exists(nrd => !isInvalidForJoining(nrd.nodeState))
      }

    if (peersThatSeeCheckedIdAsOffline.nonEmpty &&
        peersThatSeeCheckedIdAsOnline.nonEmpty && (
          !peersThatSeeCheckedIdAsOffline.values.forall(_.exists(_.roundInProgress.nonEmpty)) ||
          !peersThatSeeCheckedIdAsOnline.values.forall(_.exists(_.roundInProgress.nonEmpty))
        ))
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

  case class NodeReconciliationData(
    id: Id,
    nodeState: NodeState,
    roundInProgress: Option[Set[RoundId]],
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
