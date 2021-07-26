package org.constellation.domain.redownload

import cats.data.{EitherT, NonEmptyList}
import cats.effect.{Blocker, Concurrent, ContextShift, Timer}
import cats.syntax.all._
import cats.{Applicative, NonEmptyParallel}
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.circe.{Decoder, Encoder}
import org.constellation.ConfigUtil
import org.constellation.checkpoint.{CheckpointService, TopologicalSort}
import org.constellation.collection.MapUtils._
import org.constellation.concurrency.cuckoo.CuckooFilter
import org.constellation.domain.checkpointBlock.CheckpointStorageAlgebra
import org.constellation.domain.cloud.CloudService.CloudServiceEnqueue
import org.constellation.domain.healthcheck.HealthCheckLoggingHelper.logIds
import org.constellation.domain.cluster.{BroadcastService, ClusterStorageAlgebra, NodeStorageAlgebra}
import org.constellation.domain.redownload.RedownloadService._
import org.constellation.domain.snapshot.SnapshotStorageAlgebra
import org.constellation.domain.storage.LocalFileStorage
import org.constellation.infrastructure.p2p.PeerResponse.PeerClientMetadata
import org.constellation.infrastructure.p2p.{ClientInterpreter, PeerResponse}
import org.constellation.p2p.{Cluster, MajorityHeight, PeerData}
import org.constellation.rewards.RewardsManager
import org.constellation.schema.signature.Signed
import org.constellation.schema.snapshot._
import org.constellation.schema.{Id, NodeState}
import org.constellation.serialization.KryoSerializer
import org.constellation.storage.{JoinActivePoolCommand, SnapshotService}
import org.constellation.util.Logging.stringifyStackTrace
import org.constellation.util.Metrics

import scala.collection.SortedMap
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.math.{max, min}
import scala.util.Random

class RedownloadService[F[_]: NonEmptyParallel: Applicative](
  redownloadStorage: RedownloadStorageAlgebra[F],
  nodeStorage: NodeStorageAlgebra[F],
  clusterStorage: ClusterStorageAlgebra[F],
  majorityStateChooser: MajorityStateChooser,
  missingProposalFinder: MissingProposalFinder,
  snapshotStorage: LocalFileStorage[F, StoredSnapshot],
  snapshotInfoStorage: LocalFileStorage[F, SnapshotInfo],
  snapshotService: SnapshotService[F],
  snapshotServiceStorage: SnapshotStorageAlgebra[F],
  cloudService: CloudServiceEnqueue[F],
  checkpointService: CheckpointService[F],
  checkpointStorage: CheckpointStorageAlgebra[F],
  rewardsManager: RewardsManager[F],
  apiClient: ClientInterpreter[F],
  broadcastService: BroadcastService[F],
  nodeId: Id,
  metrics: Metrics,
  boundedExecutionContext: ExecutionContext,
  unboundedBlocker: Blocker
)(implicit F: Concurrent[F], C: ContextShift[F], T: Timer[F]) {

  private val logger = Slf4jLogger.getLogger[F]

  private val redownloadInterval =
    ConfigUtil.getOrElse("constellation.snapshot.snapshotHeightRedownloadDelayInterval", 40)
  private val heightInterval = ConfigUtil.getOrElse("constellation.snapshot.snapshotHeightInterval", 2L)
  private val meaningfulSnapshotsCount = ConfigUtil.getOrElse("constellation.snapshot.meaningfulSnapshotsCount", 80L)
  private val stallCountThreshold = ConfigUtil.getOrElse("constellation.snapshot.stallCount", 4L)
  private val proposalLookupLimit = ConfigUtil.getOrElse("constellation.snapshot.proposalLookupLimit", 6L)

  private val snapshotHeightInterval: Int = ConfigUtil.constellation.getInt("snapshot.snapshotHeightInterval")
  private val activePeersRotationInterval: Int = ConfigUtil.constellation.getInt("snapshot.activePeersRotationInterval")
  private val activePeersRotationEveryNHeights: Int = snapshotHeightInterval * activePeersRotationInterval

  def clear: F[Unit] =
    for {
      _ <- redownloadStorage.clear()
      _ <- rewardsManager.clearLastRewardedHeight()
    } yield ()

  // TODO?
  def fetchAndUpdatePeersProposals(activeFullNodes: Option[List[PeerData]] = None): F[Unit] =
    for {
      _ <- logger.debug("Fetching and updating peer proposals")
      //peers <- clusterStorage.getJoinedPeers.map(_.values.toList)
      //TODO: should joining height be checked here after introducing joining pool?
      peers <- {
        activeFullNodes match {
          case Some(activePeers) => activePeers.pure[F]
          case None =>
            clusterStorage.getActiveFullPeers
              .map(
                _.values.toList.filter(p => NodeState.isNotOffline(p.peerMetadata.nodeState))
              )
        }
      }
      apiClients = peers.map(_.peerMetadata.toPeerClientMetadata)
      responses <- apiClients.traverse { client =>
        fetchCreatedSnapshots(client).map(client.id -> _)
      }

      _ <- responses.traverse {
        case (_, proposals) => redownloadStorage.persistPeerProposals(proposals.values)
      }
    } yield ()

  private def fetchCreatedSnapshots(client: PeerClientMetadata): F[SnapshotProposalsAtHeight] =
    PeerResponse
      .run(
        apiClient.snapshot
          .getCreatedSnapshots(),
        unboundedBlocker
      )(client)
      .handleErrorWith { e =>
        logger.error(e)(s"Fetch peers proposals error") >> F.pure(Map.empty[Long, Signed[SnapshotProposal]])
      }

  private[redownload] def fetchStoredSnapshotsFromAllPeers(
    lastActivePeers: Option[List[PeerData]] = None
  ): F[Map[PeerClientMetadata, Set[String]]] =
    for {
      peers <- lastActivePeers match {
        case Some(lastActive) => lastActive.pure[F]
        case None             => clusterStorage.getActiveFullPeers().map(_.values.toList)
      }
      apiClients = peers.map(_.peerMetadata.toPeerClientMetadata)
      responses <- apiClients.traverse { client =>
        fetchStoredSnapshots(client)
          .map(client -> _.toSet)
      }
    } yield responses.toMap

  // TODO: Extract to HTTP layer
  private[redownload] def fetchAcceptedSnapshots(client: PeerClientMetadata): F[SnapshotsAtHeight] =
    PeerResponse
      .run(
        apiClient.snapshot
          .getAcceptedSnapshots(),
        unboundedBlocker
      )(client)
      .handleErrorWith(_ => F.pure(Map.empty))

  // TODO: Extract to HTTP layer
  private def fetchStoredSnapshots(client: PeerClientMetadata): F[List[String]] =
    PeerResponse
      .run(
        apiClient.snapshot
          .getStoredSnapshots(),
        unboundedBlocker
      )(client)
      .handleErrorWith(_ => F.pure(List.empty))

  // TODO: Extract to HTTP layer
  private[redownload] def fetchSnapshotInfo(client: PeerClientMetadata): F[SnapshotInfoSerialized] =
    PeerResponse
      .run(
        apiClient.snapshot
          .getSnapshotInfo(),
        unboundedBlocker
      )(client)

  // TODO: Extract to HTTP layer
  private def fetchSnapshotInfo(hash: String)(client: PeerClientMetadata): F[SnapshotInfoSerialized] =
    PeerResponse
      .run(
        apiClient.snapshot
          .getSnapshotInfo(hash),
        unboundedBlocker
      )(client)

  // TODO: Extract to HTTP layer
  private[redownload] def fetchSnapshot(hash: String)(client: PeerClientMetadata): F[SnapshotSerialized] =
    PeerResponse
      .run(
        apiClient.snapshot
          .getStoredSnapshot(hash),
        unboundedBlocker
      )(client)

  private[redownload] def fetchPeerProposals(
    query: List[ProposalCoordinate],
    client: PeerClientMetadata
  ): F[List[Option[Signed[SnapshotProposal]]]] =
    PeerResponse
      .run(
        apiClient.snapshot
          .queryPeerProposals(query),
        unboundedBlocker
      )(client)

  private def fetchAndStoreMissingSnapshots(
    snapshotsToDownload: SnapshotsAtHeight,
    lastActivePeers: Option[List[PeerData]] = None
  ): EitherT[F, Throwable, Unit] =
    for {
      storedSnapshots <- fetchStoredSnapshotsFromAllPeers(lastActivePeers).attemptT
      candidates = snapshotsToDownload.values.toList.map { hash =>
        (hash, storedSnapshots.filter { case (_, hashes) => hashes.contains(hash) }.keySet)
      }.toMap

      missingSnapshots <- candidates.toList.traverse {
        case (hash, pool) =>
          useRandomClient(pool) { client =>
            (fetchSnapshot(hash)(client), fetchSnapshotInfo(hash)(client)).parMapN { (snapshot, snapshotInfo) =>
              (hash, (snapshot, snapshotInfo))
            }
          }
      }.attemptT

      _ <- missingSnapshots.traverse {
        case (hash, (snapshot, snapshotInfo)) =>
          snapshotStorage.write(hash, snapshot) >> snapshotInfoStorage.write(hash, snapshotInfo)
      }.void
    } yield ()

  private[redownload] def useRandomClient[A](pool: Set[PeerClientMetadata], attempts: Int = 3)(
    fetchFn: PeerClientMetadata => F[A]
  ): F[A] =
    if (pool.isEmpty) {
      F.raiseError[A](new Throwable("Client pool is empty!"))
    } else {
      val poolArray = pool.toArray
      val stopAt = Random.nextInt(poolArray.length)

      def makeAttempt(index: Int, initialDelay: FiniteDuration = 0.6.seconds, errorsSoFar: Int = 0): F[A] = {
        val client = poolArray(index)
        fetchFn(client).handleErrorWith {
          case e if index == stopAt         => F.raiseError[A](e)
          case _ if errorsSoFar >= attempts => makeAttempt((index + 1) % poolArray.length)
          case _                            => T.sleep(initialDelay) >> makeAttempt(index, initialDelay * 2, errorsSoFar + 1)
        }
      }

      makeAttempt((stopAt + 1) % poolArray.length)
    }

  private def fetchAndPersistBlocksAboveMajority(
    majorityState: SnapshotsAtHeight,
    lastActivePeers: Option[List[PeerData]] = None
  ): EitherT[F, Throwable, Unit] =
    for {
      storedSnapshots <- fetchStoredSnapshotsFromAllPeers(lastActivePeers).attemptT

      maxMajorityHash = majorityState(redownloadStorage.maxHeight(majorityState))
      peersWithMajority = storedSnapshots.filter { case (_, hashes) => hashes.contains(maxMajorityHash) }.keySet

      majoritySnapshotInfo <- snapshotInfoStorage.read(maxMajorityHash)

      _ <- useRandomClient(peersWithMajority) { client =>
        for {
          accepted <- fetchAcceptedSnapshots(client)

          maxMajorityHeight = redownloadStorage.maxHeight(majorityState)
          acceptedSnapshotHashesAboveMajority = takeHighestUntilKey(accepted, maxMajorityHeight).values

          acceptedSnapshots <- acceptedSnapshotHashesAboveMajority.toList
            .traverse(hash => fetchSnapshot(hash)(client))
            .flatMap(
              snapshotsSerialized =>
                snapshotsSerialized.traverse { s =>
                  C.evalOn(boundedExecutionContext)(F.delay { KryoSerializer.deserializeCast[StoredSnapshot](s) })
                }
            )

          _ <- snapshotService.setSnapshot(majoritySnapshotInfo)

          blocksToAccept = acceptedSnapshots.flatMap(_.checkpointCache)

          sorted <- C
            .evalOn(boundedExecutionContext)(F.delay {
              TopologicalSort.sortBlocksTopologically(blocksToAccept)
            })
            .map(_.toList)

          _ <- sorted.traverse { block =>
            checkpointService.addToAcceptance(block)
          }

//          snapshotInfoFromMemPool <- fetchSnapshotInfo(client).flatMap { snapshotInfo =>
//            C.evalOn(boundedExecutionContext)(F.delay {
//              KryoSerializer.deserializeCast[SnapshotInfo](snapshotInfo)
//            })
//          }
//
//          _ <- checkpointStorage.setTips(snapshotInfoFromMemPool.tips)
//          _ <- checkpointStorage.setUsages(snapshotInfoFromMemPool.usages)
        } yield ()
      }.attemptT
    } yield ()

  def checkForAlignmentWithMajoritySnapshot(
    joiningHeight: Option[Long] = None,
    isJoiningActivePool: Boolean = false,
    lastActivePeers: Option[List[PeerData]] = None
  ): F[Unit] = {
    def wrappedRedownload(
      shouldRedownload: Boolean,
      meaningfulAcceptedSnapshots: SnapshotsAtHeight,
      meaningfulMajorityState: SnapshotsAtHeight
    ): F[Unit] =
      for {
        _ <- if (shouldRedownload) {
          val plan = calculateRedownloadPlan(meaningfulAcceptedSnapshots, meaningfulMajorityState)
          val result = getAlignmentResult(meaningfulAcceptedSnapshots, meaningfulMajorityState, redownloadInterval)
          for {
            _ <- logger.debug(s"Alignment result: $result")
            _ <- logger.debug("Redownload needed! Applying the following redownload plan:")
            _ <- applyPlan(plan, meaningfulMajorityState, lastActivePeers).value.flatMap(F.fromEither)
            _ <- metrics.updateMetricAsync[F]("redownload_hasLastRedownloadFailed", 0)
          } yield ()
        }.handleErrorWith { error =>
          metrics.updateMetricAsync[F]("redownload_hasLastRedownloadFailed", 1) >>
            metrics.incrementMetricAsync("redownload_redownloadFailures_total") >>
            error.raiseError[F, Unit]
        } else logger.debug("No redownload needed - snapshots have been already aligned with majority state. ")
      } yield ()

    val wrappedCheck = for {
      _ <- logger.debug("Checking alignment with majority snapshot...")

      lastMajority <- redownloadStorage.getLastMajorityState
      (_, createdSnapshots, peerProposals) <- redownloadStorage.removeSnapshotsAndProposalsBelowHeight(
        lastMajority.minHeight
      )

      peers <- lastActivePeers match {
        case Some(lastActivePeers) => lastActivePeers.map(pd => pd.peerMetadata.id -> pd).toMap.pure[F]
        case None                  => clusterStorage.getActiveFullPeers()
      }
      //TODO: or should this be in clusterStorage?
      activeBetweenHeights <- clusterStorage.getActiveBetweenHeights //nodeStorage.getActiveBetweenHeights
      peersCache = peers.map {
        case (id, peerData) => (id, peerData.majorityHeight)
      } ++ Map(nodeId -> NonEmptyList.one(activeBetweenHeights))

      _ <- logger.debug(s"Peers with majority heights $peersCache")

      calculatedMajority = majorityStateChooser.chooseMajorityState(
        createdSnapshots,
        peerProposals,
        peersCache
      )

      gaps = missingProposalFinder.findGaps(calculatedMajority)
      _ <- if (gaps.isEmpty) {
        logger.debug(
          s"Majority calculated in range ${calculatedMajority.heightRange} has no gaps"
        )
      } else {
        logger.warn(
          s"Majority calculated in range ${calculatedMajority.heightRange} has following gaps $gaps"
        )
      }

      calculatedMajorityWithoutGaps = if (gaps.isEmpty) calculatedMajority
      else calculatedMajority.removeHeightsAbove(gaps.min)

      //TODO: reverify I'm not sure how joining height and activeBetweenHeights should interact here
      joinHeight = joiningHeight.getOrElse(
        activeBetweenHeights.joined.getOrElse(calculatedMajorityWithoutGaps.maxHeight)
      )

      _ <- if (joiningHeight.nonEmpty) logger.debug(s"Join height is ${joinHeight}") else F.unit

      majorityBeforeCutOff = {
        val intersect = lastMajority.keySet & calculatedMajorityWithoutGaps.keySet
        val continuation = {
          val maybeLastMax = lastMajority.keySet.toList.maximumOption
          val maybeNewMin = calculatedMajorityWithoutGaps.keySet.toList.minimumOption

          (maybeLastMax, maybeNewMin) match {
            case (Some(lastMax), Some(newMin)) => newMin - lastMax == heightInterval
            case _                             => false
          }
        }
        if (lastMajority.isEmpty || intersect.nonEmpty || continuation)
          calculatedMajorityWithoutGaps
        else
          lastMajority
      }

      cutOffHeight = getCutOffHeight(joinHeight, calculatedMajorityWithoutGaps, lastMajority)

      _ <- if (joiningHeight.nonEmpty) logger.debug(s"cutOffHeight=${cutOffHeight}") else F.unit

      meaningfulMajority = majorityBeforeCutOff.removeHeightsBelow(cutOffHeight)
      _ <- redownloadStorage.setLastMajorityState(meaningfulMajority) >>
        metrics.updateMetricAsync("redownload_lastMajorityStateHeight", meaningfulMajority.maxHeight)

      _ <- logger.debug(s"cutOffHeight = $cutOffHeight")

      (meaningfulAcceptedSnapshots, _, _) <- redownloadStorage.removeSnapshotsAndProposalsBelowHeight(cutOffHeight)

      _ <- logger.debug(s"Meaningful majority in range ${calculatedMajority.heightRange}")

      _ <- F
        .pure(meaningfulMajority.maxHeight > lastMajority.maxHeight)
        .ifM(redownloadStorage.resetMajorityStallCount, redownloadStorage.incrementMajorityStallCount)

      _ <- if (meaningfulMajority.isEmpty) logger.debug("Meaningful majority is empty - skipping redownload")
      else F.unit

      shouldPerformRedownload = shouldRedownload(
        meaningfulAcceptedSnapshots,
        meaningfulMajority,
        redownloadInterval,
        joiningHeight.nonEmpty || isJoiningActivePool
      )

      _ <- if (shouldPerformRedownload) {
        {
          if (joiningHeight.nonEmpty)
            broadcastService.compareAndSet(NodeState.validForDownload, NodeState.DownloadInProgress)
          else
            broadcastService.compareAndSet(NodeState.validForRedownload, NodeState.DownloadInProgress)
        }.flatMap { state =>
          if (state.isNewSet) {
            wrappedRedownload(shouldPerformRedownload, meaningfulAcceptedSnapshots, meaningfulMajority).handleErrorWith {
              error =>
                logger.error(error)(
                  s"Redownload error, isDownload=${joiningHeight.nonEmpty}: ${stringifyStackTrace(error)}"
                ) >> {
                  if (joiningHeight.nonEmpty)
                    error.raiseError[F, Unit]
                  else
                    broadcastService
                      .compareAndSet(NodeState.validDuringDownload, NodeState.Ready)
                      .void
                }
            }
          } else
            logger.debug(
              s"Setting node state during redownload failed! Skipping redownload. isDownload=${joiningHeight.nonEmpty}"
            )
        }
      } else logger.debug("No redownload needed - snapshots have been already aligned with majority state.")

      _ <- logger.debug("Sending majority snapshots to cloud.")
      _ <- if (meaningfulMajority.nonEmpty) sendMajoritySnapshotsToCloud()
      else logger.debug("No majority - skipping sending to cloud")

      _ <- logger.debug("Rewarding majority snapshots.")
      _ <- if (meaningfulMajority.nonEmpty) rewardMajoritySnapshots().value.flatMap(F.fromEither)
      else logger.debug("No majority - skipping rewarding")

      _ <- logger.debug("Removing unaccepted snapshots from disk.")
      _ <- removeUnacceptedSnapshotsFromDisk().value.flatMap(F.fromEither)

      lowestMajorityHeight <- redownloadStorage.getLowestMajorityHeight

      checkpointsToRemove <- checkpointStorage.getInSnapshot
        .map(_.filter {
          case (_, height) => height < (lowestMajorityHeight - 2)
        })
        .map(_.map(_._1))

      _ <- logger.debug(
        s"Removing checkpoints below height: ${lowestMajorityHeight - 2}. To remove: ${checkpointsToRemove.size}"
      )
      _ <- checkpointStorage.removeCheckpoints(checkpointsToRemove)

      _ <- if (shouldPerformRedownload && joiningHeight.isEmpty) { // I think we should only set it to Ready when we are not joining
        broadcastService.compareAndSet(NodeState.validDuringDownload, NodeState.Ready)
      } else F.unit

      _ <- logger.debug("Accepting all the checkpoint blocks received during the redownload.")
      _ <- acceptCheckpointBlocks().value.flatMap(F.fromEither)

      _ <- if (!shouldPerformRedownload) findAndFetchMissingProposals(peerProposals, peersCache) else F.unit
      _ <- logger.debug(
        s"activeBetweenHeights: $activeBetweenHeights maxMajorityHeight: ${meaningfulMajority.maxHeight}"
      )
      _ <- activeBetweenHeights.left
        .exists(_ <= meaningfulMajority.maxHeight)
        .pure[F]
        .ifM(
          notifyNewActivePeersAndLeaveThePool(),
          logger.debug(s"Still in the active window!")
        )
    } yield ()

    nodeStorage.getNodeState.map { current =>
      if (joiningHeight.nonEmpty) NodeState.validForDownload.contains(current)
      else NodeState.validForRedownload.contains(current)
    }.flatMap(isValidForRedownload => clusterStorage.isAnActiveFullPeer.map(_ && isValidForRedownload))
      .map(_ || isJoiningActivePool) // TODO: Improve - different logs for every reason wrappedCheck won't run
      .ifM(
        wrappedCheck,
        logger
          .debug(s"Node state is not valid for redownload, skipping. isDownload=${joiningHeight.nonEmpty}") >> F.unit
      )
  }.handleErrorWith { error =>
    logger.error(error)("Error during checking alignment with majority snapshot.") >>
      error.raiseError[F, Unit]
  }

  // TODO: If node is a full node it shouldn't handle any requests here
  private def addAndCheckJoinActivePoolCommand(senderId: Id, joinActivePoolCommand: JoinActivePoolCommand): F[Unit] =
    for {
      _ <- clusterStorage.isAnActiveFullPeer.ifM(
        F.raiseError[Unit](new Throwable(s"Full peers don't process join active pool requests.")),
        F.unit
      )
      currentRequests <- redownloadStorage.addJoinActivePoolCommand(senderId, joinActivePoolCommand)
      fullNodes <- clusterStorage.getActiveFullPeers()
      _ <- logger.debug(s"Full nodes when joining command processed: ${fullNodes.keySet}")
      _ <- clusterStorage.isAnActiveLightPeer.ifM(
        if (currentRequests.values.toList.distinct.size == 1 && currentRequests.keySet == fullNodes.keySet) {
          redownloadStorage.clearJoinActivePoolCommands() >>
            logger.debug(
              s"Added joinActivePool request and check if all previous full nodes sent the notification was met!"
            )
        } else
          F.raiseError[Unit](
            new Throwable(
              s"Added joinActivePool request but not all previous full nodes sent the notification. request=$joinActivePoolCommand! currentRequests size = ${currentRequests.keySet} != $fullNodes."
            )
          ),
        if (currentRequests.values.toList.distinct.size == 1 && currentRequests.size == 3)
          redownloadStorage.clearJoinActivePoolCommands() >>
            logger.debug(s"Added joinActivePool request and the condition to join active pool was met!")
        else
          F.raiseError[Unit](
            new Throwable(
              s"Added joinActivePool request but the amount of needed requests wasn't met. request=$joinActivePoolCommand! currentRequests size = ${currentRequests.size} != 3."
            )
          )
      )
    } yield ()

  def redownloadBeforeJoiningActivePeersPool(senderId: Id, joinActivePoolCommand: JoinActivePoolCommand): F[Unit] = {
    for {
      _ <- addAndCheckJoinActivePoolCommand(senderId, joinActivePoolCommand)
      JoinActivePoolCommand(lastActiveFullNodes, lastActiveBetweenHeight) = joinActivePoolCommand
      _ <- logger.debug(s"Joining active peers pool! lastActivePeers: ${logIds(lastActiveFullNodes)}")
      _ <- logger.debug(s"Joining active peers pool! lastActiveBetweenHeight: $lastActiveBetweenHeight")
      peers <- lastActiveFullNodes.toList
        .traverse(
          clusterStorage.getPeer
        )
        .map(_.flatten.map(_.copy(majorityHeight = NonEmptyList.one(lastActiveBetweenHeight)))) // TODO: should we handle the situation when at least one of the last active peers is not found?
      _ <- logger.debug(s"Joining active peers pool! peers=$peers")
      _ <- clear
      _ <- fetchAndUpdatePeersProposals(peers.some)
      _ <- checkForAlignmentWithMajoritySnapshot(isJoiningActivePool = true, lastActivePeers = peers.some)
      latestMajorityHeight <- redownloadStorage.getLatestMajorityHeight
      _ <- logger.debug(s"Joining active peers pool! latestMajorityHeight: $latestMajorityHeight")
      // TODO: this was in snapshotService
      newActivePeers <- snapshotServiceStorage.getNextSnapshotFacilitators
      _ <- logger.debug(s"Joining active peers pool! newActivePeers: $newActivePeers") //TODO: logging function
      activeBetweenHeights = Cluster.calculateActiveBetweenHeights(
        latestMajorityHeight,
        activePeersRotationEveryNHeights
      )
      _ <- clusterStorage.setActivePeers(newActivePeers, activeBetweenHeights, metrics)
    } yield ()
  }.handleErrorWith { e =>
    logger.error(e)("Error during joining active peers pool!")
  }

  // TODO: notify new light nodes to join the pool also and update your light nodes list
  // TODO: we need a consensus over new light and full nodes between current active full nodes
  private def notifyNewActivePeersAndLeaveThePool(): F[Unit] =
    for {
      _ <- logger.debug(s"Notifying new active peers to join the active pool!")
      lastActiveFullNodes <- clusterStorage.getActiveFullPeers().map(_.keySet + nodeId)
      lastActiveLightNodes <- clusterStorage.getActiveLightPeers().map(_.keySet)
      lastActiveBetweenHeights <- clusterStorage.getActiveBetweenHeights
      joinActivePoolCommand = JoinActivePoolCommand(lastActiveFullNodes, lastActiveBetweenHeights)
      nextActiveNodes <- snapshotServiceStorage.getStoredSnapshot.map(_.snapshot.nextActiveNodes)
      _ <- logger.debug(s"LastActiveFullNodes: ${logIds(lastActiveFullNodes)}")
      _ <- logger.debug(s"NextActiveFullNodes: ${logIds(nextActiveNodes.full)}")
      _ <- logger.debug(s"LastActiveLightNodes: ${logIds(lastActiveLightNodes)}")
      _ <- logger.debug(s"NextActiveLightNodes: ${logIds(nextActiveNodes.light)}")
      //TODO: what about missing peers
      peersToNotify <- (nextActiveNodes.full ++ nextActiveNodes.light ++ lastActiveLightNodes -- lastActiveFullNodes).toList
        .traverse(clusterStorage.getPeer)
        .map(_.flatten)
        .map(_.map(_.peerMetadata.toPeerClientMetadata))

      _ <- peersToNotify.traverse { peerClientMetadata =>
        // notify the peers or if everybody does redownload then it's not needed, maybe send the last snapshot
        // to the nodes joining the L0 pool explicitly first
        apiClient.snapshot.notifyNextActivePeer(joinActivePoolCommand)(peerClientMetadata)
      }
      _ <- logger.debug(s"Setting next active peers!")
      latestMajorityHeight <- redownloadStorage.getLatestMajorityHeight
      activeBetweenHeights = Cluster
        .calculateActiveBetweenHeights(latestMajorityHeight, activePeersRotationEveryNHeights)
      _ <- clusterStorage
        .setActivePeers(nextActiveNodes, activeBetweenHeights, metrics) // TODO: or nextActivePeers but then all nodes should redownload
      _ <- nextActiveNodes.full
        .contains(nodeId)
        .pure[F]
        .ifM(
          F.unit,
          logger.debug("Leaving active peers pool!")
        )
    } yield ()

  def removeUnacceptedSnapshotsFromDisk(): EitherT[F, Throwable, Unit] =
    for {
      nextSnapshotHash <- snapshotServiceStorage.getNextSnapshotHash.attemptT
      stored <- snapshotStorage.list().map(_.toSet)
      accepted <- redownloadStorage.getAcceptedSnapshots.attemptT.map(_.values.toSet)
      created <- redownloadStorage.getCreatedSnapshots.attemptT.map(_.values.map(_.value.hash).toSet)
      diff = stored.diff(accepted ++ created ++ Set(nextSnapshotHash))
      sentToCloud <- cloudService.getAlreadySent().map(_.map(_.hash)).attemptT
      toRemove = diff.intersect(sentToCloud).toList
      _ <- toRemove.traverse { hash =>
        snapshotStorage.delete(hash) >>
          snapshotInfoStorage.delete(hash) >>
          cloudService.removeSentSnapshot(hash).attemptT
      }
    } yield ()

  private def getCutOffHeight(
    joinHeight: Long,
    candidateMajority: SnapshotsAtHeight,
    lastMajority: SnapshotsAtHeight
  ): Long = {
    val ignorePoint = getIgnorePoint(candidateMajority.maxHeight)
    max(joinHeight - heightInterval, min(max(ignorePoint, joinHeight), lastMajority.maxHeight))
  }

  def getIgnorePoint(maxHeight: Long): Long = maxHeight - meaningfulSnapshotsCount

  private[redownload] def getLookupRange: F[HeightRange] =
    for {
      stallCount <- redownloadStorage.getMajorityStallCount
      majorityHeight <- redownloadStorage.getLatestMajorityHeight
      result = if (stallCount > stallCountThreshold)
        HeightRange(majorityHeight + heightInterval, majorityHeight + proposalLookupLimit)
      else HeightRange.Empty
    } yield result

  private def findAndFetchMissingProposals(peersProposals: PeersProposals, peersCache: PeersCache): F[Unit] =
    for {
      lookupRange <- getLookupRange
      _ <- lookupRange.empty
        .pure[F]
        .ifM(
          logger.info("Skip looking for missing proposals"), {
            val missingProposals =
              missingProposalFinder.findMissingPeerProposals(lookupRange, peersProposals, peersCache)
            missingProposals.nonEmpty
              .pure[F]
              .ifM(
                logger.info(s"Missing proposals found in range $lookupRange, $missingProposals") >>
                  queryMissingProposals(missingProposals),
                logger.info(s"No missing proposals found")
              ) >> updateMissingProposalsMetric(missingProposals)
          }
        )

    } yield ()

  def updateMissingProposalsMetric(coordinates: Set[ProposalCoordinate]): F[Unit] =
    coordinates
      .groupBy(_._1)
      .toList
      .traverse {
        case (id, coordinates) =>
          coordinates.map(_._2).toList.minimumOption.traverse { height =>
            metrics.updateMetricAsync(
              "redownload_lowestMissingSnapshotProposalHeight",
              height,
              Seq(("peerId", id.hex))
            )
          }
      }
      .void

  private def queryMissingProposals(coordinates: Set[ProposalCoordinate]): F[Unit] =
    for {
      remoteFilters <- shuffledRemoteFilterList
      peerToQuery = selectPeersForQueries(coordinates, remoteFilters)
      _ <- logger.info(s"Querying missing proposals from peers $peerToQuery")
      _ <- peerToQuery.toList.traverse {
        case (peerId, query) => fetchAndUpdatePeerProposals(query, peerId)
      }
    } yield ()

  private def shuffledRemoteFilterList: F[List[(Id, CuckooFilter)]] =
    for {
      map <- redownloadStorage.getRemoteFilters
      result <- F.delay {
        Random.shuffle(map.toList)
      }
    } yield result

  private def selectPeersForQueries(
    missingProposals: Set[ProposalCoordinate],
    remoteFilters: List[(Id, CuckooFilter)]
  ): Map[Id, Set[ProposalCoordinate]] =
    missingProposals
      .foldLeft(Map.empty[Id, Set[ProposalCoordinate]]) { (acc, proposalCoordinate) =>
        remoteFilters.find {
          case (_, f) => f.contains(proposalCoordinate)
        }.map {
          case (id, _) => acc |+| Map(id -> Set(proposalCoordinate))
        }.getOrElse(acc)
      }

  private def fetchAndUpdatePeerProposals(query: Set[ProposalCoordinate], peerId: Id): F[Unit] =
    for {
      peer <- clusterStorage.getPeers.map(_.get(peerId))
      apiClient = peer.map(_.peerMetadata.toPeerClientMetadata)
      _ <- apiClient.traverse { client =>
        for {
          proposals <- fetchPeerProposals(query.toList, client)
          filteredProposals = proposals
            .mapFilter(identity)
            .filter(_.validSignature)
          _ <- redownloadStorage.persistPeerProposals(filteredProposals)
        } yield ()
      }
    } yield ()

  private[redownload] def sendMajoritySnapshotsToCloud(): F[Unit] = {
    val send = for {
      majorityState <- redownloadStorage.getLastMajorityState
      accepted <- redownloadStorage.getAcceptedSnapshots
      alreadySent <- cloudService.getAlreadySent().map(_.map(_.height))

      maxAccepted = accepted.keySet.toList.maximumOption.getOrElse(0L)
      toSend = majorityState.filterKeys(_ <= maxAccepted) -- alreadySent

      toSendInOrder = toSend.toList.sortBy { case (height, _) => height }

      uploadSnapshots <- toSendInOrder.traverse {
        case (height, hash) =>
          snapshotStorage
            .getFile(hash)
            .rethrowT
            .map {
              cloudService.enqueueSnapshot(_, height, hash)
            }
            .handleErrorWith(
              err => logger.error(err)(s"Cannot enqueue snapshot, height: ${height}, hash: ${hash}").pure[F]
            )
      }

      uploadSnapshotInfos <- toSendInOrder.traverse {
        case (height, hash) =>
          snapshotInfoStorage
            .getFile(hash)
            .rethrowT
            .map {
              cloudService.enqueueSnapshotInfo(_, height, hash)
            }
            .handleErrorWith(
              err => logger.error(err)(s"Cannot enqueue snapshot info, height: ${height}, hash: ${hash}").pure[F]
            )
      }

      // For now we do not restore EigenTrust model
      //      uploadRewards <- hashes.traverse {
      //        case (height, hash) =>
      //          rewardsStorage.getFile(hash).rethrowT.map {
      //            rewardsCloudStorage.write(height, hash, _).rethrowT
      //          }
      //      }

      _ <- uploadSnapshots.sequence
      _ <- uploadSnapshotInfos.sequence
      // For now we do not restore EigenTrust model
      //      _ <- uploadRewards.sequence

      toSendHeights = toSendInOrder.map { case (height, _) => height }
      majorityHeights = majorityState.keys.toList.sorted

      _ <- logger.debug(
        s"Enqueued heights: ${toSendHeights} from majority: ${majorityHeights} where maxAccepted=${maxAccepted}."
      )

    } yield ()

    send.handleErrorWith(error => {
      logger.error(error)(s"Sending snapshot to cloud failed with") >> metrics.incrementMetricAsync(
        "sendToCloud_failure"
      )
    })
  }

  private[redownload] def rewardMajoritySnapshots(): EitherT[F, Throwable, Unit] =
    for {
      majorityState <- redownloadStorage.getLastMajorityState.attemptT
      accepted <- redownloadStorage.getAcceptedSnapshots.attemptT
      lastHeight <- rewardsManager.getLastRewardedHeight().attemptT
      maxAccepted = accepted.keySet.toList.maximumOption.getOrElse(0L)

      _ <- EitherT.liftF(logger.debug(s"Last rewarded height is $lastHeight. Rewarding majority snapshots above."))

      majoritySubsetToReward = takeHighestUntilKey(majorityState, lastHeight).filterKeys(_ <= maxAccepted)

      snapshotsToReward <- majoritySubsetToReward.toList.sortBy {
        case (height, _) => height
      }.traverse {
        case (height, hash) => snapshotStorage.read(hash).map(s => (height, s.snapshot))
      }

      _ <- snapshotsToReward.traverse {
        case (height, snapshot) => rewardsManager.attemptReward(snapshot, height)
      }.attemptT
    } yield ()

  private[redownload] def applyPlan(
    plan: RedownloadPlan,
    majorityState: SnapshotsAtHeight,
    lastActivePeers: Option[List[PeerData]] = None
  ): EitherT[F, Throwable, Unit] =
    for {
      _ <- EitherT.liftF(logRedownload(plan, majorityState))

      _ <- EitherT.liftF(logger.debug("Fetching and storing missing snapshots on disk."))
      _ <- fetchAndStoreMissingSnapshots(plan.toDownload, lastActivePeers)

      _ <- EitherT.liftF(logger.debug("Filling missing createdSnapshots by majority state"))
      _ <- redownloadStorage.updateCreatedSnapshots(plan).attemptT

      _ <- EitherT.liftF(logger.debug("Replacing acceptedSnapshots by majority state"))
      _ <- redownloadStorage.updateAcceptedSnapshots(plan).attemptT

      _ <- EitherT.liftF(logger.debug("Fetching and persisting blocks above majority."))
      _ <- fetchAndPersistBlocksAboveMajority(majorityState, lastActivePeers)

      _ <- EitherT.liftF(logger.debug("RedownloadPlan has been applied succesfully."))

      _ <- EitherT.liftF(metrics.incrementMetricAsync("reDownloadFinished_total"))
    } yield ()

  private def logRedownload(plan: RedownloadPlan, majorityState: SnapshotsAtHeight): F[Unit] =
    for {
      _ <- logger.debug(s"Majority state: ${formatSnapshots(majorityState)}")
      _ <- logger.debug(s"To download: ${formatSnapshots(plan.toDownload)}")
      _ <- logger.debug(s"To remove: ${formatSnapshots(plan.toRemove)}")
      _ <- logger.debug(s"To leave: ${formatSnapshots(plan.toLeave)}")
    } yield ()

  private def takeHighestUntilKey[K <: Long, V](data: Map[K, V], key: K): Map[K, V] =
    data.filterKeys(_ > key)

  private[redownload] def acceptCheckpointBlocks(): EitherT[F, Throwable, Unit] =
    (for {
      blocksToAccept <- checkpointStorage.getCheckpointsForAcceptanceAfterDownload

      _ <- blocksToAccept.toList.traverse { checkpointService.addToAcceptance }
    } yield ()).attemptT

//  private[redownload] def updateHighestSnapshotInfo(): EitherT[F, Throwable, Unit] =
//    for {
//      highestSnapshotInfo <- redownloadStorage.getAcceptedSnapshots.attemptT
//        .map(_.maxBy { case (height, _) => height } match { case (_, hash) => hash })
//        .flatMap(hash => snapshotInfoStorage.read(hash))
//      _ <- snapshotService.setSnapshot(highestSnapshotInfo).attemptT
//    } yield ()

  private[redownload] def shouldRedownload(
    acceptedSnapshots: SnapshotsAtHeight,
    majorityState: SnapshotsAtHeight,
    redownloadInterval: Int,
    isDownload: Boolean = false
  ): Boolean =
    if (majorityState.isEmpty) false // TODO: should it skyrocket when majority isEmpty
    else
      isDownload || (getAlignmentResult(acceptedSnapshots, majorityState, redownloadInterval) match {
        case AlignedWithMajority         => false
        case AbovePastRedownloadInterval => false
        case _                           => true
      })

  private[redownload] def calculateRedownloadPlan(
    acceptedSnapshots: SnapshotsAtHeight,
    majorityState: SnapshotsAtHeight
  ): RedownloadPlan = {
    val toDownload = majorityState.toSet.diff(acceptedSnapshots.toSet).toMap
    val toRemove = acceptedSnapshots.toSet.diff(majorityState.toSet).toMap
    val toLeave = acceptedSnapshots.toSet.diff(toRemove.toSet).toMap
    RedownloadPlan(toDownload, toRemove, toLeave)
  }

  private[redownload] def getAlignmentResult(
    acceptedSnapshots: SnapshotsAtHeight,
    majorityState: SnapshotsAtHeight,
    redownloadInterval: Int
  ): MajorityAlignmentResult = {
    val highestAcceptedHeight = redownloadStorage.maxHeight(acceptedSnapshots)
    val highestMajorityHeight = redownloadStorage.maxHeight(majorityState)

    val isAbove = highestAcceptedHeight > highestMajorityHeight
    val isBelow = highestAcceptedHeight < highestMajorityHeight
    val heightDiff = Math.abs(highestAcceptedHeight - highestMajorityHeight)
    val reachedRedownloadInterval = heightDiff >= redownloadInterval

    if (heightDiff == 0 && !majorityState.equals(acceptedSnapshots)) {
      MisalignedAtSameHeight
    } else if (isAbove && reachedRedownloadInterval) {
      AbovePastRedownloadInterval
    } else if (isAbove && !majorityState.toSet.subsetOf(acceptedSnapshots.toSet)) {
      AboveButMisalignedBeforeRedownloadIntervalAnyway
    } else if (isBelow && reachedRedownloadInterval) {
      BelowPastRedownloadInterval
    } else if (isBelow && !acceptedSnapshots.toSet.subsetOf(majorityState.toSet)) {
      BelowButMisalignedBeforeRedownloadIntervalAnyway
    } else {
      AlignedWithMajority
    }
  }

  private[redownload] def maxHeight[V](snapshots: Map[Long, V]): Long =
    if (snapshots.isEmpty) 0
    else snapshots.keySet.max

  private[redownload] def minHeight[V](snapshots: Map[Long, V]): Long =
    if (snapshots.isEmpty) 0
    else snapshots.keySet.min

  private def formatSnapshots(snapshots: SnapshotsAtHeight) =
    SortedMap[Long, String]() ++ snapshots

}

object RedownloadService {
  type Reputation = SortedMap[Id, Double]
  type ProposalCoordinate = (Id, Long)
  type SnapshotsAtHeight = Map[Long, String] // height -> hash
  type SnapshotProposalsAtHeight = Map[Long, Signed[SnapshotProposal]]
  type PeersProposals = Map[Id, SnapshotProposalsAtHeight]
  type PeersCache = Map[Id, NonEmptyList[MajorityHeight]]
  type SnapshotInfoSerialized = Array[Byte]
  type SnapshotSerialized = Array[Byte]

  def apply[F[_]: Concurrent: ContextShift: NonEmptyParallel: Timer](
    redownloadStorage: RedownloadStorageAlgebra[F],
    nodeStorage: NodeStorageAlgebra[F],
    clusterStorage: ClusterStorageAlgebra[F],
    majorityStateChooser: MajorityStateChooser,
    missingProposalFinder: MissingProposalFinder,
    snapshotStorage: LocalFileStorage[F, StoredSnapshot],
    snapshotInfoStorage: LocalFileStorage[F, SnapshotInfo],
    snapshotService: SnapshotService[F],
    snapshotServiceStorage: SnapshotStorageAlgebra[F],
    cloudService: CloudServiceEnqueue[F],
    checkpointService: CheckpointService[F],
    checkpointStorage: CheckpointStorageAlgebra[F],
    rewardsManager: RewardsManager[F],
    apiClient: ClientInterpreter[F],
    broadcastService: BroadcastService[F],
    nodeId: Id,
    metrics: Metrics,
    boundedExecutionContext: ExecutionContext,
    unboundedBlocker: Blocker
  ): RedownloadService[F] =
    new RedownloadService[F](
      redownloadStorage,
      nodeStorage,
      clusterStorage,
      majorityStateChooser,
      missingProposalFinder,
      snapshotStorage,
      snapshotInfoStorage,
      snapshotService,
      snapshotServiceStorage,
      cloudService,
      checkpointService,
      checkpointStorage,
      rewardsManager,
      apiClient,
      broadcastService,
      nodeId,
      metrics,
      boundedExecutionContext,
      unboundedBlocker
    )

  implicit val snapshotProposalsAtHeightEncoder: Encoder[Map[Long, SnapshotProposal]] =
    Encoder.encodeMap[Long, SnapshotProposal]
  implicit val snapshotProposalsAtHeightDecoder: Decoder[Map[Long, SnapshotProposal]] =
    Decoder.decodeMap[Long, SnapshotProposal]

  implicit val proposalCoordinateToString: ProposalCoordinate => String = {
    case (id, height) => s"$id:$height"
  }
}
