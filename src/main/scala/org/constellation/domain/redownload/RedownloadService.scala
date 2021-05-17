package org.constellation.domain.redownload

import java.security.KeyPair

import cats.data.{EitherT, NonEmptyList}
import cats.effect.concurrent.Ref
import cats.effect.{Blocker, Concurrent, ContextShift, Timer}
import cats.syntax.all._
import cats.{Applicative, NonEmptyParallel}
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.chrisdavenport.mapref.MapRef
import io.circe.{Decoder, Encoder}
import org.constellation.ConfigUtil
import org.constellation.checkpoint.{CheckpointAcceptanceService, TopologicalSort}
import org.constellation.concurrency.MapRefUtils
import org.constellation.concurrency.MapRefUtils.MapRefOps
import org.constellation.concurrency.cuckoo.{CuckooFilter, MutableCuckooFilter}
import org.constellation.domain.cloud.CloudService.CloudServiceEnqueue
import org.constellation.domain.redownload.RedownloadService._
import org.constellation.domain.storage.LocalFileStorage
import org.constellation.infrastructure.p2p.PeerResponse.PeerClientMetadata
import org.constellation.infrastructure.p2p.{ClientInterpreter, PeerResponse}
import org.constellation.p2p.{Cluster, MajorityHeight}
import org.constellation.rewards.RewardsManager
import org.constellation.schema.signature.Signed
import org.constellation.schema.signature.Signed.signed
import org.constellation.schema.snapshot._
import org.constellation.schema.{Id, NodeState}
import org.constellation.serialization.KryoSerializer
import org.constellation.storage.SnapshotService
import org.constellation.util.Logging.stringifyStackTrace
import org.constellation.util.Metrics

import scala.collection.SortedMap
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.math.{max, min}
import scala.util.Random

class RedownloadService[F[_]: NonEmptyParallel: Applicative](
  meaningfulSnapshotsCount: Int,
  redownloadInterval: Int,
  heightInterval: Long,
  cluster: Cluster[F],
  majorityStateChooser: MajorityStateChooser,
  missingProposalFinder: MissingProposalFinder,
  snapshotStorage: LocalFileStorage[F, StoredSnapshot],
  snapshotInfoStorage: LocalFileStorage[F, SnapshotInfo],
  snapshotService: SnapshotService[F],
  cloudService: CloudServiceEnqueue[F],
  checkpointAcceptanceService: CheckpointAcceptanceService[F],
  rewardsManager: RewardsManager[F],
  apiClient: ClientInterpreter[F],
  keyPair: KeyPair,
  metrics: Metrics,
  boundedExecutionContext: ExecutionContext,
  unboundedBlocker: Blocker
)(implicit F: Concurrent[F], C: ContextShift[F], T: Timer[F]) {

  /**
    * It contains immutable historical data
    * (even incorrect snapshots which have been "fixed" by majority in acceptedSnapshots).
    * It is used as own proposals along with peerProposals to calculate majority state.
    */
  private[redownload] val createdSnapshots: Ref[F, SnapshotProposalsAtHeight] = Ref.unsafe(Map.empty)

  /**
    * Can be modified ("fixed"/"aligned") by redownload process. It stores current state of snapshots after auto-healing.
    */
  private[redownload] val acceptedSnapshots: Ref[F, SnapshotsAtHeight] = Ref.unsafe(Map.empty)

  /**
    * Majority proposals from other peers. It is used to calculate majority state.
    */
  private[redownload] val peerProposals: MapRef[F, Id, Option[SnapshotProposalsAtHeight]] =
    MapRefUtils.ofConcurrentHashMap()

  private[redownload] val lastMajorityState: Ref[F, SnapshotsAtHeight] = Ref.unsafe(Map.empty)

  private[redownload] var lastSentHeight: Ref[F, Long] = Ref.unsafe(-1L)

  private[redownload] val majorityStallCount: Ref[F, Int] = Ref.unsafe(0)

  private[redownload] val localFilter: MutableCuckooFilter[F, ProposalCoordinate] =
    MutableCuckooFilter[F, ProposalCoordinate]()

  private[redownload] val remoteFilters: MapRef[F, Id, Option[CuckooFilter]] =
    MapRefUtils.ofConcurrentHashMap()

  private val logger = Slf4jLogger.getLogger[F]

  private val stallCountThreshold = ConfigUtil.getOrElse("constellation.snapshot.stallCount", 4)
  private val proposalLookupLimit = ConfigUtil.getOrElse("constellation.snapshot.proposalLookupLimit", 8)

  def setLastSentHeight(height: Long): F[Unit] =
    lastSentHeight.set(height) >> metrics.updateMetricAsync[F]("redownload_lastSentHeight", height)

  def setLastMajorityState(majorityState: SnapshotsAtHeight): F[Unit] =
    lastMajorityState.set(majorityState) >>
      metrics.updateMetricAsync[F]("redownload_lastMajorityStateHeight", majorityState.maxHeight)

  def getLastMajorityState(): F[SnapshotsAtHeight] =
    lastMajorityState.get

  def latestMajorityHeight: F[Long] = lastMajorityState.get.map(_.maxHeight)

  def lowestMajorityHeight: F[Long] = lastMajorityState.get.map(_.minHeight)

  def getMajorityRange: F[HeightRange] = lastMajorityState.get.map { s =>
    HeightRange(minHeight(s), maxHeight(s))
  }

  def replacePeerProposals(peer: Id, proposals: SnapshotProposalsAtHeight): F[Unit] =
    peerProposals(peer).get.flatMap { maybeMap =>
      maybeMap.traverse(removeFromLocalFilter)
    } >> peerProposals(peer).set(proposals.some) >> addToLocalFilter(proposals)

  def removePeerProposalsBelowHeight(height: Long): F[PeersProposals] =
    for {
      ids <- peerProposals.keys
      results <- ids.traverse { id =>
        peerProposals(id).modify { maybeMap =>
          val oldProposals = maybeMap.getOrElse(Map.empty)
          val removedProposals = oldProposals.filterKeys(_ < height)
          val result = oldProposals.removeHeightsBelow(height)
          (result.some, (removedProposals, result))
        }.flatMap { case (removed, result) => removeFromLocalFilter(removed) >> F.pure((id, result)) }
      }
    } yield results.toMap

  private def removeFromLocalFilter(proposals: SnapshotProposalsAtHeight): F[Unit] =
    proposals.values.toList.map { spp =>
      (spp.signature.id, spp.value.height)
    }.traverse { p =>
      localFilter
        .delete(p)
        .ifM(F.unit, logger.warn(s"Proposal $p could not be removed from local filter"))
    } >> F.unit

  def addPeerProposal(proposal: Signed[SnapshotProposal]): F[Unit] =
    logger.debug(
      s"Persisting proposal of ${proposal.signature.id.hex} at height ${proposal.value.height} and hash ${proposal.value.hash}"
    ) >> addPeerProposals(List(proposal))

  def addPeerProposals(proposals: Iterable[Signed[SnapshotProposal]]): F[Unit] =
    proposals.groupBy(_.signature.id).toList.traverse {
      case (id, proposals) =>
        peerProposals(id)
          .modify[SnapshotProposalsAtHeight] { maybeMap =>
            val oldProposals = maybeMap.getOrElse(Map.empty)
            val newProposals = proposals.map(s => (s.value.height, s)).toMap
            ((oldProposals ++ newProposals).some, newProposals -- oldProposals.keySet)
          }
          .flatMap(addToLocalFilter)
    } >> F.unit

  private def addToLocalFilter(proposals: SnapshotProposalsAtHeight): F[Unit] =
    proposals.values.toList.map { spp =>
      (spp.signature.id, spp.value.height)
    }.traverse { p =>
      localFilter
        .insert(p)
        .ifM(F.unit, F.raiseError(new RuntimeException(s"Unable to insert proposal $p to local filter")))
    } >> F.unit

  def replaceRemoteFilter(peerId: Id, filterData: FilterData): F[Unit] =
    remoteFilters(peerId).set(CuckooFilter(filterData).some)

  def localFilterData: F[FilterData] = localFilter.getFilterData

  def persistCreatedSnapshot(height: Long, hash: String, reputation: Reputation): F[Unit] =
    createdSnapshots.modify { m =>
      val updated =
        if (m.contains(height)) m
        else m.updated(height, signed(SnapshotProposal(hash, height, reputation), keyPair))
      (updated, updated.maxHeight)
    }.flatMap(metrics.updateMetricAsync[F]("redownload_maxCreatedSnapshotHeight", _))

  def persistAcceptedSnapshot(height: Long, hash: String): F[Unit] =
    acceptedSnapshots.modify { m =>
      val updated = m.updated(height, hash)
      (updated, updated.maxHeight)
    }.flatMap(metrics.updateMetricAsync[F]("redownload_maxAcceptedSnapshotHeight", _))

  def getCreatedSnapshots(): F[SnapshotProposalsAtHeight] = createdSnapshots.get

  def getAcceptedSnapshots(): F[SnapshotsAtHeight] = acceptedSnapshots.get

  def getAllPeerProposals(): F[PeersProposals] = peerProposals.toMap

  def getPeerProposals(peerId: Id): F[Option[SnapshotProposalsAtHeight]] = peerProposals(peerId).get

  def clear: F[Unit] =
    for {
      _ <- createdSnapshots.modify(_ => (Map.empty, ()))
      _ <- acceptedSnapshots.modify(_ => (Map.empty, ()))
      _ <- peerProposals.clear
      _ <- localFilter.clear
      _ <- remoteFilters.clear
      _ <- setLastMajorityState(Map.empty)
      _ <- setLastSentHeight(-1)
      _ <- rewardsManager.clearLastRewardedHeight()
    } yield ()

  @deprecated("Proposals are now pushed via gossip instead of pulling")
  def fetchAndUpdatePeersProposals(): F[Unit] =
    for {
      _ <- logger.debug("Fetching and updating peer proposals")
      peers <- cluster.getPeerInfo.map(
        _.values.toList.filter(p => NodeState.isNotOffline(p.peerMetadata.nodeState))
      )
      apiClients = peers.map(_.peerMetadata.toPeerClientMetadata)
      responses <- apiClients.traverse { client =>
        fetchCreatedSnapshots(client).map(client.id -> _)
      }

      _ <- responses.traverse {
        case (_, proposals) => addPeerProposals(proposals.values)
      }
    } yield ()

  private[redownload] def fetchStoredSnapshotsFromAllPeers(): F[Map[PeerClientMetadata, Set[String]]] =
    for {
      peers <- cluster.getPeerInfo.map(_.values.toList)
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
    snapshotsToDownload: SnapshotsAtHeight
  ): EitherT[F, Throwable, Unit] =
    for {
      storedSnapshots <- fetchStoredSnapshotsFromAllPeers().attemptT
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

  private def fetchAndPersistBlocksAboveMajority(majorityState: SnapshotsAtHeight): EitherT[F, Throwable, Unit] =
    for {
      storedSnapshots <- fetchStoredSnapshotsFromAllPeers().attemptT

      maxMajorityHash = majorityState(maxHeight(majorityState))
      peersWithMajority = storedSnapshots.filter { case (_, hashes) => hashes.contains(maxMajorityHash) }.keySet

      majoritySnapshotInfo <- snapshotInfoStorage.read(maxMajorityHash)

      _ <- useRandomClient(peersWithMajority) { client =>
        for {
          accepted <- fetchAcceptedSnapshots(client)

          maxMajorityHeight = maxHeight(majorityState)
          acceptedSnapshotHashesAboveMajority = takeHighestUntilKey(accepted, maxMajorityHeight).values

          acceptedSnapshots <- acceptedSnapshotHashesAboveMajority.toList
            .traverse(hash => fetchSnapshot(hash)(client))
            .flatMap(
              snapshotsSerialized =>
                snapshotsSerialized.traverse { s =>
                  C.evalOn(boundedExecutionContext)(F.delay { KryoSerializer.deserializeCast[StoredSnapshot](s) })
                }
            )

          snapshotInfoFromMemPool <- fetchSnapshotInfo(client).flatMap { s =>
            C.evalOn(boundedExecutionContext)(F.delay { KryoSerializer.deserializeCast[SnapshotInfo](s) })
          }
          acceptedBlocksFromSnapshotInfo = snapshotInfoFromMemPool.acceptedCBSinceSnapshotCache.toSet
          awaitingBlocksFromSnapshotInfo = snapshotInfoFromMemPool.awaitingCbs

          blocksFromSnapshots = acceptedSnapshots.flatMap(_.checkpointCache)
          blocksToAccept = (blocksFromSnapshots ++ acceptedBlocksFromSnapshotInfo ++ awaitingBlocksFromSnapshotInfo).distinct

          _ <- snapshotService.setSnapshot(majoritySnapshotInfo)

          _ <- checkpointAcceptanceService.waitingForAcceptance.modify { blocks =>
            val updated = blocks ++ blocksToAccept.map(_.checkpointBlock.soeHash)
            (updated, ())
          }

          sorted <- C.evalOn(boundedExecutionContext)(F.delay {
            TopologicalSort.sortBlocksTopologically(blocksToAccept)
          })
          _ <- sorted.toList.traverse { b =>
            logger.debug(s"Accepting block above majority: ${b.height}") >>
              C.evalOn(boundedExecutionContext) {
                  checkpointAcceptanceService
                    .accept(b)
                }
                .handleErrorWith(
                  error => logger.warn(error)(s"Error during blocks acceptance after redownload") >> F.unit
                )
          }
        } yield ()
      }.attemptT
    } yield ()

  def checkForAlignmentWithMajoritySnapshot(isDownload: Boolean = false): F[Unit] = {
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
            _ <- applyPlan(plan, meaningfulMajorityState).value.flatMap(F.fromEither)
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

      lastMajority <- getLastMajorityState()
      (_, createdSnapshots, peerProposals) <- removeSnapshotsAndProposalsBelowHeight(lastMajority.minHeight)

      peers <- cluster.getPeerInfo
      ownPeer <- cluster.getOwnJoinedHeight()
      peersCache = peers.map {
        case (id, peerData) => (id, peerData.majorityHeight)
      } ++ Map(cluster.id -> NonEmptyList.one(MajorityHeight(ownPeer, None)))

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

      joinHeight = if (isDownload) calculatedMajorityWithoutGaps.maxHeight
      else ownPeer.getOrElse(calculatedMajorityWithoutGaps.maxHeight)

      majorityBeforeCutOff = {
        val intersect = lastMajority.keySet & calculatedMajorityWithoutGaps.keySet
        if (lastMajority.isEmpty || intersect.nonEmpty)
          calculatedMajorityWithoutGaps
        else
          lastMajority
      }

      cutOffHeight = getCutOffHeight(joinHeight, calculatedMajorityWithoutGaps, lastMajority)
      meaningfulMajority = majorityBeforeCutOff.removeHeightsBelow(cutOffHeight)
      _ <- setLastMajorityState(meaningfulMajority)

      (meaningfulAcceptedSnapshots, _, _) <- removeSnapshotsAndProposalsBelowHeight(cutOffHeight)

      _ <- logger.debug(s"Meaningful majority in range ${calculatedMajority.heightRange}")

      _ <- F
        .pure(meaningfulMajority.maxHeight > lastMajority.maxHeight)
        .ifM(majorityStallCount.set(0), majorityStallCount.update(_ + 1))

      _ <- if (meaningfulMajority.isEmpty) logger.debug("Meaningful majority is empty - skipping redownload")
      else F.unit

      shouldPerformRedownload = shouldRedownload(
        meaningfulAcceptedSnapshots,
        meaningfulMajority,
        redownloadInterval,
        isDownload
      )

      _ <- if (shouldPerformRedownload) {
        {
          if (isDownload)
            cluster.compareAndSet(NodeState.validForDownload, NodeState.DownloadInProgress)
          else
            cluster.compareAndSet(NodeState.validForRedownload, NodeState.DownloadInProgress)
        }.flatMap { state =>
          if (state.isNewSet) {
            wrappedRedownload(shouldPerformRedownload, meaningfulAcceptedSnapshots, meaningfulMajority).handleErrorWith {
              error =>
                logger.error(error)(s"Redownload error, isDownload=$isDownload: ${stringifyStackTrace(error)}") >> {
                  if (isDownload)
                    error.raiseError[F, Unit]
                  else
                    cluster
                      .compareAndSet(NodeState.validDuringDownload, NodeState.Ready)
                      .void
                }
            }
          } else
            logger.debug(s"Setting node state during redownload failed! Skipping redownload. isDownload=$isDownload")
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

      _ <- if (shouldPerformRedownload && !isDownload) { // I think we should only set it to Ready when we are not joining
        cluster.compareAndSet(NodeState.validDuringDownload, NodeState.Ready)
      } else F.unit

      _ <- logger.debug("Accepting all the checkpoint blocks received during the redownload.")
      _ <- acceptCheckpointBlocks().value.flatMap(F.fromEither)

      _ <- if (!shouldPerformRedownload) findAndFetchMissingProposals(peerProposals, peersCache) else F.unit
    } yield ()

    cluster.getNodeState.map { current =>
      if (isDownload) NodeState.validForDownload.contains(current)
      else NodeState.validForRedownload.contains(current)
    }.ifM(
      wrappedCheck,
      logger.debug(s"Node state is not valid for redownload, skipping. isDownload=$isDownload") >> F.unit
    )
  }.handleErrorWith { error =>
    logger.error(error)("Error during checking alignment with majority snapshot.") >>
      error.raiseError[F, Unit]
  }

  private def getCutOffHeight(
    joinHeight: Long,
    candidateMajority: SnapshotsAtHeight,
    lastMajority: SnapshotsAtHeight
  ): Long = {
    val ignorePoint = getIgnorePoint(candidateMajority.maxHeight)
    min(max(ignorePoint, joinHeight), lastMajority.maxHeight)
  }

  private[redownload] def removeSnapshotsAndProposalsBelowHeight(
    height: Long
  ): F[(SnapshotsAtHeight, SnapshotProposalsAtHeight, PeersProposals)] =
    for {
      as <- acceptedSnapshots.updateAndGet(_.removeHeightsBelow(height))
      cs <- createdSnapshots.updateAndGet(_.removeHeightsBelow(height))
      pp <- removePeerProposalsBelowHeight(height)
    } yield (as, cs, pp)

  private[redownload] def getLookupRange: F[HeightRange] =
    for {
      stallCount <- majorityStallCount.get
      majorityHeight <- latestMajorityHeight
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
              )
          }
        )

    } yield ()

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
      map <- remoteFilters.toMap
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
      peer <- cluster.getPeerInfo.map(_.get(peerId))
      apiClient = peer.map(_.peerMetadata.toPeerClientMetadata)
      _ <- apiClient.traverse { client =>
        for {
          proposals <- fetchPeerProposals(query.toList, client)
          filteredProposals = proposals
            .mapFilter(identity)
            .filter(_.validSignature)
          _ <- addPeerProposals(filteredProposals)
        } yield ()
      }
    } yield ()

  private[redownload] def sendMajoritySnapshotsToCloud(): F[Unit] = {
    val send = for {
      majorityState <- lastMajorityState.get
      accepted <- acceptedSnapshots.get
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
      majorityState <- lastMajorityState.get.attemptT
      accepted <- acceptedSnapshots.get.attemptT
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
    majorityState: SnapshotsAtHeight
  ): EitherT[F, Throwable, Unit] =
    for {
      _ <- EitherT.liftF(logRedownload(plan, majorityState))

      _ <- EitherT.liftF(logger.debug("Fetching and storing missing snapshots on disk."))
      _ <- fetchAndStoreMissingSnapshots(plan.toDownload)

      _ <- EitherT.liftF(logger.debug("Filling missing createdSnapshots by majority state"))
      _ <- updateCreatedSnapshots(plan)

      _ <- EitherT.liftF(logger.debug("Replacing acceptedSnapshots by majority state"))
      _ <- updateAcceptedSnapshots(plan)

      _ <- EitherT.liftF(logger.debug("Fetching and persisting blocks above majority."))
      _ <- fetchAndPersistBlocksAboveMajority(majorityState)

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

  private[redownload] def getIgnorePoint(maxHeight: Long): Long = maxHeight - meaningfulSnapshotsCount

  private[redownload] def getRemovalPoint(maxHeight: Long): Long = getIgnorePoint(maxHeight) - redownloadInterval * 2

  def removeUnacceptedSnapshotsFromDisk(): EitherT[F, Throwable, Unit] =
    for {
      nextSnapshotHash <- snapshotService.nextSnapshotHash.get.attemptT
      stored <- snapshotStorage.list().map(_.toSet)
      accepted <- getAcceptedSnapshots().attemptT.map(_.values.toSet)
      created <- getCreatedSnapshots().attemptT.map(_.values.map(_.value.hash).toSet)
      diff = stored.diff(accepted ++ created ++ Set(nextSnapshotHash))
      sentToCloud <- cloudService.getAlreadySent().map(_.map(_.hash)).attemptT
      toRemove = diff.intersect(sentToCloud).toList
      _ <- toRemove.traverse { hash =>
        snapshotStorage.delete(hash) >>
          snapshotInfoStorage.delete(hash) >>
          cloudService.removeSentSnapshot(hash).attemptT
      }
    } yield ()

  private def takeHighestUntilKey[K <: Long, V](data: Map[K, V], key: K): Map[K, V] =
    data.filterKeys(_ > key)

  private[redownload] def updateCreatedSnapshots(plan: RedownloadPlan): EitherT[F, Throwable, Unit] =
    plan.toDownload.map {
      // IMPORTANT! persistCreatedSnapshot DOES NOT override existing values (!)
      case (height, hash) => persistCreatedSnapshot(height, hash, SortedMap.empty)
    }.toList.sequence.void.attemptT

  private[redownload] def updateAcceptedSnapshots(plan: RedownloadPlan): EitherT[F, Throwable, Unit] =
    acceptedSnapshots.modify { m =>
      // It removes everything from "ignored" pool!
      val updated = plan.toLeave ++ plan.toDownload // TODO: mwadon - is correct?
      // It leaves "ignored" pool untouched and aligns above "ignored" pool
      // val updated = (m -- plan.toRemove.keySet) |+| plan.toDownload
      (updated, ())
    }.attemptT

  private[redownload] def acceptCheckpointBlocks(): EitherT[F, Throwable, Unit] =
    (for {
      blocksToAccept <- snapshotService.syncBufferPull().map(_.values.toSeq.map(_.checkpointCacheData).distinct)
      _ <- checkpointAcceptanceService.waitingForAcceptance.modify { blocks =>
        val updated = blocks ++ blocksToAccept.map(_.checkpointBlock.soeHash)
        (updated, ())
      }
      _ <- TopologicalSort.sortBlocksTopologically(blocksToAccept).toList.traverse { b =>
        logger.debug(s"Accepting sync buffer block: ${b.height}") >>
          checkpointAcceptanceService.accept(b).handleErrorWith { error =>
            logger.warn(error)(s"Error during buffer pool blocks acceptance after redownload") >> F.unit
          }
      }
    } yield ()).attemptT

  private[redownload] def updateHighestSnapshotInfo(): EitherT[F, Throwable, Unit] =
    for {
      highestSnapshotInfo <- getAcceptedSnapshots().attemptT
        .map(_.maxBy { case (height, _) => height } match { case (_, hash) => hash })
        .flatMap(hash => snapshotInfoStorage.read(hash))
      _ <- snapshotService.setSnapshot(highestSnapshotInfo).attemptT
    } yield ()

  private[redownload] def shouldRedownload(
    acceptedSnapshots: SnapshotsAtHeight,
    majorityState: SnapshotsAtHeight,
    redownloadInterval: Int,
    isDownload: Boolean = false
  ): Boolean =
    if (majorityState.isEmpty) false
    else
      isDownload || (getAlignmentResult(acceptedSnapshots, majorityState, redownloadInterval) match {
        case AlignedWithMajority => false
        case _                   => true
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
    val highestAcceptedHeight = maxHeight(acceptedSnapshots)
    val highestMajorityHeight = maxHeight(majorityState)

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

  def apply[F[_]: Concurrent: ContextShift: NonEmptyParallel: Timer](
    meaningfulSnapshotsCount: Int,
    redownloadInterval: Int,
    heightInterval: Long,
    cluster: Cluster[F],
    majorityStateChooser: MajorityStateChooser,
    missingProposalFinder: MissingProposalFinder,
    snapshotStorage: LocalFileStorage[F, StoredSnapshot],
    snapshotInfoStorage: LocalFileStorage[F, SnapshotInfo],
    snapshotService: SnapshotService[F],
    cloudService: CloudServiceEnqueue[F],
    checkpointAcceptanceService: CheckpointAcceptanceService[F],
    rewardsManager: RewardsManager[F],
    apiClient: ClientInterpreter[F],
    keyPair: KeyPair,
    metrics: Metrics,
    boundedExecutionContext: ExecutionContext,
    unboundedBlocker: Blocker
  ): RedownloadService[F] =
    new RedownloadService[F](
      meaningfulSnapshotsCount,
      redownloadInterval,
      heightInterval,
      cluster,
      majorityStateChooser,
      missingProposalFinder,
      snapshotStorage,
      snapshotInfoStorage,
      snapshotService,
      cloudService,
      checkpointAcceptanceService,
      rewardsManager,
      apiClient,
      keyPair,
      metrics,
      boundedExecutionContext,
      unboundedBlocker
    )

  type Reputation = SortedMap[Id, Double]
  type ProposalCoordinate = (Id, Long)
  type SnapshotsAtHeight = Map[Long, String] // height -> hash
  type SnapshotProposalsAtHeight = Map[Long, Signed[SnapshotProposal]]
  type PeersProposals = Map[Id, SnapshotProposalsAtHeight]
  type PeersCache = Map[Id, NonEmptyList[MajorityHeight]]
  type SnapshotInfoSerialized = Array[Byte]
  type SnapshotSerialized = Array[Byte]

  implicit val snapshotProposalsAtHeightEncoder: Encoder[Map[Long, SnapshotProposal]] =
    Encoder.encodeMap[Long, SnapshotProposal]
  implicit val snapshotProposalsAtHeightDecoder: Decoder[Map[Long, SnapshotProposal]] =
    Decoder.decodeMap[Long, SnapshotProposal]

  implicit val proposalCoordinateToString: ProposalCoordinate => String = {
    case (id, height) => s"$id:$height"
  }

  implicit class HeightMapOps[V](val map: Map[Long, V]) {

    def removeHeightsBelow(height: Long): Map[Long, V] =
      map.filterKeys(_ >= height).toSeq.toMap // `.toSeq.toMap` is used for strict (eager) filtering

    def removeHeightsAbove(height: Long): Map[Long, V] =
      map.filterKeys(_ <= height).toSeq.toMap // `.toSeq.toMap` is used for strict (eager) filtering

    def minHeight: Long = minHeightEntry.map(_._1).getOrElse(0)

    def maxHeight: Long = maxHeightEntry.map(_._1).getOrElse(0)

    def minHeightEntry: Option[(Long, V)] = map.toList.minimumByOption { case (height, _) => height }

    def maxHeightEntry: Option[(Long, V)] = map.toList.maximumByOption { case (height, _) => height }

    def heightRange: HeightRange = HeightRange(minHeight, maxHeight)
  }
}
