package org.constellation.p2p

import cats.effect.concurrent.Ref
import cats.effect.{Blocker, Concurrent, ContextShift, Timer}
import cats.syntax.all._
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.domain.checkpointBlock.CheckpointStorageAlgebra
import org.constellation.domain.cluster.ClusterStorageAlgebra
import org.constellation.domain.consensus.ConsensusStatus
import org.constellation.domain.observation.ObservationService
import org.constellation.domain.transaction.TransactionService
import org.constellation.infrastructure.p2p.PeerResponse.{PeerClientMetadata, PeerResponse}
import org.constellation.infrastructure.p2p.{ClientInterpreter, PeerResponse}
import org.constellation.p2p.DataResolver._
import org.constellation.schema.NodeState
import org.constellation.schema.checkpoint.CheckpointCache
import org.constellation.schema.consensus.RoundId
import org.constellation.schema.observation.{Observation, RequestTimeoutOnResolving}
import org.constellation.schema.transaction.TransactionCacheData
import org.constellation.util.Distance
import org.constellation.util.Logging._

import java.net.SocketTimeoutException
import java.security.KeyPair

class DataResolver[F[_]](
  keyPair: KeyPair,
  apiClient: ClientInterpreter[F],
  clusterStorage: ClusterStorageAlgebra[F],
  checkpointStorage: CheckpointStorageAlgebra[F],
  transactionService: TransactionService[F],
  observationService: ObservationService[F],
  unboundedBlocker: Blocker
)(
  implicit F: Concurrent[F],
  T: Timer[F],
  CS: ContextShift[F]
) {

  implicit private val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]

  private val getPeersForResolving: F[List[PeerClientMetadata]] =
    for {
      notOffline <- clusterStorage.getNotOfflinePeers
      peers = notOffline.map { case (_, peer) => peer.peerMetadata.toPeerClientMetadata }
    } yield peers.toList

  def resolveBatchTransactionsDefaults(
    hashes: List[String],
    priorityClient: Option[PeerClientMetadata] = None,
    roundId: Option[RoundId] = None
  ): F[List[TransactionCacheData]] =
    if (hashes.nonEmpty)
      getPeersForResolving.flatMap(resolveBatchTransactions(hashes, _, priorityClient, roundId))
    else List.empty[TransactionCacheData].pure[F]

  def resolveBatchTransactions(
    hashes: List[String],
    pool: List[PeerClientMetadata],
    priorityClient: Option[PeerClientMetadata],
    roundId: Option[RoundId] = None
  ): F[List[TransactionCacheData]] =
    logger.debug(s"Start resolving transactions = $hashes for round = $roundId") >>
      resolveBatchData[TransactionCacheData](hashes, apiClient.transaction.getBatch, pool ++ priorityClient).flatMap(
        txs =>
          txs.traverse { tcd =>
            transactionService
              .put(tcd, ConsensusStatus.Unknown)
              .flatTap(
                _ => logger.debug(s"Stored resolved transactions = ${tcd.hash} for roundId=$roundId")
              )
          }
      )

  def resolveBatchObservationsDefaults(
    hashes: List[String],
    priorityClient: Option[PeerClientMetadata] = None,
    roundId: Option[RoundId] = None
  ): F[List[Observation]] =
    if (hashes.nonEmpty)
      getPeersForResolving.flatMap(resolveBatchObservations(hashes, _, priorityClient, roundId))
    else List.empty[Observation].pure[F]

  def resolveBatchObservations(
    hashes: List[String],
    pool: List[PeerClientMetadata],
    priorityClient: Option[PeerClientMetadata],
    roundId: Option[RoundId] = None
  ): F[List[Observation]] =
    logger.debug(s"Start resolving observations=$hashes for round = $roundId") *>
      resolveBatchData[Observation](hashes, apiClient.observation.getBatch, pool ++ priorityClient)
        .flatMap(
          obs =>
            obs.traverse { o =>
              observationService
                .put(o, ConsensusStatus.Unknown)
                .flatTap(
                  _ => logger.debug(s"Stored resolved observations=${o.hash} for roundId=$roundId")
                )
            }
        )

  private[p2p] def resolveBatchData[A <: AnyRef](
    hashes: List[String],
    endpoint: List[String] => PeerResponse[F, List[(String, Option[A])]],
    sortedPeers: List[PeerClientMetadata]
  ): F[List[A]] = {

    def makeAttempt(
      peers: List[PeerClientMetadata],
      allHashes: List[String],
      innerHashes: List[String] = List.empty[String],
      response: List[A] = List.empty[A]
    ): F[List[A]] = {

      if (peers.isEmpty && innerHashes.size != allHashes.size) {
        return F.raiseError(DataResolutionOutOfPeers(hashes, sortedPeers.map(_.id.short)))
      }

      if (innerHashes.size == allHashes.size) {
        return response.pure[F]
      }

      val peer = peers.head
      val filteredPeers = peers.filterNot(_ == peer)
      val requestHashes = allHashes.filterNot(innerHashes.toSet)

      getBatchData[A](requestHashes, endpoint, peer).flatMap { data =>
        logger.debug(
          s"Requested : ${peer.host} : for hashes = ${requestHashes.mkString(",")} : and response hashes = ${data.size}"
        ) >>
          CS.shift >> makeAttempt(filteredPeers, allHashes, innerHashes ++ data.map(_._1), response ++ data.map(_._2))
      }
    }

    makeAttempt(sortedPeers, hashes).handleErrorWith(
      e => logger.error(e)(s"Unexpected error while resolving hashes") *> F.raiseError[List[A]](e)
    )
  }

  private[p2p] def getBatchData[A <: AnyRef](
    hashes: List[String],
    endpoint: List[String] => PeerResponse[F, List[(String, Option[A])]],
    peerClientMetadata: PeerClientMetadata
  ): F[List[(String, A)]] =
    PeerResponse
      .run(endpoint(hashes), unboundedBlocker)(peerClientMetadata)
      .map(_.mapFilter {
        case (hash, data) => data.map((hash, _))
      })
      .onError {
        case _: SocketTimeoutException =>
          observationService
            .put(Observation.create(peerClientMetadata.id, RequestTimeoutOnResolving(hashes))(keyPair))
            .void
      }

  def resolveCheckpointDefaults(
    hash: String,
    priorityClient: Option[PeerClientMetadata] = None
  ): F[CheckpointCache] =
    checkpointStorage.markCheckpointForResolving(hash) >>
      getPeersForResolving.flatMap(resolveCheckpoint(hash, _, priorityClient))

  def resolveCheckpoint(
    hash: String,
    pool: List[PeerClientMetadata],
    priorityClient: Option[PeerClientMetadata] = None
  ): F[CheckpointCache] =
    logThread(
      resolveDataByDistance[CheckpointCache](
        List(hash),
        apiClient.checkpoint.getCheckpoint,
        pool,
        priorityClient
      ).head.flatTap { cb =>
        logger.debug(s"Resolving checkpoint=$hash with baseHash=${cb.checkpointBlock.baseHash}") >>
          checkpointStorage.persistCheckpoint(cb) >>
          checkpointStorage.registerUsage(cb.checkpointBlock.soeHash) >>
          checkpointStorage.unmarkCheckpointForResolving(hash)
      }.onError { case _ => checkpointStorage.unmarkCheckpointForResolving(hash) },
      s"dataResolver_resolveCheckpoint [$hash]"
    )

  def resolveDataByDistance[A <: AnyRef](
    hashes: List[String],
    endpoint: String => PeerResponse[F, Option[A]],
    pool: List[PeerClientMetadata],
    priorityClient: Option[PeerClientMetadata] = None
  ): List[F[A]] =
    hashes.map { hash =>
      resolveData[A](hash, endpoint, priorityClient.toList ++ pool.sortBy { p =>
        Distance.calculate(hash, p.id)
      })
    }

  private[p2p] def resolveData[A <: AnyRef](
    hash: String,
    endpoint: String => PeerResponse[F, Option[A]],
    sortedPeers: List[PeerClientMetadata],
    maxErrors: Int = 100
  ): F[A] = {

    def makeAttempt(
      innerPeers: Iterable[PeerClientMetadata],
      allPeers: Iterable[PeerClientMetadata],
      errorsSoFar: Int = 0
    ): F[A] =
      innerPeers match {
        case _ if errorsSoFar >= maxErrors =>
          F.raiseError[A](DataResolutionMaxErrors(hash))
        case Nil =>
          F.raiseError[A](
            DataResolutionOutOfPeers(List(hash), allPeers.map(_.id.short))
          )
        case head :: tail =>
          getData[A](hash, endpoint, head).flatMap {
            case Some(a) => a.pure[F]
            case None =>
              F.raiseError[A](DataResolutionNoneResponse(hash, head))
          }.handleErrorWith {
            case e: DataResolutionMaxErrors  => F.raiseError[A](e)
            case e: DataResolutionOutOfPeers => F.raiseError[A](e)
            case e if tail.isEmpty           => F.raiseError[A](e)
            case e: DataResolutionNoneResponse =>
              logger.warn(e)(s"Data resolution none response") >>
                CS.shift >> makeAttempt(tail, allPeers, errorsSoFar + 1)
            case e =>
              logger.error(e)(
                s"Unexpected error while resolving hash=$hash with host=${head.host}, and id ${head.id} trying next peer"
              ) >>
                CS.shift >> makeAttempt(tail, allPeers, errorsSoFar + 1)
          }

      }

    for {
      _ <- logger.debug(s"Resolve $endpoint/$hash")
      t <- makeAttempt(sortedPeers, sortedPeers)
    } yield t
  }

  private[p2p] def getData[T <: AnyRef](
    hash: String,
    endpoint: String => PeerResponse[F, Option[T]],
    peerClientMetadata: PeerClientMetadata
  ): F[Option[T]] =
    PeerResponse.run(endpoint(hash), unboundedBlocker)(peerClientMetadata).onError {
      case _: SocketTimeoutException =>
        observationService
          .put(Observation.create(peerClientMetadata.id, RequestTimeoutOnResolving(List(hash)))(keyPair))
          .void
    }
}

object DataResolver {

  def apply[F[_]: Concurrent: ContextShift: Timer](
    keyPair: KeyPair,
    apiClient: ClientInterpreter[F],
    clusterStorage: ClusterStorageAlgebra[F],
    checkpointStorage: CheckpointStorageAlgebra[F],
    transactionService: TransactionService[F],
    observationService: ObservationService[F],
    unboundedBlocker: Blocker
  ): DataResolver[F] =
    new DataResolver(
      keyPair,
      apiClient,
      clusterStorage,
      checkpointStorage,
      transactionService,
      observationService,
      unboundedBlocker
    )

  case class DataResolutionOutOfPeers(hashes: List[String], peers: Iterable[String])
      extends Exception(
        s"Node run out of peers when resolving with hashes: $hashes following tried: $peers"
      )

  case class DataResolutionNoneResponse(hash: String, client: PeerClientMetadata)
      extends Exception(
        s"Failed to resolve hash=$hash with host=${client.host} and id ${client.id.short}, returned None"
      )
  case class DataResolutionMaxErrors(hash: String)
      extends Exception(
        s"Max errors threshold reached when resolving hash: $hash aborting"
      )
}
