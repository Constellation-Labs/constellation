package org.constellation.p2p

import java.net.SocketTimeoutException

import cats.effect.{ContextShift, IO}
import cats.implicits._
import constellation._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.DAO
import org.constellation.consensus.Consensus.RoundId
import org.constellation.domain.consensus.ConsensusStatus
import org.constellation.domain.observation.{Observation, RequestTimeoutOnResolving}
import org.constellation.infrastructure.p2p.ClientInterpreter
import org.constellation.infrastructure.p2p.PeerResponse.PeerResponse
import org.constellation.primitives.Schema.{CheckpointCache, NodeState, SignedObservationEdge}
import org.constellation.primitives.{ChannelMessageMetadata, TransactionCacheData}
import org.constellation.util.Logging._
import org.constellation.util.{Distance, PeerApiClient}

import scala.concurrent.duration._

class DataResolver(dao: DAO) {

  val apiClient = dao.apiClient

  implicit val logger = Slf4jLogger.getLogger[IO]

  def resolveMessageDefaults(
    hash: String,
    priorityClient: Option[PeerApiClient] = None
  )(
    contextToReturn: ContextShift[IO]
  )(implicit apiTimeout: Duration = 3.seconds, dao: DAO): IO[ChannelMessageMetadata] =
    getPeersForResolving(dao).flatMap(resolveMessage(hash, _, priorityClient)(contextToReturn))

  def resolveMessage(
    hash: String,
    pool: List[PeerApiClient],
    priorityClient: Option[PeerApiClient]
  )(
    contextToReturn: ContextShift[IO]
  )(implicit apiTimeout: Duration = 3.seconds, dao: DAO): IO[ChannelMessageMetadata] =
    IO.raiseError(new NotImplementedError())

  def resolveTransactionDefaults(
    hash: String,
    priorityClient: Option[PeerApiClient] = None,
    roundId: Option[RoundId] = None
  )(contextToReturn: ContextShift[IO])(implicit apiTimeout: Duration = 3.seconds, dao: DAO): IO[TransactionCacheData] =
    getPeersForResolving(dao).flatMap(resolveTransaction(hash, _, priorityClient, roundId)(contextToReturn))

  def resolveBatchTransactionsDefaults(
    hashes: List[String],
    priorityClient: Option[PeerApiClient] = None,
    roundId: Option[RoundId] = None
  )(
    contextToReturn: ContextShift[IO]
  )(implicit apiTimeout: Duration = 3.seconds, dao: DAO): IO[List[TransactionCacheData]] =
    if (hashes.nonEmpty)
      getPeersForResolving(dao).flatMap(resolveBatchTransactions(hashes, _, priorityClient, roundId)(contextToReturn))
    else List.empty[TransactionCacheData].pure[IO]

  def resolveBatchObservationsDefaults(
    hashes: List[String],
    priorityClient: Option[PeerApiClient] = None,
    roundId: Option[RoundId] = None
  )(
    contextToReturn: ContextShift[IO]
  )(implicit apiTimeout: Duration = 3.seconds, dao: DAO): IO[List[Observation]] =
    if (hashes.nonEmpty)
      getPeersForResolving(dao).flatMap(resolveBatchObservations(hashes, _, priorityClient, roundId)(contextToReturn))
    else List.empty[Observation].pure[IO]

  def resolveTransaction(
    hash: String,
    pool: List[PeerApiClient],
    priorityClient: Option[PeerApiClient],
    roundId: Option[RoundId] = None
  )(contextToReturn: ContextShift[IO])(implicit apiTimeout: Duration = 3.seconds, dao: DAO): IO[TransactionCacheData] =
    logThread(
      logger.debug(s"Start resolving transaction=${hash} for round $roundId") >>
        resolveDataByDistance[TransactionCacheData](
          List(hash),
          apiClient.transaction.getTransaction,
          pool,
          priorityClient
        )(contextToReturn).sequence.flatMap { txs =>
          txs.headOption match {
            case Some(tcd) =>
              dao.transactionService
                .put(tcd, ConsensusStatus.Unknown)
                .flatTap(_ => logger.debug(s"Stored resolved transaction=${tcd.hash} for roundId=${roundId}"))
            case _ =>
              IO.raiseError[TransactionCacheData](
                new Throwable(s"Failed with resolving transaction=${hash} for roundId=${roundId}")
              )
          }
        },
      s"dataResolver_resolveTransaction [${hash}]"
    )

  def resolveBatchTransactions(
    hashes: List[String],
    pool: List[PeerApiClient],
    priorityClient: Option[PeerApiClient],
    roundId: Option[RoundId] = None
  )(
    contextToReturn: ContextShift[IO]
  )(implicit apiTimeout: Duration = 3.seconds, dao: DAO): IO[List[TransactionCacheData]] =
    logger.debug(s"Start resolving transactions = ${hashes} for round = $roundId") *>
      resolveBatchData[TransactionCacheData](hashes, apiClient.transaction.getBatch, pool ++ priorityClient)(
        contextToReturn
      ).flatMap(
        txs =>
          txs.traverse { tcd =>
            dao.transactionService
              .put(tcd, ConsensusStatus.Unknown)
              .flatTap(
                _ => logger.debug(s"Stored resolved transactions = ${tcd.hash} for roundId = ${roundId}")
              )
          }
      )

  def resolveBatchObservations(
    hashes: List[String],
    pool: List[PeerApiClient],
    priorityClient: Option[PeerApiClient],
    roundId: Option[RoundId] = None
  )(
    contextToReturn: ContextShift[IO]
  )(implicit apiTimeout: Duration = 3.seconds, dao: DAO): IO[List[Observation]] =
    logger.debug(s"Start resolving observations = ${hashes} for round = $roundId") *>
      resolveBatchData[Observation](hashes, apiClient.observation.getBatch, pool ++ priorityClient)(contextToReturn)
        .flatMap(
          obs =>
            obs.traverse { o =>
              dao.observationService
                .put(o, ConsensusStatus.Unknown)
                .flatTap(
                  _ => logger.debug(s"Stored resolved observations = ${o.hash} for roundId = ${roundId}")
                )
            }
        )

  def resolveCheckpointDefaults(
    hash: String,
    priorityClient: Option[PeerApiClient] = None
  )(contextToReturn: ContextShift[IO])(implicit apiTimeout: Duration = 3.seconds, dao: DAO): IO[CheckpointCache] =
    getPeersForResolving(dao).flatMap(resolveCheckpoint(hash, _, priorityClient)(contextToReturn))

  def resolveCheckpoint(
    hash: String,
    pool: List[PeerApiClient],
    priorityClient: Option[PeerApiClient] = None
  )(contextToReturn: ContextShift[IO])(implicit apiTimeout: Duration = 3.seconds, dao: DAO): IO[CheckpointCache] =
    logThread(
      resolveDataByDistance[CheckpointCache](
        List(hash),
        apiClient.checkpoint.getCheckpoint,
        pool,
        priorityClient
      )(contextToReturn).head
        .flatTap(dao.checkpointAcceptanceService.accept(_)(contextToReturn).start(contextToReturn))
        .flatTap { cb =>
          logger.debug(s"Resolving checkpoint=$hash with baseHash=${cb.checkpointBlock.baseHash}")
        },
      s"dataResolver_resolveCheckpoint [${hash}]"
    )

  def resolveSoeDefaults(
    hashes: List[String],
    priorityClient: Option[PeerApiClient] = None
  )(
    contextToReturn: ContextShift[IO]
  )(implicit apiTimeout: Duration = 3.seconds, dao: DAO): IO[List[SignedObservationEdge]] =
    getPeersForResolving(dao).flatMap(resolveSoe(hashes, _, priorityClient)(contextToReturn))

  def resolveSoe(
    hashes: List[String],
    pool: List[PeerApiClient],
    priorityClient: Option[PeerApiClient] = None
  )(
    contextToReturn: ContextShift[IO]
  )(implicit apiTimeout: Duration = 3.seconds, dao: DAO): IO[List[SignedObservationEdge]] =
    logThread(
      resolveDataByDistanceFlat[SignedObservationEdge](
        hashes,
        apiClient.soe.getSoe,
        pool,
        priorityClient
      )(contextToReturn).flatTap(_.traverse(soe => dao.soeService.put(soe.hash, soe))),
      s"dataResolver_resolveSoe [${hashes}]"
    )

  def resolveCheckpoints(
    hashes: List[String],
    pool: List[PeerApiClient],
    priorityClient: Option[PeerApiClient] = None
  )(contextToReturn: ContextShift[IO])(implicit apiTimeout: Duration = 3.seconds, dao: DAO): IO[List[CheckpointCache]] =
    logThread(
      resolveDataByDistanceFlat[CheckpointCache](
        hashes,
        apiClient.checkpoint.getCheckpoint,
        pool,
        priorityClient
      )(contextToReturn).flatTap(
        cbs => cbs.traverse(cb => dao.checkpointAcceptanceService.accept(cb)(contextToReturn))
      ),
      s"dataResolver_resolveCheckpoints [${hashes}]"
    )

  def resolveObservationDefaults(
    hash: String,
    priorityClient: Option[PeerApiClient] = None
  )(contextToReturn: ContextShift[IO])(implicit apiTimeout: Duration = 3.seconds, dao: DAO): IO[Observation] =
    getPeersForResolving(dao).flatMap(resolveObservation(hash, _, priorityClient)(contextToReturn))

  def resolveObservation(
    hash: String,
    pool: List[PeerApiClient],
    priorityClient: Option[PeerApiClient]
  )(contextToReturn: ContextShift[IO])(implicit apiTimeout: Duration = 3.seconds, dao: DAO): IO[Observation] =
    logThread(
      resolveDataByDistance[Observation](
        List(hash),
        apiClient.observation.getObservation,
        pool,
        priorityClient
      )(contextToReturn).head
        .flatTap(o => logger.debug(s"Resolving observation=${o.hash}"))
        .flatTap(o => dao.observationService.put(o, ConsensusStatus.Unknown)),
      s"dataResolver_resolveObservation [${hash}]"
    )

  def resolveDataByDistance[T <: AnyRef](
    hashes: List[String],
    endpoint: String => PeerResponse[IO, Option[T]],
    pool: List[PeerApiClient],
    priorityClient: Option[PeerApiClient] = None
  )(
    contextToReturn: ContextShift[IO]
  )(implicit apiTimeout: Duration = 3.seconds, m: Manifest[T], dao: DAO): List[IO[T]] =
    hashes.map { hash =>
      resolveData[T](hash, endpoint, priorityClient.toList ++ pool.sortBy { p =>
        Distance.calculate(hash, p.id)
      })(contextToReturn)
    }

  def resolveBatchDataByDistance[T <: AnyRef](
    hashes: List[String],
    endpoint: List[String] => PeerResponse[IO, List[(String, Option[T])]],
    pool: List[PeerApiClient],
    priorityClient: Option[PeerApiClient] = None
  )(
    contextToReturn: ContextShift[IO]
  )(implicit apiTimeout: Duration = 30.seconds, m: Manifest[T], dao: DAO): IO[List[T]] =
    hashes match {
      case Nil => List.empty.pure[IO]
      case _ =>
        resolveBatchData[T](hashes, endpoint, priorityClient.toList ++ pool.sortBy { p =>
          Distance.calculate(hashes.head, p.id)
        })(contextToReturn)
    }

  private[p2p] def resolveData[T <: AnyRef](
    hash: String,
    endpoint: String => PeerResponse[IO, Option[T]],
    sortedPeers: List[PeerApiClient],
    maxErrors: Int = 100
  )(contextToReturn: ContextShift[IO])(implicit apiTimeout: Duration = 3.seconds, m: Manifest[T], dao: DAO): IO[T] = {

    def makeAttempt(
      innerPeers: Iterable[PeerApiClient],
      allPeers: Iterable[PeerApiClient],
      errorsSoFar: Int = 0
    ): IO[T] =
      innerPeers match {
        case _ if errorsSoFar >= maxErrors =>
          IO.raiseError[T](DataResolutionMaxErrors(hash))
        case Nil =>
          IO.raiseError[T](
            DataResolutionOutOfPeers(dao.id.short, List(hash), allPeers.map(_.id.short))
          )
        case head :: tail =>
          getData[T](hash, endpoint, head)(contextToReturn).flatMap {
            case Some(a) => IO.pure(a)
            case None =>
              IO.raiseError[T](DataResolutionNoneResponse(hash, head))
          }.handleErrorWith {
            case e: DataResolutionMaxErrors  => IO.raiseError[T](e)
            case e: DataResolutionOutOfPeers => IO.raiseError[T](e)
            case e if tail.isEmpty           => IO.raiseError[T](e)
            case e: DataResolutionNoneResponse =>
              logger.warn(e.getMessage) >>
                makeAttempt(tail, allPeers, errorsSoFar + 1)
            case e =>
              logger.error(
                s"Unexpected error while resolving hash=${hash} with host=${head.client.host}, and id ${head.client.id} trying next peer | ${e.getMessage}"
              ) >>
                makeAttempt(tail, allPeers, errorsSoFar + 1)
          }

      }

    for {
      _ <- logger.debug(s"Resolve $endpoint/$hash")
      t <- makeAttempt(sortedPeers, sortedPeers)
    } yield t
  }

  private[p2p] def resolveBatchData[T <: AnyRef](
    hashes: List[String],
    endpoint: List[String] => PeerResponse[IO, List[(String, Option[T])]],
    sortedPeers: List[PeerApiClient]
  )(
    contextToReturn: ContextShift[IO]
  )(implicit apiTimeout: Duration = 3.seconds, m: Manifest[T], dao: DAO): IO[List[T]] = {

    def makeAttempt(
      peers: List[PeerApiClient],
      allHashes: List[String],
      innerHashes: List[String] = List.empty[String],
      response: List[T] = List.empty[T]
    ): IO[List[T]] = {

      if (peers.isEmpty && innerHashes.size != allHashes.size) {
        return IO.raiseError(DataResolutionOutOfPeers(dao.id.short, hashes, sortedPeers.map(_.id.short)))
      }

      if (innerHashes.size == allHashes.size) {
        return response.pure[IO]
      }

      val peer = peers.head
      val filteredPeers = peers.filterNot(_ == peer)
      val requestHashes = allHashes.filterNot(innerHashes.toSet)

      getBatchData[T](requestHashes, endpoint, peer)(contextToReturn).flatMap { data =>
        logger.debug(
          s"Requested : ${peer.client.host} : for hashes = ${requestHashes
            .mkString(",")} : and response hashes = ${data.size}"
        ) >>
          makeAttempt(filteredPeers, allHashes, innerHashes ++ data.map(_._1), response ++ data.map(_._2))
      }
    }

    makeAttempt(sortedPeers, hashes).handleErrorWith(
      e => logger.error(s"Unexpected error while resolving hashes : ${e.getMessage}") *> IO.raiseError(e)
    )
  }

  private[p2p] def getData[T <: AnyRef](
    hash: String,
    endpoint: String => PeerResponse[IO, Option[T]],
    peerApiClient: PeerApiClient
  )(contextToReturn: ContextShift[IO])(implicit apiTimeout: Duration, m: Manifest[T], dao: DAO): IO[Option[T]] =
    endpoint(hash).run(peerApiClient.client).onError {
      case _: SocketTimeoutException =>
        dao.observationService
          .put(Observation.create(peerApiClient.id, RequestTimeoutOnResolving(List(hash)))(dao.keyPair))
          .void
    }

  private[p2p] def getBatchData[T <: AnyRef](
    hashes: List[String],
    endpoint: List[String] => PeerResponse[IO, List[(String, Option[T])]],
    peerApiClient: PeerApiClient
  )(contextToReturn: ContextShift[IO])(implicit apiTimeout: Duration, m: Manifest[T], dao: DAO): IO[List[(String, T)]] =
    endpoint(hashes)
      .run(peerApiClient.client)
      .map(_.mapFilter {
        case (hash, data) => data.map((hash, _))
      })
      .onError {
        case _: SocketTimeoutException =>
          dao.observationService
            .put(Observation.create(peerApiClient.id, RequestTimeoutOnResolving(hashes))(dao.keyPair))
            .void
      }

  private[p2p] def resolveDataByDistanceFlat[T <: AnyRef](
    hashes: List[String],
    endpoint: String => PeerResponse[IO, Option[T]],
    pool: List[PeerApiClient],
    priorityClient: Option[PeerApiClient] = None
  )(
    contextToReturn: ContextShift[IO]
  )(implicit apiTimeout: Duration = 3.seconds, m: Manifest[T], dao: DAO): IO[List[T]] =
    resolveDataByDistance[T](hashes, endpoint, pool, priorityClient)(contextToReturn).sequence

  private[p2p] def getPeersForResolving(dao: DAO): IO[List[PeerApiClient]] = {
    val peers = for {
      all <- dao.peerInfo
      ready = all.filter { case (_, peer) => NodeState.isNotOffline(peer.peerMetadata.nodeState) }
      leaving <- dao.leavingPeers
    } yield (ready ++ leaving)
    peers.map(_.map(p => PeerApiClient(p._1, p._2.peerMetadata.toPeerClientMetadata)).toList)
  }

}

case class DataResolutionOutOfPeers(thisNode: String, hashes: List[String], peers: Iterable[String])
    extends Exception(
      s"Node [$thisNode] run out of peers when resolving with hashes: $hashes following tried: $peers"
    )

case class DataResolutionNoneResponse(hash: String, client: PeerApiClient)
    extends Exception(
      s"Failed to resolve hash=${hash} with host=${client.client.host} and id ${client.id.short}, returned None"
    )
case class DataResolutionMaxErrors(hash: String)
    extends Exception(
      s"Max errors threshold reached when resolving hash: $hash aborting"
    )
