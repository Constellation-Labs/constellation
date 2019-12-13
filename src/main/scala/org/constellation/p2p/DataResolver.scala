package org.constellation.p2p

import java.net.SocketTimeoutException

import cats.effect.{Concurrent, ContextShift, IO}
import cats.implicits._
import com.typesafe.scalalogging.StrictLogging
import constellation._
import org.constellation.DAO
import org.constellation.consensus.Consensus.RoundId
import org.constellation.domain.consensus.ConsensusStatus
import org.constellation.domain.observation.{Observation, RequestTimeoutOnResolving}
import org.constellation.primitives.Schema.{CheckpointCache, SignedObservationEdge}
import org.constellation.primitives.{ChannelMessageMetadata, TransactionCacheData}
import org.constellation.util.Logging._
import org.constellation.util.{Distance, PeerApiClient}

import scala.concurrent.duration._

class DataResolver extends StrictLogging {

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
    logThread(
      resolveDataByDistance[ChannelMessageMetadata](
        List(hash),
        "message",
        pool,
        priorityClient
      )(contextToReturn).head
        .flatTap(mcd => dao.messageService.memPool.put(mcd.channelMessage.signedMessageData.hash, mcd))
        .flatTap(m => IO.delay(logger.debug(s"Resolving message=${m.channelMessage.signedMessageData.hash}"))),
      s"dataResolver_resolveMessage [$hash]",
      logger
    )

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
      IO.delay(logger.debug(s"Start resolving transaction=${hash} for round $roundId")) >>
        resolveDataByDistance[TransactionCacheData](
          List(hash),
          "transaction",
          pool,
          priorityClient
        )(contextToReturn).sequence.flatMap { txs =>
          txs.headOption match {
            case Some(tcd) =>
              dao.transactionService
                .put(tcd, ConsensusStatus.Unknown)
                .flatTap(_ => IO.delay(logger.debug(s"Stored resolved transaction=${tcd.hash} for roundId=${roundId}")))
            case _ =>
              IO.raiseError[TransactionCacheData](
                new Throwable(s"Failed with resolving transaction=${hash} for roundId=${roundId}")
              )
          }
        },
      s"dataResolver_resolveTransaction [${hash}]",
      logger
    )

  def resolveBatchTransactions(
    hashes: List[String],
    pool: List[PeerApiClient],
    priorityClient: Option[PeerApiClient],
    roundId: Option[RoundId] = None
  )(
    contextToReturn: ContextShift[IO]
  )(implicit apiTimeout: Duration = 3.seconds, dao: DAO): IO[List[TransactionCacheData]] =
    IO.delay(logger.debug(s"Start resolving transactions = ${hashes} for round = $roundId")) *>
      resolveBatchData[TransactionCacheData](hashes, "transactions", pool ++ priorityClient)(contextToReturn)
        .flatMap(
          txs =>
            txs.traverse { tcd =>
              dao.transactionService
                .put(tcd, ConsensusStatus.Unknown)
                .flatTap(
                  _ => IO.delay(logger.debug(s"Stored resolved transactions = ${tcd.hash} for roundId = ${roundId}"))
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
    IO.delay(logger.debug(s"Start resolving observations = ${hashes} for round = $roundId")) *>
      resolveBatchData[Observation](hashes, "observations", pool ++ priorityClient)(contextToReturn)
        .flatMap(
          obs =>
            obs.traverse { o =>
              dao.observationService
                .put(o, ConsensusStatus.Unknown)
                .flatTap(
                  _ => IO.delay(logger.debug(s"Stored resolved observations = ${o.hash} for roundId = ${roundId}"))
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
        "checkpoint",
        pool,
        priorityClient
      )(contextToReturn).head
        .flatTap(dao.checkpointAcceptanceService.accept(_)(contextToReturn).start(contextToReturn))
        .flatTap { cb =>
          IO.delay(logger.debug(s"Resolving checkpoint=$hash with baseHash=${cb.checkpointBlock.baseHash}"))
        },
      s"dataResolver_resolveCheckpoint [${hash}]",
      logger
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
        "soe",
        pool,
        priorityClient
      )(contextToReturn).flatTap(_.traverse(soe => dao.soeService.put(soe.hash, soe))),
      s"dataResolver_resolveSoe [${hashes}]",
      logger
    )

  def resolveCheckpoints(
    hashes: List[String],
    pool: List[PeerApiClient],
    priorityClient: Option[PeerApiClient] = None
  )(contextToReturn: ContextShift[IO])(implicit apiTimeout: Duration = 3.seconds, dao: DAO): IO[List[CheckpointCache]] =
    logThread(
      resolveDataByDistanceFlat[CheckpointCache](
        hashes,
        "checkpoint",
        pool,
        priorityClient
      )(contextToReturn).flatTap(
        cbs => cbs.traverse(cb => dao.checkpointAcceptanceService.accept(cb)(contextToReturn))
      ),
      s"dataResolver_resolveCheckpoints [${hashes}]",
      logger
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
        "observation",
        pool,
        priorityClient
      )(contextToReturn).head
        .flatTap(o => IO.delay(logger.debug(s"Resolving observation=${o.hash}")))
        .flatTap(o => dao.observationService.put(o, ConsensusStatus.Unknown)),
      s"dataResolver_resolveObservation [${hash}]",
      logger
    )

  def resolveDataByDistance[T <: AnyRef](
    hashes: List[String],
    endpoint: String,
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
    endpoint: String,
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
    endpoint: String,
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
          IO.raiseError[T](DataResolutionMaxErrors(endpoint, hash))
        case Nil =>
          IO.raiseError[T](
            DataResolutionOutOfPeers(dao.id.short, endpoint, List(hash), allPeers.map(_.id.short))
          )
        case head :: tail =>
          getData[T](hash, endpoint, head)(contextToReturn).flatMap {
            case Some(a) => IO.pure(a)
            case None =>
              IO.raiseError[T](DataResolutionNoneResponse(endpoint, hash, head))
          }.handleErrorWith {
            case e: DataResolutionMaxErrors  => IO.raiseError[T](e)
            case e: DataResolutionOutOfPeers => IO.raiseError[T](e)
            case e if tail.isEmpty           => IO.raiseError[T](e)
            case e: DataResolutionNoneResponse =>
              logger.warn(e.getMessage)
              makeAttempt(tail, allPeers, errorsSoFar + 1)
            case e =>
              logger.error(
                s"Unexpected error while resolving hash=${hash} on endpoint $endpoint with host=${head.client.hostPortForLogging}, and id ${head.client.id} trying next peer",
                e
              )
              makeAttempt(tail, allPeers, errorsSoFar + 1)
          }

      }

    for {
      _ <- IO.delay(logger.debug(s"Resolve $endpoint/$hash"))
      t <- makeAttempt(sortedPeers, sortedPeers)
    } yield t
  }

  private[p2p] def resolveBatchData[T <: AnyRef](
    hashes: List[String],
    endpoint: String,
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
        return IO.raiseError(DataResolutionOutOfPeers(dao.id.short, endpoint, hashes, sortedPeers.map(_.id.short)))
      }

      if (innerHashes.size == allHashes.size) {
        return response.pure[IO]
      }

      val peer = peers.head
      val filteredPeers = peers.filterNot(_ == peer)
      val requestHashes = allHashes.filterNot(innerHashes.toSet)

      getBatchData[T](requestHashes, endpoint, peer)(contextToReturn).flatMap { data =>
        logger.debug(
          s"Requested : ${peer.client.hostPortForLogging} : to endpoint = $endpoint : for hashes = ${requestHashes
            .mkString(",")} : and response hashes = ${data.size}"
        )
        makeAttempt(filteredPeers, allHashes, innerHashes ++ data.map(_._1), response ++ data.map(_._2))
      }
    }

    makeAttempt(sortedPeers, hashes).handleErrorWith(
      e => IO.delay(logger.error(s"Unexpected error while resolving hashes : ${e.getMessage}")) *> IO.raiseError(e)
    )
  }

  private[p2p] def getData[T <: AnyRef](
    hash: String,
    endpoint: String,
    peerApiClient: PeerApiClient
  )(contextToReturn: ContextShift[IO])(implicit apiTimeout: Duration, m: Manifest[T], dao: DAO): IO[Option[T]] =
    peerApiClient.client
      .getNonBlockingIO[Option[T]](s"$endpoint/$hash", timeout = apiTimeout)(contextToReturn)
      .onError {
        case _: SocketTimeoutException =>
          dao.observationService
            .put(Observation.create(peerApiClient.id, RequestTimeoutOnResolving(endpoint, List(hash)))(dao.keyPair))
            .void
      }

  private[p2p] def getBatchData[T <: AnyRef](
    hashes: List[String],
    endpoint: String,
    peerApiClient: PeerApiClient
  )(contextToReturn: ContextShift[IO])(implicit apiTimeout: Duration, m: Manifest[T], dao: DAO): IO[List[(String, T)]] =
    peerApiClient.client
      .postNonBlockingIO[List[(String, T)]](
        s"batch/$endpoint",
        hashes,
        timeout = apiTimeout
      )(contextToReturn)
      .onError {
        case _: SocketTimeoutException =>
          dao.observationService
            .put(Observation.create(peerApiClient.id, RequestTimeoutOnResolving(endpoint, hashes))(dao.keyPair))
            .void
      }

  private[p2p] def resolveDataByDistanceFlat[T <: AnyRef](
    hashes: List[String],
    endpoint: String,
    pool: List[PeerApiClient],
    priorityClient: Option[PeerApiClient] = None
  )(
    contextToReturn: ContextShift[IO]
  )(implicit apiTimeout: Duration = 3.seconds, m: Manifest[T], dao: DAO): IO[List[T]] =
    resolveDataByDistance[T](hashes, endpoint, pool, priorityClient)(contextToReturn).sequence

  private[p2p] def getPeersForResolving(dao: DAO): IO[List[PeerApiClient]] = {
    val peers = for {
      ready <- dao.peerInfo
      leaving <- dao.leavingPeers
    } yield (ready ++ leaving)
    peers.map(_.map(p => PeerApiClient(p._1, p._2.client)).toList)
  }

}

object DataResolver extends DataResolver

case class DataResolutionOutOfPeers(thisNode: String, endpoint: String, hashes: List[String], peers: Iterable[String])
    extends Exception(
      s"Node [$thisNode] run out of peers when resolving: $endpoint with hashes: $hashes following tried: $peers"
    )

case class DataResolutionNoneResponse(endpoint: String, hash: String, client: PeerApiClient)
    extends Exception(
      s"Failed to resolve hash=${hash} on endpoint $endpoint with host=${client.client.hostPortForLogging} and id ${client.id.short}, returned None"
    )
case class DataResolutionMaxErrors(endpoint: String, hash: String)
    extends Exception(
      s"Max errors threshold reached when resolving: $endpoint and hash: $hash aborting"
    )
