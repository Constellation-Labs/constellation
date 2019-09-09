package org.constellation.p2p
import cats.effect.{ContextShift, IO}
import cats.implicits._
import com.typesafe.scalalogging.StrictLogging
import constellation._
import org.constellation.DAO
import org.constellation.primitives.Schema.{CheckpointCache, SignedObservationEdgeCache}
import org.constellation.primitives.{ChannelMessageMetadata, Observation, TransactionCacheData}
import org.constellation.storage.ConsensusStatus
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
    resolveDataByDistance[ChannelMessageMetadata](
      List(hash),
      "message",
      pool,
      (t: ChannelMessageMetadata) =>
        dao.messageService.memPool.put(t.channelMessage.signedMessageData.hash, t).unsafeRunSync(),
      priorityClient
    )(contextToReturn).head
      .flatTap(m => IO.delay(logger.debug(s"Resolving message=${m.channelMessage.signedMessageData.hash}")))

  def resolveTransactionDefaults(
    hash: String,
    priorityClient: Option[PeerApiClient] = None
  )(contextToReturn: ContextShift[IO])(implicit apiTimeout: Duration = 3.seconds, dao: DAO): IO[TransactionCacheData] =
    getPeersForResolving(dao).flatMap(resolveTransaction(hash, _, priorityClient)(contextToReturn))

  def resolveTransaction(
    hash: String,
    pool: List[PeerApiClient],
    priorityClient: Option[PeerApiClient]
  )(contextToReturn: ContextShift[IO])(implicit apiTimeout: Duration = 3.seconds, dao: DAO): IO[TransactionCacheData] =
    resolveDataByDistance[TransactionCacheData](
      List(hash),
      "transaction",
      pool,
      (t: TransactionCacheData) => {
        dao.transactionService.put(t, ConsensusStatus.Unknown).unsafeRunSync()
      },
      priorityClient
    )(contextToReturn).head
      .flatTap(tx => IO.delay(logger.debug(s"Resolving transaction=${tx.hash}")))

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
    resolveDataByDistance[CheckpointCache](
      List(hash),
      "checkpoint",
      pool,
      (t: CheckpointCache) => t.checkpointBlock.foreach(cb => cb.store(t)),
      priorityClient
    )(contextToReturn).head.flatTap(
      cb => IO.delay(logger.debug(s"Resolving checkpoint=$hash with baseHash=${cb.checkpointBlock.map(_.baseHash)}"))
    )

  def resolveSoeDefaults(
    hashes: List[String],
    priorityClient: Option[PeerApiClient] = None
  )(
    contextToReturn: ContextShift[IO]
  )(implicit apiTimeout: Duration = 3.seconds, dao: DAO): IO[List[SignedObservationEdgeCache]] =
    getPeersForResolving(dao).flatMap(resolveSoe(hashes, _, priorityClient)(contextToReturn))

  def resolveSoe(
    hashes: List[String],
    pool: List[PeerApiClient],
    priorityClient: Option[PeerApiClient] = None
  )(
    contextToReturn: ContextShift[IO]
  )(implicit apiTimeout: Duration = 3.seconds, dao: DAO): IO[List[SignedObservationEdgeCache]] =
    resolveDataByDistance[SignedObservationEdgeCache](
      hashes,
      "soe",
      pool,
      (t: SignedObservationEdgeCache) => {
        dao.soeService.put(t.signedObservationEdge.hash, t).unsafeRunSync()
      },
      priorityClient
    )(contextToReturn).sequence

  def resolveCheckpoints(
    hashes: List[String],
    pool: List[PeerApiClient],
    priorityClient: Option[PeerApiClient] = None
  )(contextToReturn: ContextShift[IO])(implicit apiTimeout: Duration = 3.seconds, dao: DAO): IO[List[CheckpointCache]] =
    resolveDataByDistanceFlat[CheckpointCache](
      hashes,
      "checkpoint",
      pool,
      (t: CheckpointCache) => {
        t.checkpointBlock.foreach(cb => cb.store(t))
      },
      priorityClient
    )(contextToReturn)

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
    resolveDataByDistance[Observation](
      List(hash),
      "observation",
      pool,
      (t: Observation) => {
        dao.observationService.put(t, ConsensusStatus.Unknown).unsafeRunSync()
      },
      priorityClient
    )(contextToReturn).head.flatTap(o => IO.delay(logger.debug(s"Resolving observation=${o.hash}")))

  private[p2p] def resolveDataByDistance[T <: AnyRef](
    hashes: List[String],
    endpoint: String,
    pool: List[PeerApiClient],
    store: T => Any,
    priorityClient: Option[PeerApiClient] = None
  )(
    contextToReturn: ContextShift[IO]
  )(implicit apiTimeout: Duration = 3.seconds, m: Manifest[T], dao: DAO): List[IO[T]] =
    hashes.map { hash =>
      resolveData[T](hash, endpoint, priorityClient.toList ++ pool.sortBy { p =>
        Distance.calculate(hash, p.id)
      }, store)(contextToReturn)
    }

  private[p2p] def resolveData[T <: AnyRef](
    hash: String,
    endpoint: String,
    sortedPeers: List[PeerApiClient],
    store: T => Any,
    maxErrors: Int = 25
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
            DataResolutionOutOfPeers(dao.id.short, endpoint, hash, allPeers.map(_.id.short))
          )
        case head :: tail =>
          getData[T](hash, endpoint, head, store)(contextToReturn).flatMap {
            case Some(a) => IO.pure(a)
            case None    => makeAttempt(tail, allPeers, errorsSoFar + 1)
          }.handleErrorWith {
            case e if tail.isEmpty => IO.raiseError[T](e)
            case e =>
              logger.error(
                s"Failed to resolve with host=${head.client.hostPortForLogging}, trying next peer",
                e
              )
              makeAttempt(tail, allPeers, errorsSoFar + 1)
          }

      }

    for {
      _ <- IO.delay(logger.debug(s"Resolve $endpoint/$hash"))
      t <- makeAttempt(sortedPeers, sortedPeers)
      _ <- IO.delay(store(t))
      _ <- IO.delay(logger.debug(s"Stored resolved $endpoint with $hash"))
    } yield t
  }

  private[p2p] def getData[T <: AnyRef](
    hash: String,
    endpoint: String,
    peerApiClient: PeerApiClient,
    store: T => Any
  )(contextToReturn: ContextShift[IO])(implicit apiTimeout: Duration, m: Manifest[T], dao: DAO): IO[Option[T]] =
    peerApiClient.client
      .getNonBlockingIO[Option[T]](s"$endpoint/$hash", timeout = apiTimeout)(contextToReturn)

  private[p2p] def resolveDataByDistanceFlat[T <: AnyRef](
    hashes: List[String],
    endpoint: String,
    pool: List[PeerApiClient],
    store: T => Any,
    priorityClient: Option[PeerApiClient] = None
  )(
    contextToReturn: ContextShift[IO]
  )(implicit apiTimeout: Duration = 3.seconds, m: Manifest[T], dao: DAO): IO[List[T]] =
    resolveDataByDistance[T](hashes, endpoint, pool, store, priorityClient)(contextToReturn).sequence

  private[p2p] def getPeersForResolving(dao: DAO): IO[List[PeerApiClient]] = {
    val peers = for {
      ready <- dao.readyPeers
      leaving <- dao.leavingPeers
    } yield (ready ++ leaving)
    peers.map(_.map(p => PeerApiClient(p._1, p._2.client)).toList)
  }

}

object DataResolver extends DataResolver

case class DataResolutionOutOfPeers(thisNode: String, endpoint: String, hash: String, peers: Iterable[String])
    extends Exception(
      s"node [$thisNode] Run out of peers when resolving: $endpoint with hash: $hash following tried: $peers"
    )

case class DataResolutionMaxErrors(endpoint: String, hash: String)
    extends Exception(
      s"Max errors threshold reached when resolving: $endpoint and hash: $hash aborting"
    )
