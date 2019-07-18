package org.constellation.p2p
import cats.effect.IO
import cats.implicits._
import com.typesafe.scalalogging.StrictLogging
import constellation._
import org.constellation.DAO
import org.constellation.primitives.Schema.{CheckpointCache, SignedObservationEdgeCache}
import org.constellation.primitives.{ChannelMessageMetadata, TransactionCacheData}
import org.constellation.storage.transactions.TransactionStatus
import org.constellation.util.{Distance, PeerApiClient}

import scala.concurrent.duration._

class DataResolver extends StrictLogging {

  def resolveMessagesDefaults(
    hash: String,
    priorityClient: Option[PeerApiClient] = None
  )(implicit apiTimeout: Duration = 3.seconds, dao: DAO): IO[ChannelMessageMetadata] =
    getReadyPeers(dao).flatMap(resolveMessages(hash, _, priorityClient))

  def resolveMessages(
    hash: String,
    pool: List[PeerApiClient],
    priorityClient: Option[PeerApiClient]
  )(implicit apiTimeout: Duration = 3.seconds, dao: DAO): IO[ChannelMessageMetadata] =
    resolveDataByDistance[ChannelMessageMetadata](
      List(hash),
      "message",
      pool,
      (t: ChannelMessageMetadata) =>
        dao.messageService.memPool.put(t.channelMessage.signedMessageData.hash, t).unsafeRunSync(),
      priorityClient
    ).head

  def resolveDataByDistance[T <: AnyRef](
    hashes: List[String],
    endpoint: String,
    pool: List[PeerApiClient],
    store: T => Any,
    priorityClient: Option[PeerApiClient] = None
  )(implicit apiTimeout: Duration = 3.seconds, m: Manifest[T], dao: DAO): List[IO[T]] =
    hashes.map { hash =>
      resolveData[T](hash, endpoint, priorityClient.toList ++ pool.sortBy { p =>
        Distance.calculate(hash, p.id)
      }, store)
    }

  def resolveData[T <: AnyRef](
    hash: String,
    endpoint: String,
    sortedPeers: List[PeerApiClient],
    store: T => Any,
    maxErrors: Int = 10
  )(implicit apiTimeout: Duration = 3.seconds, m: Manifest[T], dao: DAO): IO[T] = {

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
          getData[T](hash, endpoint, head, store).flatMap {
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

  private def getData[T <: AnyRef](
    hash: String,
    endpoint: String,
    peerApiClient: PeerApiClient,
    store: T => Any
  )(implicit apiTimeout: Duration, m: Manifest[T], dao: DAO): IO[Option[T]] =
    peerApiClient.client
      .getNonBlockingIO[Option[T]](s"$endpoint/$hash", timeout = apiTimeout)

  def resolveTransactionsDefaults(
    hash: String,
    priorityClient: Option[PeerApiClient] = None
  )(implicit apiTimeout: Duration = 3.seconds, dao: DAO): IO[TransactionCacheData] =
    getReadyPeers(dao).flatMap(resolveTransactions(hash, _, priorityClient))

  def resolveTransactions(
    hash: String,
    pool: List[PeerApiClient],
    priorityClient: Option[PeerApiClient]
  )(implicit apiTimeout: Duration = 3.seconds, dao: DAO): IO[TransactionCacheData] =
    resolveDataByDistance[TransactionCacheData](
      List(hash),
      "transaction",
      pool,
      (t: TransactionCacheData) => {
        dao.transactionService.put(t, TransactionStatus.Unknown).unsafeRunSync()
      },
      priorityClient
    ).head

  def resolveCheckpointDefaults(
    hash: String,
    priorityClient: Option[PeerApiClient] = None
  )(implicit apiTimeout: Duration = 3.seconds, dao: DAO): IO[CheckpointCache] =
    getReadyPeers(dao).flatMap(
      resolveDataByDistance[CheckpointCache](
        List(hash),
        "checkpoint",
        _,
        (t: CheckpointCache) => t.checkpointBlock.foreach(cb => cb.store(t)),
        priorityClient
      ).head
    )

  def resolveCheckpoint(
    hash: String,
    pool: List[PeerApiClient],
    priorityClient: Option[PeerApiClient] = None
  )(implicit apiTimeout: Duration = 3.seconds, dao: DAO): IO[CheckpointCache] =
    resolveDataByDistance[CheckpointCache](
      List(hash),
      "checkpoint",
      pool,
      (t: CheckpointCache) => t.checkpointBlock.foreach(cb => cb.store(t)),
      priorityClient
    ).head

  def resolveSoe(
    hashes: List[String],
    pool: List[PeerApiClient],
    priorityClient: Option[PeerApiClient] = None
  )(implicit apiTimeout: Duration = 3.seconds, dao: DAO): IO[List[SignedObservationEdgeCache]] =
    resolveDataByDistance[SignedObservationEdgeCache](
      hashes,
      "soe",
      pool,
      (t: SignedObservationEdgeCache) => {
        dao.soeService.put(t.signedObservationEdge.hash, t).unsafeRunSync()
      },
      priorityClient
    ).sequence

  def resolveCheckpoints(
    hashes: List[String],
    pool: List[PeerApiClient],
    priorityClient: Option[PeerApiClient] = None
  )(implicit apiTimeout: Duration = 3.seconds, dao: DAO): IO[List[CheckpointCache]] =
    resolveDataByDistanceFlat[CheckpointCache](
      hashes,
      "checkpoint",
      pool,
      (t: CheckpointCache) => {
        t.checkpointBlock.foreach(cb => cb.store(t))
      },
      priorityClient
    )

  def resolveDataByDistanceFlat[T <: AnyRef](
    hashes: List[String],
    endpoint: String,
    pool: List[PeerApiClient],
    store: T => Any,
    priorityClient: Option[PeerApiClient] = None
  )(implicit apiTimeout: Duration = 3.seconds, m: Manifest[T], dao: DAO): IO[List[T]] =
    resolveDataByDistance[T](hashes, endpoint, pool, store, priorityClient).sequence

  def getReadyPeers(dao: DAO): IO[List[PeerApiClient]] =
    dao.readyPeers.map(_.map(p => PeerApiClient(p._1, p._2.client)).toList)

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
