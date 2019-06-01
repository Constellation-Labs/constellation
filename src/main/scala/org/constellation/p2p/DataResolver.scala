package org.constellation.p2p
import cats.effect.IO
import cats.implicits._
import com.typesafe.scalalogging.StrictLogging
import constellation._
import org.constellation.DAO
import org.constellation.primitives.Schema.CheckpointCache
import org.constellation.storage.TransactionStatus
import org.constellation.primitives.{ChannelMessageMetadata, TransactionCacheData}
import org.constellation.util.PeerApiClient

import scala.concurrent.duration._

class DataResolver extends StrictLogging {

  def resolveMessagesDefaults(
    hash: String,
    priorityClient: Option[PeerApiClient] = None
  )(implicit apiTimeout: Duration = 3.seconds, dao: DAO): IO[Option[ChannelMessageMetadata]] = {
    resolveMessages(hash, getReadyPeers(dao), priorityClient)
  }

  def resolveMessages(
    hash: String,
    pool: Iterable[PeerApiClient],
    priorityClient: Option[PeerApiClient]
  )(implicit apiTimeout: Duration = 3.seconds, dao: DAO): IO[Option[ChannelMessageMetadata]] = {
    logger.info(s"Resolve message=$hash")
    resolveDataByDistance[ChannelMessageMetadata](
      List(hash),
      "message",
      pool,
      (t: ChannelMessageMetadata) =>
        dao.messageService.memPool.put(t.channelMessage.signedMessageData.hash, t).unsafeRunSync(),
      priorityClient
    ).head

  }

  def resolveDataByDistance[T <: AnyRef](
    hashes: List[String],
    endpoint: String,
    pool: Iterable[PeerApiClient],
    store: T => Any,
    priorityClient: Option[PeerApiClient] = None
  )(implicit apiTimeout: Duration = 3.seconds, m: Manifest[T], dao: DAO): List[IO[Option[T]]] = {
    hashes.map { hash =>
      val dataHash = BigInt(hash.getBytes)

      resolveData[T](hash, endpoint, priorityClient.toSeq ++ pool.toSeq.sortBy { p =>
        BigInt(p.id.hex.getBytes()) ^ dataHash
      }, store)
    }
  }

  def resolveData[T <: AnyRef](
    hash: String,
    endpoint: String,
    sortedPeers: Iterable[PeerApiClient],
    store: T => Any,
    maxErrors: Int = 10
  )(implicit apiTimeout: Duration = 3.seconds, m: Manifest[T], dao: DAO): IO[Option[T]] = {
    val storeIO = (t: T) => IO { store(t) }.void

    def makeAttempt(innerPeers: Iterable[PeerApiClient], errorsSoFar: Int = 0): IO[Option[T]] = {
      innerPeers match {
        case _ if errorsSoFar >= maxErrors =>
          IO.raiseError(DataResolutionMaxErrors(endpoint, hash))
        case Nil =>
          IO.raiseError(
            DataResolutionOutOfPeers(dao.id.short, endpoint, hash, sortedPeers.map(_.id.short))
          )
        case head :: tail =>
          getData[T](hash, endpoint, head, store)
            .handleErrorWith {
              case e if tail.isEmpty => IO.raiseError(e)
              case e =>
                logger.error(
                  s"Failed to resolve with host=${head.client.hostPortForLogging}, trying next peer",
                  e
                )
                makeAttempt(tail, errorsSoFar + 1)
            }
            .flatTap(_.map(storeIO).getOrElse(IO.unit))
            .flatMap { response =>
              if (response.isEmpty) {
                logger.warn(
                  s"Empty response resolving with host=${head.client.hostPortForLogging}, trying next peer"
                )
                makeAttempt(tail, errorsSoFar + 1)
              } else {
                IO.pure(response)
              }
            }
      }
    }
    makeAttempt(sortedPeers)
  }

  private def getData[T <: AnyRef](
    hash: String,
    endpoint: String,
    peerApiClient: PeerApiClient,
    store: T => Any
  )(implicit apiTimeout: Duration, m: Manifest[T], dao: DAO): IO[Option[T]] = {
    peerApiClient.client
      .getNonBlockingIO[Option[T]](s"$endpoint/$hash", timeout = apiTimeout)
  }

  def resolveTransactionsDefaults(
    hash: String,
    priorityClient: Option[PeerApiClient] = None
  )(implicit apiTimeout: Duration = 3.seconds, dao: DAO): IO[Option[TransactionCacheData]] = {
    resolveTransactions(hash, getReadyPeers(dao), priorityClient)
  }

  def resolveTransactions(
    hash: String,
    pool: Iterable[PeerApiClient],
    priorityClient: Option[PeerApiClient]
  )(implicit apiTimeout: Duration = 3.seconds, dao: DAO): IO[Option[TransactionCacheData]] = {
    logger.info(s"Resolve transaction=$hash")
    resolveDataByDistance[TransactionCacheData](
      List(hash),
      "transaction",
      pool,
      (t: TransactionCacheData) => {
        dao.transactionService.put(t, TransactionStatus.Unknown).unsafeRunSync()
      },
      priorityClient
    ).head
  }

  def resolveCheckpointDefaults(
    hash: String,
    priorityClient: Option[PeerApiClient] = None
  )(implicit apiTimeout: Duration = 3.seconds, dao: DAO): IO[Option[CheckpointCache]] = {
    resolveDataByDistance[CheckpointCache](
      List(hash),
      "checkpoint",
      getReadyPeers(dao),
      (t: CheckpointCache) => t.checkpointBlock.foreach(cb => cb.store(t)),
      priorityClient
    ).head
  }

  def resolveCheckpoint(
    hash: String,
    pool: Iterable[PeerApiClient],
    priorityClient: Option[PeerApiClient]
  )(implicit apiTimeout: Duration = 3.seconds, dao: DAO): IO[Option[CheckpointCache]] = {
    logger.info(s"Resolve cb=$hash")
    resolveDataByDistance[CheckpointCache](
      List(hash),
      "checkpoint",
      pool,
      (t: CheckpointCache) => t.checkpointBlock.foreach(cb => cb.store(t)),
      priorityClient
    ).head
  }

  def resolveCheckpoints(
    hashes: List[String],
    pool: Iterable[PeerApiClient],
    priorityClient: Option[PeerApiClient] = None
  )(implicit apiTimeout: Duration = 3.seconds, dao: DAO): IO[List[Option[CheckpointCache]]] = {
    resolveDataByDistanceFlat[CheckpointCache](
      hashes,
      "checkpoint",
      pool,
      (t: CheckpointCache) => t.checkpointBlock.foreach(cb => cb.store(t)),
      priorityClient
    )
  }

  def resolveDataByDistanceFlat[T <: AnyRef](
    hashes: List[String],
    endpoint: String,
    pool: Iterable[PeerApiClient],
    store: T => Any,
    priorityClient: Option[PeerApiClient] = None
  )(implicit apiTimeout: Duration = 3.seconds, m: Manifest[T], dao: DAO): IO[List[Option[T]]] = {
    resolveDataByDistance[T](hashes, endpoint, pool, store, priorityClient).sequence
  }

  def getReadyPeers(dao: DAO): Iterable[PeerApiClient] = {
    dao.readyPeers.unsafeRunSync().map(p => PeerApiClient(p._1, p._2.client))
  }

}

object DataResolver extends DataResolver

case class DataResolutionOutOfPeers(thisNode: String,
                                    endpoint: String,
                                    hash: String,
                                    peers: Iterable[String])
    extends Exception(
      s"node [$thisNode] Run out of peers when resolving: $endpoint with hash: $hash following tried: $peers"
    )

case class DataResolutionMaxErrors(endpoint: String, hash: String)
    extends Exception(
      s"Max errors threshold reached when resolving: $endpoint and hash: $hash aborting"
    )
