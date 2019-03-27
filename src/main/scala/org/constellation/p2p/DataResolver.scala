package org.constellation.p2p
import cats.effect.IO
import cats.implicits._
import constellation._
import org.constellation.DAO
import org.constellation.primitives.Schema.CheckpointCacheData
import org.constellation.primitives.{ChannelMessageMetadata, ChannelProof, TransactionCacheData}
import org.constellation.util.APIClient

import scala.concurrent.duration._

object DataResolver {

  def resolveMessages(hash: String,
                      pool: Iterable[APIClient],
                      priorityClient: Option[APIClient] = None)(
    implicit apiTimeout: Duration = 3.seconds,
    dao: DAO
  ): IO[Option[ChannelMessageMetadata]] = {
    resolveDataByDistance[ChannelProof](
      List(hash),
      "channel",
      pool,
      (t: ChannelProof) => {
        dao.messageService.put(t.channelMessageMetadata.channelMessage.signedMessageData.hash,
                               t.channelMessageMetadata)
      },
      priorityClient
    ).head.map(_.map(_.channelMessageMetadata))

  }

  def resolveTransactions(hash: String,
                          pool: Iterable[APIClient],
                          priorityClient: Option[APIClient] = None)(
    implicit apiTimeout: Duration = 3.seconds,
    dao: DAO
  ): IO[Option[TransactionCacheData]] = {
    resolveDataByDistance[TransactionCacheData](
      List(hash),
      "transaction",
      pool,
      (t: TransactionCacheData) => {
        dao.transactionService.memPool.put(t.transaction.hash, t)
      },
      priorityClient
    ).head
  }
  def resolveCheckpoint(hash: String,
                        pool: Iterable[APIClient],
                        priorityClient: Option[APIClient] = None)(
    implicit apiTimeout: Duration = 3.seconds,
    dao: DAO
  ): IO[Option[CheckpointCacheData]] = {
    resolveDataByDistance[CheckpointCacheData](List(hash),
                                               "checkpoint",
                                               pool,
                                               (t: CheckpointCacheData) => {
                                                 t.checkpointBlock.foreach(cb => cb.store(t))
                                               },
                                               priorityClient).head
  }

  def resolveDataByDistanceFlat[T <: AnyRef](hashes: List[String],
                                             endpoint: String,
                                             pool: Iterable[APIClient],
                                             store: T => Any,
                                             priorityClient: Option[APIClient] = None)(
    implicit apiTimeout: Duration = 3.seconds,
    m: Manifest[T],
    dao: DAO
  ): IO[List[Option[T]]] = {
    resolveDataByDistance[T](hashes, endpoint, pool, store, priorityClient).sequence
  }

  def resolveDataByDistance[T <: AnyRef](hashes: List[String],
                                         endpoint: String,
                                         pool: Iterable[APIClient],
                                         store: T => Any,
                                         priorityClient: Option[APIClient] = None)(
    implicit apiTimeout: Duration = 3.seconds,
    m: Manifest[T],
    dao: DAO
  ): List[IO[Option[T]]] = {
    hashes.map { hash =>
      val dataHash = BigInt(hash.getBytes)

      resolveData[T](hash, endpoint, priorityClient.toSeq ++ pool.toSeq.sortBy { p =>
        BigInt(p.id.hex.getBytes()) ^ dataHash
      }, store)
    }
  }

  def resolveData[T <: AnyRef](hash: String,
                               endpoint: String,
                               sortedPeers: Iterable[APIClient],
                               store: T => Any,
                               maxErrors: Int = 10)(
    implicit apiTimeout: Duration = 3.seconds,
    m: Manifest[T],
    dao: DAO
  ): IO[Option[T]] = {

    def makeAttempt(sortedPeers: Iterable[APIClient], errorsSoFar: Int = 0): IO[Option[T]] = {
      sortedPeers match {
        case _ if errorsSoFar >= maxErrors =>
          IO.raiseError(
            new RuntimeException(
              s"Max errors threshold reached when resolving: $endpoint and hash: $hash aborting"
            )
          )
        case Nil =>
          IO.raiseError(new RuntimeException(s"Unable to download $endpoint from empty peer list"))
        case head :: tail =>
          getData[T](hash, endpoint, head, store)
            .handleErrorWith {
              case e if tail.isEmpty => IO.raiseError(e)
              case _                 => makeAttempt(sortedPeers.tail, errorsSoFar + 1)
            }
            .flatMap { response =>
              if (response.isEmpty) makeAttempt(sortedPeers.tail, errorsSoFar + 1)
              else IO.pure(response)
            }
      }
    }
    makeAttempt(sortedPeers)
  }
  private def getData[T <: AnyRef](hash: String,
                                   endpoint: String,
                                   client: APIClient,
                                   store: T => Any)(
    implicit apiTimeout: Duration,
    m: Manifest[T],
    dao: DAO
  ): IO[Option[T]] = IO.fromFuture {
    IO {
      client
        .getNonBlocking[Option[T]](s"$endpoint/$hash", timeout = apiTimeout)
        .map { x =>
          x.foreach(store)
          x
        }(dao.edgeExecutionContext)
    }
  }

}
