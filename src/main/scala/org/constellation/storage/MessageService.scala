package org.constellation.storage

import cats.effect.{Concurrent, Sync}
import cats.implicits._
import org.constellation.{ConstellationExecutionContext, DAO}
import org.constellation.primitives.{ChannelMessageMetadata, ChannelMetadata}
import org.constellation.storage.algebra.{Lookup, MerkleStorageAlgebra}

class MessageService[F[_]: Concurrent]()(implicit dao: DAO)
    extends MerkleStorageAlgebra[F, String, ChannelMessageMetadata] {

  val merklePool = new ConcurrentStorageService[F, Seq[String]](
    ConstellationExecutionContext.createSemaphore(),
    "message_merkle_pool".some
  )

  val arbitraryPool = new ConcurrentStorageService[F, ChannelMessageMetadata](
    ConstellationExecutionContext.createSemaphore(),
    "message_arbitrary_pool".some
  )

  val memPool = new ConcurrentStorageService[F, ChannelMessageMetadata](
    ConstellationExecutionContext.createSemaphore(),
    "message_mem_pool".some
  )

  def put(key: String, value: ChannelMessageMetadata): F[ChannelMessageMetadata] =
    memPool
      .put(key, value)
      .flatTap { _ =>
        Sync[F].delay(dao.channelStorage.insert(value))
      }

  def lookup(key: String): F[Option[ChannelMessageMetadata]] =
    Lookup.extendedLookup[F, String, ChannelMessageMetadata](List(memPool))(key)

  def contains(key: String): F[Boolean] =
    Lookup.extendedContains[F, String, ChannelMessageMetadata](List(memPool))(key)

  def findHashesByMerkleRoot(merkleRoot: String): F[Option[Seq[String]]] =
    merklePool.lookup(merkleRoot)

  override def addMerkle(merkleRoot: String, keys: Seq[String]): F[Seq[String]] = merklePool.put(merkleRoot, keys)

}

class ChannelService[F[_]: Concurrent]() extends StorageService[F, ChannelMetadata]("channel_mem_pool".some)
