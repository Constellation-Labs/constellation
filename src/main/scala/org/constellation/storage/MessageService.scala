package org.constellation.storage

import cats.effect.{Concurrent, Sync}
import cats.implicits._
import org.constellation.DAO
import org.constellation.primitives.{ChannelMessageMetadata, ChannelMetadata}
import org.constellation.storage.algebra.{Lookup, MerkleStorageAlgebra}

class MessageService[F[_]: Concurrent]()(implicit dao: DAO)
    extends MerkleStorageAlgebra[F, String, ChannelMessageMetadata] {
  val merklePool = new StorageService[F, Seq[String]]()
  val arbitraryPool = new StorageService[F, ChannelMessageMetadata]()
  val memPool = new StorageService[F, ChannelMessageMetadata]()

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
}

class ChannelService[F[_]: Concurrent]() extends StorageService[F, ChannelMetadata]()
