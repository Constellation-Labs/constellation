package org.constellation.storage

import cats.effect.IO
import cats.implicits._
import org.constellation.DAO
import org.constellation.primitives.{ChannelMessageMetadata, ChannelMetadata}
import org.constellation.storage.algebra.{Lookup, MerkleStorageAlgebra}

class MessageService()(implicit dao: DAO) extends MerkleStorageAlgebra[IO, String, ChannelMessageMetadata] {
  val merklePool = new StorageService[IO, Seq[String]]()
  val arbitraryPool = new StorageService[IO, ChannelMessageMetadata]()
  val memPool = new StorageService[IO, ChannelMessageMetadata]()

  def put(key: String, value: ChannelMessageMetadata): IO[ChannelMessageMetadata] = {
    memPool.put(key, value)
      .flatTap { _ => IO { dao.channelStorage.insert(value) } }
  }

  def lookup(key: String): IO[Option[ChannelMessageMetadata]] =
    Lookup.extendedLookup[IO, String, ChannelMessageMetadata](List(memPool))(key)

  def contains(key: String): IO[Boolean] =
    Lookup.extendedContains[IO, String, ChannelMessageMetadata](List(memPool))(key)

  override def findHashesByMerkleRoot(merkleRoot: String): IO[Option[Seq[String]]] =
    merklePool.lookup(merkleRoot)
}
class ChannelService() extends StorageService[IO, ChannelMetadata]()
