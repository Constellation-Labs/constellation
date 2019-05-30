package org.constellation.primitives.storage

import cats.effect.IO
import org.constellation.DAO
import org.constellation.primitives.{ChannelMessageMetadata, ChannelMetadata}

class MessageService()(implicit dao: DAO) extends MerkleService[String, ChannelMessageMetadata] {
  val merklePool = new StorageService[Seq[String]]()
  val arbitraryPool = new StorageService[ChannelMessageMetadata]()
  val memPool = new StorageService[ChannelMessageMetadata]()

  def putSync(key: String, value: ChannelMessageMetadata): ChannelMessageMetadata = {
    dao.channelStorage.insert(value)
    memPool.putSync(key, value)
  }

  def put(key: String, value: ChannelMessageMetadata): IO[ChannelMessageMetadata] = {
    dao.channelStorage.insert(value)
    memPool.put(key, value)
  }


  override def lookup: String => IO[Option[ChannelMessageMetadata]] =
    DbStorage.extendedLookup[String, ChannelMessageMetadata](List(memPool))

  def contains: String ⇒ IO[Boolean] =
    DbStorage.extendedContains[String, ChannelMessageMetadata](List(memPool))

  override def findHashesByMerkleRoot(merkleRoot: String): IO[Option[Seq[String]]] =
    merklePool.get(merkleRoot)
}
class ChannelService() extends StorageService[ChannelMetadata]()
