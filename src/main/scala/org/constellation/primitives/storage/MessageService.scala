package org.constellation.primitives.storage

import cats.effect.IO
import org.constellation.primitives.{ChannelMessageMetadata, ChannelMetadata}

class MessageService(size: Int = 2000)  {
  val merklePool = new StorageService[Seq[String]](size)
  val memPool = new StorageService[ChannelMessageMetadata](size)

  def lookup: String => IO[Option[ChannelMessageMetadata]] =
    DbStorage.extendedLookup[String, ChannelMessageMetadata](List(memPool))

}
class ChannelService(size: Int = 2000) extends StorageService[ChannelMetadata](size)
