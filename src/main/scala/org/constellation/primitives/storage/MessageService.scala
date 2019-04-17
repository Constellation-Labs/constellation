package org.constellation.primitives.storage

import cats.effect.IO
import org.constellation.DAO
import org.constellation.primitives.{ChannelMessageMetadata, ChannelMetadata}

class MessageService(size: Int = 2000)(implicit dao: DAO)
    extends StorageService[ChannelMessageMetadata](size) {

  override def putSync(key: String, value: ChannelMessageMetadata): ChannelMessageMetadata = {
    dao.channelStorage.insert(value)
    super.putSync(key, value)
  }

  override def put(key: String, value: ChannelMessageMetadata): IO[ChannelMessageMetadata] = {
    dao.channelStorage.insert(value)
    super.put(key, value)
  }

}
class ChannelService(size: Int = 2000) extends StorageService[ChannelMetadata](size)
