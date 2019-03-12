package org.constellation.primitives.storage

import org.constellation.primitives.{ChannelMessageMetadata, ChannelMetadata}

class MessageService(size: Int = 2000) extends StorageService[ChannelMessageMetadata](size)
class ChannelService(size: Int = 2000) extends StorageService[ChannelMetadata](size)
