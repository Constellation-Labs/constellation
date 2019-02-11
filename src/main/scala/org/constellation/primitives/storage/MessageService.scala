package org.constellation.primitives.storage

import org.constellation.primitives.ChannelMessageMetadata

class MessageService(size: Int = 50000) extends StorageService[ChannelMessageMetadata](size)
