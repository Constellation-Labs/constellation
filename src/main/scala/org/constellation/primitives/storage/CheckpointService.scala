package org.constellation.primitives.storage

import org.constellation.primitives.Schema.CheckpointCacheData

// TODO: Make separate one for acceptedCheckpoints vs nonresolved etc.
class CheckpointService(size: Int = 50000) extends StorageService[CheckpointCacheData](size)
