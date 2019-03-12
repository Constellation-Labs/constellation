package org.constellation.primitives.storage

import org.constellation.primitives.Schema.SignedObservationEdgeCache

class SOEService(size: Int = 2000) extends StorageService[SignedObservationEdgeCache](size)
