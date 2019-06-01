package org.constellation.storage

import cats.effect.IO
import org.constellation.primitives.Schema.SignedObservationEdgeCache

class SOEService() extends StorageService[IO, SignedObservationEdgeCache]()
