package org.constellation.storage

import cats.effect.IO
import org.constellation.primitives.Schema.SignedObservationEdgeCache

class SOEService(expireAfterMinutes: Int) extends StorageService[IO, SignedObservationEdgeCache](expireAfterMinutes = Some(expireAfterMinutes))
