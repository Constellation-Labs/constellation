package org.constellation.storage

import cats.effect.Sync
import org.constellation.primitives.Schema.SignedObservationEdgeCache

class SOEService[F[_]: Sync]() extends StorageService[F, SignedObservationEdgeCache]()
