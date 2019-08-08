package org.constellation.storage

import cats.effect.Concurrent
import org.constellation.primitives.Schema.SignedObservationEdgeCache
import org.constellation.storage.algebra.MemPool

class SOEService[F[_]: Concurrent]() extends MemPool[F, String, SignedObservationEdgeCache]() {}
