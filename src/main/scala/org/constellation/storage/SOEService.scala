package org.constellation.storage

import cats.effect.Sync
import cats.implicits._
import org.constellation.primitives.Schema.SignedObservationEdgeCache

class SOEService[F[_]: Sync]() extends StorageService[F, SignedObservationEdgeCache]() {
  override def put(key: String, value: SignedObservationEdgeCache): F[SignedObservationEdgeCache] = {
    super.put(key, value)
//      .flatTap(_ => Sync[F].delay(println(s"---- ---- PUT $key to SOEService")))
  }

  override def update(
    key: String,
    updateFunc: SignedObservationEdgeCache => SignedObservationEdgeCache,
    empty: => SignedObservationEdgeCache
  ): F[SignedObservationEdgeCache] =
    super.update(key, updateFunc, empty)
//      .flatTap(_ => Sync[F].delay(println(s"---- ---- UPDATE $key to SOEService")))

  override def update(
    key: String,
    updateFunc: SignedObservationEdgeCache => SignedObservationEdgeCache
  ): F[Option[SignedObservationEdgeCache]] =
    super.update(key, updateFunc)
//      .flatTap(_ => Sync[F].delay(println(s"---- ---- UPDATE no empty $key to SOEService")))
}
