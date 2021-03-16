package org.constellation.concurrency

import java.util.concurrent.ConcurrentHashMap

import cats.Monad
import cats.effect.Sync
import cats.syntax.all._
import io.chrisdavenport.mapref.MapRef

object MapRefUtils {

  implicit class MapRefOps[F[_]: Monad, K, V](val mapRef: MapRef[F, K, Option[V]]) {

    def toMap: F[Map[K, V]] =
      for {
        keys <- mapRef.keys
        keyValues <- keys.traverseFilter { id =>
          mapRef(id).get.map(_.map((id, _)))
        }
      } yield keyValues.toMap

    def clear: F[Unit] =
      for {
        keys <- mapRef.keys
        _ <- keys.traverse { id =>
          mapRef(id).set(none)
        }
      } yield ()
  }

  def ofConcurrentHashMap[F[_]: Sync, K, V](
    initialCapacity: Int = 16,
    loadFactor: Float = 0.75f,
    concurrencyLevel: Int = 16
  ): MapRef[F, K, Option[V]] =
    MapRef.fromConcurrentHashMap(new ConcurrentHashMap[K, V](initialCapacity, loadFactor, concurrencyLevel))

}
