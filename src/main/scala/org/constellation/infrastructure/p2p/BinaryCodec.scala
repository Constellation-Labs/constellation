package org.constellation.infrastructure.p2p

import cats.effect.Sync
import org.constellation.serialization.KryoSerializer
import org.http4s._

object BinaryCodec {
  implicit def encoder[F[_], A <: AnyRef]: EntityEncoder[F, A] =
    EntityEncoder.byteArrayEncoder[F].contramap[A] { anyRef =>
      KryoSerializer.serializeAnyRef(anyRef)
    }

  implicit def decoder[F[_]: Sync, A <: AnyRef]: EntityDecoder[F, A] =
    EntityDecoder.byteArrayDecoder.map { bytes =>
      KryoSerializer.deserializeCast[A](bytes)
    }
}
