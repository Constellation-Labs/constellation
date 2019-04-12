package org.constellation.datastore.swaydb
import cats.effect.IO
import org.constellation.consensus.Snapshot
import org.constellation.primitives.Schema.{CheckpointCacheData, CheckpointCacheFullData}
import org.constellation.primitives.TransactionCacheData
import org.constellation.serializer.KryoSerializer
import swaydb.data.slice.Slice
import swaydb.serializers.Serializer

object SwayDbConversions {
  implicit def swayIOtoIO[A](io: swaydb.data.IO[A]): IO[A] = IO(io.get)

  class SwayDbKryoSerializer[T <: AnyRef] extends Serializer[T] {
    def write(data: T): Slice[Byte] = {
      Slice(KryoSerializer.serializeAnyRef(data))
    }

    def read(data: Slice[Byte]): T = {
      KryoSerializer.deserializeCast[T](data.toArray)
    }
  }

  implicit object TransactionSerializer extends SwayDbKryoSerializer[TransactionCacheData]
  implicit object CheckpointBlockSerializer extends SwayDbKryoSerializer[CheckpointCacheData]
  implicit object SnapshotSerializer extends SwayDbKryoSerializer[Snapshot]
}
