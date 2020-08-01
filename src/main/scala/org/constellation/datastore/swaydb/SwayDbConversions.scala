package org.constellation.datastore.swaydb
import org.constellation.consensus.Snapshot
import org.constellation.primitives.Schema.CheckpointCacheMetadata
import org.constellation.primitives.TransactionCacheData
import org.constellation.serializer.KryoSerializer
import swaydb.data.slice.Slice
import swaydb.serializers.Serializer

object SwayDbConversions {

  class SwayDbKryoSerializer[T <: AnyRef] extends Serializer[T] {

    def write(data: T): Slice[Byte] =
      Slice(KryoSerializer.serializeAnyRef(data))

    def read(data: Slice[Byte]): T =
      KryoSerializer.deserializeCast[T](data.toArray)
  }

  implicit object TransactionSerializer extends SwayDbKryoSerializer[TransactionCacheData]
  implicit object CheckpointBlockSerializer extends SwayDbKryoSerializer[CheckpointCacheMetadata]
  implicit object SnapshotSerializer extends SwayDbKryoSerializer[Snapshot]
}
