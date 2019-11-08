package org.constellation.serializer

import com.google.common.hash.Hashing
import org.constellation.schema.HashGenerator

case class KryoHashGenerator() extends HashGenerator {

  override def hash(anyRef: AnyRef): String =
    Hashing.sha256().hashBytes(KryoSerializer.serializeAnyRef(anyRef)).toString
}
