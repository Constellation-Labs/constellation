package org.constellation.schema.serialization

import cats.Monoid
import com.twitter.chill.{IKryoRegistrar, Kryo}
import enumeratum.{Enum, EnumEntry}
import org.constellation.schema.serialization.ExplicitKryoRegistrar.KryoSerializer
import org.constellation.schema.serialization.ExplicitKryoRegistrar.KryoSerializer.{
  CompatibleSerializer,
  DefaultSerializer
}

// TODO: We shoud use Map[Int, Class[_]] but for it was just easier to use Set and keep the order of registration as it was before
case class ExplicitKryoRegistrar(classes: Set[(Class[_], Int, KryoSerializer)]) extends IKryoRegistrar {

  def apply(k: Kryo): Unit =
    classes.foreach {
      case (registryClass, id, serializer) =>
        serializer match {
          case DefaultSerializer => k.register(registryClass, id)
          case CompatibleSerializer =>
            val fs = new com.esotericsoftware.kryo.serializers.CompatibleFieldSerializer(k, registryClass)
            fs.setIgnoreSyntheticFields(false) // scala generates a lot of these attributes
            k.register(registryClass, fs, id)
        }
    }
}

object ExplicitKryoRegistrar {
  implicit val monoid = new Monoid[ExplicitKryoRegistrar] {

    // TODO: Consider throwing if there is intersection between registrars
    def combine(x: ExplicitKryoRegistrar, y: ExplicitKryoRegistrar): ExplicitKryoRegistrar =
      ExplicitKryoRegistrar(x.classes ++ y.classes)

    def empty: ExplicitKryoRegistrar = ExplicitKryoRegistrar(Set.empty[(Class[_], Int, KryoSerializer)])
  }

  sealed trait KryoSerializer extends EnumEntry

  object KryoSerializer extends Enum[KryoSerializer] {

    case object DefaultSerializer extends KryoSerializer
    case object CompatibleSerializer extends KryoSerializer

    val values = findValues

  }
}
