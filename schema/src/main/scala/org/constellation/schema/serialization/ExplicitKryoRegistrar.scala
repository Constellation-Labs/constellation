package org.constellation.schema.serialization

import cats.Monoid
import com.twitter.chill.{IKryoRegistrar, Kryo}

case class ExplicitKryoRegistrar(classes: Set[(Class[_], Int)]) extends IKryoRegistrar {

  def apply(k: Kryo): Unit =
    classes.foreach { case (registryClass, id) => k.register(registryClass, id) }
}

object ExplicitKryoRegistrar {
  implicit val monoid = new Monoid[ExplicitKryoRegistrar] {

    def combine(x: ExplicitKryoRegistrar, y: ExplicitKryoRegistrar): ExplicitKryoRegistrar =
      ExplicitKryoRegistrar(x.classes ++ y.classes)

    def empty: ExplicitKryoRegistrar = ExplicitKryoRegistrar(Set.empty[(Class[_], Int)])
  }
}
