package org.constellation.schema.serialization

import cats.Monoid
import com.twitter.chill.{IKryoRegistrar, Kryo}

// TODO: We shoud use Map[Int, Class[_]] but for it was just easier to use Set and keep the order of registration as it was before
case class ExplicitKryoRegistrar(classes: Set[(Class[_], Int)]) extends IKryoRegistrar {

  def apply(k: Kryo): Unit =
    classes.foreach { case (registryClass, id) => k.register(registryClass, id) }
}

object ExplicitKryoRegistrar {
  implicit val monoid = new Monoid[ExplicitKryoRegistrar] {

    // TODO: Consider throwing if there is intersection between registrars
    def combine(x: ExplicitKryoRegistrar, y: ExplicitKryoRegistrar): ExplicitKryoRegistrar =
      ExplicitKryoRegistrar(x.classes ++ y.classes)

    def empty: ExplicitKryoRegistrar = ExplicitKryoRegistrar(Set.empty[(Class[_], Int)])
  }
}
