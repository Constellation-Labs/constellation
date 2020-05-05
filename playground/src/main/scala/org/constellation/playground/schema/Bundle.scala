package org.constellation.playground.schema

case class Bundle(fibers: Seq[Fiber])

object Bundle {
  /*
  implicit val bundleFunctorImpl: Functor[Bundle] = new Functor[Bundle] {
    override def map[A, B](fa: Bundle[A])(f: A => B): Bundle[B] = ???
  }
 */
}
