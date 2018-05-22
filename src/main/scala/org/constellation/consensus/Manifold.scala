package org.constellation.consensus

import cats.Functor

object Manifold extends Recursive {
  def algebra[B](f: Functor[B]): B = {
    //TODO fires off and retries until successful, result is enqueued again with new call tree
  }

  def coAlgebra[B](g: B): Functor[B] = {
    //TODO collapses successive call trees
  }

  /**
    * This is the stream io monad, tear down chained futures and build up result. would be awesome if we could make monadic like https://patternsinfp.wordpress.com/2017/10/04/metamorphisms/
    */
  def meta[F[_] : Functor, A, B](g: A => F[A])(f: F[B] => B): F[A] => F[B] = g(fix.unfix(cata(f)))

  def metaMorphism[S, D](bundle: Bundle) = {
    //TODO chain whatever queries we need in order to validate a tx and route it appropriately
    //TODO Bundles should have unfold operations defined as monadic semigroup operators. Through a reduce we dynamically
    // turn into a graph of linked bundles, self organizing into full bundles. When a Bundle is linked it returns a onComplete to
    // the monad in the ring buffer. Only the 'tip' remains till the end and then it and its leaves are recursively
    // hashed into a signed bundle. Allows for concurrency.
  }
}

/**
  * Created by Wyatt on 5/18/18.
  */
class Manifold[S, D] extends ChainStateManager[S, D] {
  // extends chainStateActor //TODO chainStateActor is the real main nugget of logic, it runs a subprocess for forming bundles of high dimension
  // with consensusActor {
  syncChain()
}
