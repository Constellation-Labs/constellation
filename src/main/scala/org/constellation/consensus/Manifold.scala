package org.constellation.consensus

import akka.actor.FSM

object Manifold {
  /**
    * This is the flow stream io monad, validator => p2p
    */
  def metaMorphism[S, D](manifold: Manifold[S, D]) = {
    //TODO chain whatever queries we need in order to validate a tx and route it appropriately
  }
    /*
  chain sync logic in consensus actor should go here
   */
  def syncChain() = {}

  def performConsensus() = {}
}
/**
  * Created by Wyatt on 5/18/18.
  */
class Manifold[S, D] extends FSM[S, D] {
  // extends chainStateActor with consensusActor {
  Manifold.syncChain()
}
