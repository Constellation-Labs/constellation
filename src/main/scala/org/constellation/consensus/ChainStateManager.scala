package org.constellation.consensus

import akka.actor.FSM

/**
  * Created by Wyatt on 5/22/18.
  */
class ChainStateManager[S,D] extends FSM[S,D] {
  /*
  these methods should live in chain state manager, consensusActor talks with chainStateActor
   */
  def syncChain() = {}

  def performConsensus() = {}
}
