package org.constellation.state

import akka.actor.FSM

/**
  * Created by Wyatt on 5/15/18.
  */
class AtMostOnceFSM[S, D] extends FSM[S, D] {

  /**
    * TODO: do local lookup, this is where we will handle rate limiting
    * @param message S message to be processed
    * @return to drop or not
    */
  protected[this] def hasBeenProcessed(message: S): Boolean = {
    false
  }

  def AtMostOnce(stateFunction: StateFunction): StateFunction = new StateFunction() {
    override def isDefinedAt(msg: Event): Boolean = stateFunction.isDefinedAt(msg)

    override def apply(msg: Event): State = msg match {
      case msg@Event(s: S, _) =>
        if (!hasBeenProcessed(s)) {
          stateFunction(msg)
        } else {
          stay()
        }
      case msg@_ => stateFunction(msg)
    }
  }
}
