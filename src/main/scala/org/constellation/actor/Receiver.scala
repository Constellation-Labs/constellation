package org.constellation.actor

import akka.actor.Actor
import akka.actor.Actor.emptyBehavior

class Receiver extends Actor {
  var receivers: Receive = emptyBehavior
  def receiver(next: Receive) { receivers = receivers orElse next }
  final def receive: Receive = receivers
}
