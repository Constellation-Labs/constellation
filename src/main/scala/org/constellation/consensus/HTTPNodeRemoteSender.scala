package org.constellation.consensus
import akka.actor.Props

class HTTPNodeRemoteSender extends NodeRemoteSender {
  override def receive: Receive = ???
}

object HTTPNodeRemoteSender {
  def props: Props = Props(new HTTPNodeRemoteSender)
}