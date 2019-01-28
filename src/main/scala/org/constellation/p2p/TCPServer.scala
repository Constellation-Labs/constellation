package org.constellation.p2p

import java.net.InetSocketAddress
import akka.actor.{Actor, Props}
import akka.io.{IO, Tcp}

/** Minimal handler. */
class SimplisticHandler extends Actor {

  import Tcp._

  /** Receive method of actor. */
  def receive: PartialFunction[Any, Unit] = {
    case Received(data) => sender() ! Write(data)
    case PeerClosed => context stop self
  }
}

/** Transmission Control Protocol server class */
class TCPServer(hostInterface: String, port: Int) extends Actor {

  import akka.io.Tcp._
  import context.system

  IO(Tcp) ! Bind(self, new InetSocketAddress(hostInterface, port))

  /** Receive method of actor. */
  def receive: PartialFunction[Any, Unit] = {
    case b@Bound(localAddress) =>
      context.parent ! b

    case CommandFailed(_: Bind) => context stop self

    case c@Connected(remote, local) =>
      val handler = context.actorOf(Props[SimplisticHandler])
      val connection = sender()
      connection ! Register(handler)
  }

} // end TCPServer
