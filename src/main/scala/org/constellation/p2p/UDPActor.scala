package org.constellation.p2p

import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorRef}
import akka.io.{IO, Udp}
import akka.serialization.SerializationExtension
import akka.util.{ByteString, Timeout}
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Input
import org.constellation.p2p.PeerToPeer.Id

// Consider adding ID to all UDP messages? Possibly easier.
case class UDPMessage(data: Any, remote: InetSocketAddress)
case class GetUDPSocketRef()
case class UDPSend(data: ByteString, remote: InetSocketAddress)
case class UDPSendJSON(data: Any, remote: InetSocketAddress)
case class UDPSendToIDByte(data: ByteString, remote: Id)
case class UDPSendToID[T](data: T, remote: Id)
case class UDPSendTyped[T](data: T, remote: InetSocketAddress)
case class RegisterNextActor(nextActor: ActorRef)
case class GetSelfAddress()
case class Ban(address: InetSocketAddress)
case class GetBanList()

case object GetPacketGroups

// This is using java serialization which is NOT secure. We need to update to use another serializer
// Examples below:
// https://github.com/calvinlfer/Akka-Persistence-example-with-Protocol-Buffers-serialization/blob/master/src/main/scala/com/experiments/calculator/serialization/CalculatorEventProtoBufSerializer.scala
// https://github.com/dnvriend/akka-serialization-test/tree/master/src/main/scala/com/github/dnvriend/serializer
// Serialization below is just a temporary hack to avoid having to make more changes for now.


// Need to catch alert messages to detect socket closure.
class UDPActor(
                @volatile var nextActor: Option[ActorRef] = None,
                port: Int = 16180,
                bindInterface: String = "0.0.0.0"
              ) extends Actor {

  import context.system

  private val address = new InetSocketAddress(bindInterface, port)
  IO(Udp) ! Udp.Bind(self, address, List(
      Udp.SO.ReceiveBufferSize(1024 * 1024 * 20),
      Udp.SO.SendBufferSize(1024 * 1024 * 20),
    Udp.SO.ReuseAddress.apply(true))
  )

  @volatile var udpSocket: ActorRef = _
  @volatile var bannedIPs: Seq[InetSocketAddress] = Seq.empty[InetSocketAddress]
  implicit val timeout: Timeout = Timeout(10, TimeUnit.SECONDS)
  private val packetGroups = scala.collection.mutable.HashMap[Long, Seq[SerializedUDPMessage]]()

  import constellation._


  def receive: PartialFunction[Any, Unit] = {
    case Udp.Bound(_) =>
      val ref = sender()
      udpSocket = ref
      context.become(ready(ref))
    case RegisterNextActor(next) =>
      // println(s"Registered next actor for udp on port $port")
      nextActor = Some(next)

  }

  def ready(socket: ActorRef): Receive = {

    case GetPacketGroups => sender() ! packetGroups

    case Udp.Received(data, remote) =>
      // println(s"Received UDP message from $remote -- sending to $nextActor")
      if (!bannedIPs.contains(remote)) {
        val serialization = SerializationExtension(context.system)

        val str = data.utf8String
        val serMsg = str.x[SerializedUDPMessage]
        // val bb = data.toArray
        //val serMsg = bb.kryoRead.asInstanceOf[SerializedUDPMessage]
        this.synchronized {
          if (serMsg.packetGroup.nonEmpty) {
            val pg = serMsg.packetGroup.get
            packetGroups.get(pg) match {
              case Some(messages) =>
                if (messages.length + 1 == serMsg.packetGroupSize.get) {
                  // Done
                //  println("UDP Receiver")
                  val dat = {messages ++ Seq(serMsg)}.sortBy(_.packetGroupId.get).flatMap{_.data}.toArray
                  val deser = serialization.deserialize(dat, serMsg.serializer, Some(classOf[Any]))
               //   val kryoInput = new Input(dat)
               //   val deser = Some(kryo.readClassAndObject(kryoInput))
                 // println(s"Received BULK UDP message from $remote -- $deser -- sending to $nextActor")
                  deser.foreach { d =>
                    nextActor.foreach { n => n ! UDPMessage(d, remote) }
                  }
                //  packetGroups.remove(pg)
                } else {
                  packetGroups(pg) = messages ++ Seq(serMsg)
                }
              case None =>
                packetGroups(pg) = Seq(serMsg)
            }
          } else {

            val deser = serialization.deserialize(serMsg.data, serMsg.serializer, Some(classOf[Any]))
         //   val kryoInput = new Input(serMsg.data)
         //   val deser = Some(kryo.readObject(kryoInput, classOf[Any]))
            //    println(s"Received UDP message from $remote -- $deser -- sending to $nextActor")
            deser.foreach { d =>
              nextActor.foreach { n => n ! UDPMessage(d, remote) }
            }
          }
        }
      } else {
        println(s"BANNED MESSAGE DETECTED FROM $remote")
      }

    case UDPSend(data, remote) =>
      //   println(s"Attempting UDPSend from port: $port to $remote of data length: ${data.length}")
      socket ! Udp.Send(data, remote)

    case UDPSendTyped(dataA, remote) =>
      import constellation.UDPSerExt
      val ser = dataA.asInstanceOf[AnyRef].udpSerializeGrouped()
      ser.foreach{ s => self ! UDPSend(ByteString(s.json), remote)}
     // ser.foreach{ s => self ! UDPSend(ByteString(s.kryoWrite), remote)}

    case UDPSendJSON(data, remote) =>
      self ! UDPSend(ByteString(data.json), remote)
      //self ! UDPSend(ByteString(data.kryoWrite), remote)

    case u @ UDPSendToID(_, _) => nextActor.foreach{ na => na ! u}

    case RegisterNextActor(next) => nextActor = Some(next)

    case Ban(remote) => bannedIPs = {bannedIPs ++ Seq(remote)}.distinct
    case GetBanList => sender() ! bannedIPs

    case GetUDPSocketRef => sender() ! udpSocket
    case GetSelfAddress => sender() ! address

    case Udp.Unbind => socket ! Udp.Unbind
    case Udp.Unbound => context.stop(self)

    case x =>
      println(s"UDPActor unrecognized message: $x")

  }
}

// Change packetGroup to UUID
case class SerializedUDPMessage(data: Array[Byte], serializer: Int, packetGroup: Option[Long] = None,
                                packetGroupSize: Option[Long] = None, packetGroupId: Option[Int] = None
                               )
