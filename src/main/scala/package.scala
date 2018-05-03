
import java.net.InetSocketAddress
import java.security.KeyPair

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, PredefinedFromEntityUnmarshallers, Unmarshal}
import akka.serialization.SerializationExtension
import akka.stream.ActorMaterializer
import akka.util.ByteString
import com.google.common.hash.Hashing
import org.constellation.p2p.PeerToPeer.{Id, PeerRef}
import org.constellation.p2p.{SerializedUDPMessage, UDPSend, UDPSendToIDByte}
import org.constellation.util.{POWExt, POWSignHelp, ProductHash}
import org.constellation.wallet.KeyUtils.{KeyPairSerializer, PrivateKeySerializer, PublicKeySerializer}
import org.constellation.wallet.KeyUtilsExt
import org.json4s.JsonAST.{JInt, JString}
import org.json4s.native._
import org.json4s.{CustomSerializer, DefaultFormats, Extraction, Formats, JObject, JValue, native}

import scala.concurrent.{Await, Future}
import scala.util.Try

/**
  * Project wide convenience functions.
  */
package object constellation extends KeyUtilsExt with POWExt
  with POWSignHelp {

  val minimumTime : Long = 1518898908367L

  implicit class EasyFutureBlock[T](f: Future[T]) {
    def get(t: Int = 5): T = {
      import scala.concurrent.duration._
      Await.result(f, t.seconds)
    }
  }

  implicit def addressToSocket(peerAddress: String): InetSocketAddress =
    peerAddress.split(":") match { case Array(ip, port) => new InetSocketAddress(ip, port.toInt)}

  implicit def socketToAddress(peerAddress: InetSocketAddress): String =
    peerAddress.getHostString + ":" + peerAddress.getPort

  class InetSocketAddressSerializer extends CustomSerializer[InetSocketAddress](format => ( {
    case jstr: JObject =>
      val host = (jstr \ "host").extract[String]
      val port = (jstr \ "port").extract[Int]
      new InetSocketAddress(host, port)
  }, {
    case key: InetSocketAddress =>
      JObject("host" -> JString(key.getHostString), "port" -> JInt(key.getPort))
  }
  ))

  implicit val constellationFormats: Formats = DefaultFormats +
    new PublicKeySerializer + new PrivateKeySerializer + new KeyPairSerializer + new InetSocketAddressSerializer

  def caseClassToJson(message: Any): String = {
    compactRender(Extraction.decompose(message))
  }

  def parse4s(msg: String) : JValue = parseJsonOpt(msg).get

  def compactRender(msg: JValue): String = Serialization.write(msg)

  implicit class SerExt(jsonSerializable: Any) {
    def json: String = caseClassToJson(jsonSerializable)
    def tryJson: Try[String] = Try{caseClassToJson(jsonSerializable)}
    def j: String = json
    def jsonSave(f: String): Unit = scala.tools.nsc.io.File(f).writeAll(json)
  }

  implicit class ParseExt(input: String) {
    def jValue: JValue = parse4s(input)
    def jv: JValue = jValue
    def x[T](implicit m: Manifest[T]): T = jv.extract[T](constellationFormats, m)
  }

  implicit class SHA256Ext(s: String) {
    def sha256: String = Hashing.sha256().hashBytes(s.getBytes()).toString
  }

  import java.nio.ByteBuffer
  import java.nio.ByteOrder

  def intToByte(myInteger: Int): Array[Byte] =
    ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(myInteger).array

  def byteToInt(byteBarray: Array[Byte]): Int =
    ByteBuffer.wrap(byteBarray).order(ByteOrder.BIG_ENDIAN).getInt

  implicit class UDPSerExt[T <: AnyRef](data: T)(implicit system: ActorSystem) {
    def udpSerialize(
                      packetGroup: Option[Long] = None,
                      packetGroupSize: Option[Long] = None, packetGroupId: Option[Int] = None
                    ): SerializedUDPMessage = {
      val serialization = SerializationExtension(system)
      val serializer = serialization.findSerializerFor(data)
      val bytes = serializer.toBinary(data)
      val serMsg = SerializedUDPMessage(bytes, serializer.identifier)
      serMsg
    }
  }

  implicit class UDPActorExt(udpActor: ActorRef) {
    def udpSendToId[T <: AnyRef](data: T, remote: Id)(implicit system: ActorSystem): Unit = {
      val serialization = SerializationExtension(system)
      val serializer = serialization.findSerializerFor(data)
      val bytes = serializer.toBinary(data)
      val serMsg = SerializedUDPMessage(bytes, serializer.identifier)
      udpActor ! UDPSendToIDByte(ByteString(serMsg.json), remote)
    }
    def udpSend[T <: AnyRef](data: T, remote: InetSocketAddress)(implicit system: ActorSystem): Unit = {
      val serialization = SerializationExtension(system)
      val serializer = serialization.findSerializerFor(data)
      val bytes = serializer.toBinary(data)
      val serMsg = SerializedUDPMessage(bytes, serializer.identifier)
      udpActor ! UDPSend(ByteString(serMsg.json), remote)
    }

    def udpSign[T <: ProductHash](data: T, remote: InetSocketAddress, difficulty: Int = 0)
                                 (implicit system: ActorSystem, keyPair: KeyPair): Unit = {
      udpActor ! UDPSend(ByteString(data.udpSerialize().json), remote)
    }
  }

  implicit class HTTPHelp(httpResponse: HttpResponse)
                         (implicit val materialize: ActorMaterializer) {
    def unmarshal: Future[String] = Unmarshal(httpResponse.entity).to[String]
  }

  def pprintInet(inetSocketAddress: InetSocketAddress): String = {
    s"address: ${inetSocketAddress.getAddress}, hostname: ${inetSocketAddress.getHostName}, " +
      s"hostString: ${inetSocketAddress.getHostString}, port: ${inetSocketAddress.getPort}"
  }


}