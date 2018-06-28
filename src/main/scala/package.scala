
import java.net.InetSocketAddress
import java.security.{KeyPair, PublicKey}
import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.ActorMaterializer
import akka.util.{ByteString, Timeout}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.minlog.Log
import com.google.common.hash.Hashing
import com.twitter.chill.{KryoPool, ScalaKryoInstantiator}
import org.constellation.consensus.Consensus.RemoteMessage
import org.constellation.p2p._
import org.constellation.primitives.Schema.{Address, Bundle, Id}
import org.constellation.util.{POWExt, POWSignHelp, ProductHash}
import org.constellation.crypto.KeyUtilsExt
import org.json4s.JsonAST.{JInt, JString}
import org.json4s.native._
import org.json4s.{CustomSerializer, DefaultFormats, Extraction, Formats, JObject, JValue, native}

import scala.concurrent.{Await, Future}
import scala.reflect.ClassTag
import scala.util.{Random, Try}

/**
  * Project wide convenience functions.
  */
package object constellation extends KeyUtilsExt with POWExt
  with POWSignHelp {

  val minimumTime : Long = 1518898908367L

  implicit class EasyFutureBlock[T](f: Future[T]) {
    def get(t: Int = 30): T = {
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
    def prettyJson: String = Serialization.writePretty(Extraction.decompose(jsonSerializable))
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

  case class EncodedPubKey(pubKeyEncoded: Array[Byte])

  class PubKeyKryoSerializer extends Serializer[PublicKey] {
    override def write(kryoI: Kryo, output: Output, `object`: PublicKey): Unit = {
      val enc = `object`.getEncoded
      kryoI.writeClassAndObject(output, EncodedPubKey(enc))
    }

    override def read(kryoI: Kryo, input: Input, `type`: Class[PublicKey]): PublicKey = {
      val encP = EncodedPubKey(null)
      kryoI.reference(encP)
      val enc = kryoI.readClassAndObject(input).asInstanceOf[EncodedPubKey]
      bytesToPublicKey(enc.pubKeyEncoded)
    }
  }

  class SerializedUDPMessageSerializer extends Serializer[SerializedUDPMessage] {
    override def write(kryoI: Kryo, output: Output, `object`: SerializedUDPMessage): Unit = {
      kryoI.writeClassAndObject(output, `object`)
    }

    override def read(kryoI: Kryo, input: Input, `type`: Class[SerializedUDPMessage]): SerializedUDPMessage = {
      kryoI.readClassAndObject(input).asInstanceOf[SerializedUDPMessage]
    }
  }

  def guessThreads: Int = {
    val cores = Runtime.getRuntime.availableProcessors
    val GUESS_THREADS_PER_CORE = 4
    GUESS_THREADS_PER_CORE * cores
  }

  val kryoPool: KryoPool = KryoPool.withBuffer(guessThreads,
    new ScalaKryoInstantiator(), 32, 1024*1024*100)

  Log.TRACE()

  /*
  kryoInstance.register(classOf[SerializedUDPMessage], new SerializedUDPMessageSerializer())

  kryoInstance.addDefaultSerializer(classOf[SerializedUDPMessage], new SerializedUDPMessageSerializer())

  kryoInstance.register(classOf[PublicKey], new PubKeyKryoSerializer())

  kryoInstance.addDefaultSerializer(classOf[PublicKey], new PubKeyKryoSerializer())
  */

  def udpSerializeGrouped[T <: RemoteMessage](data: T, groupSize: Int = 500): Seq[SerializedUDPMessage] = {
    val bytes = kryoPool.toBytesWithClass(data)

    val idx = bytes.grouped(groupSize).zipWithIndex.toSeq

    val pg = Random.nextLong()

    idx.map { case (b, i) =>
      SerializedUDPMessage(ByteString(b), data.getClass.getName,
        packetGroup = pg, packetGroupSize = idx.length, packetGroupId = i)
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

  implicit def pubKeyToAddress(key: PublicKey): Address =  Address(publicKeyToAddressString(key))
  implicit def pubKeysToAddress(key: Seq[PublicKey]): Address =  Address(publicKeysToAddressString(key))

  implicit class KeyPairFix(kp: KeyPair) {

    // This is because the equals method on keypair relies on an object hash code instead of an actual check on the data.
    // The equals method for public and private keys is totally fine though.

    def dataEqual(other: KeyPair): Boolean = {
      kp.getPrivate == other.getPrivate &&
        kp.getPublic == other.getPublic
    }

    def address: Address = pubKeyToAddress(kp.getPublic)

  }

  implicit class ActorQuery(a: ActorRef) {
    import akka.pattern.ask
    implicit val timeout: Timeout = Timeout(5, TimeUnit.SECONDS)
    def query[T: ClassTag](m: Any): T = (a ? m).mapTo[T].get()
  }

/*
  implicit def orderingByBundle[A <: Bundle]: Ordering[A] =
    Ordering.by(e =>
      (e.extractTX.size, e.extractIds.size, e.totalNumEvents, e.maxStackDepth, e.hash)
    )

*/

}