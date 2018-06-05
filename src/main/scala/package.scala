
import java.io.ByteArrayOutputStream
import java.net.InetSocketAddress
import java.security.{KeyPair, PrivateKey, PublicKey}
import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, PredefinedFromEntityUnmarshallers, Unmarshal}
import akka.serialization.SerializationExtension
import akka.stream.ActorMaterializer
import akka.util.{ByteString, Timeout}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.esotericsoftware.kryo.io.{Input, Output}
import com.google.common.hash.Hashing
import com.twitter.chill.{IKryoRegistrar, KryoBase, ScalaKryoInstantiator}
import org.constellation.p2p._
import org.constellation.primitives.Schema.{Address, Id}
import org.constellation.util.{POWExt, POWSignHelp, ProductHash}
import org.constellation.crypto.KeyUtils.{KeyPairSerializer, PrivateKeySerializer, PublicKeySerializer}
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


  /*

    private val keyPairKryoSer = new Serializer[KeyPair] {
      override def write(kryo: Kryo, output: Output, `object`: KeyPair): Unit = {
        `object`.getPrivate.getEncoded ++ `object`.getPublic.getEncoded
      }

      override def read(kryo: Kryo, input: Input, `type`: Class[KeyPair]): KeyPair = {
        val buf = input.getBuffer
        val priv = bytesToPrivateKey(buf.slice(0, 144))
        val pub = bytesToPublicKey(buf.slice(144, 144+88))
        new KeyPair(pub, priv)
      }
    }
  */

  case class EncodedPubKey(pubKeyEncoded: Array[Byte])

  class PubKeyKryoSerializer extends Serializer[PublicKey] {
    override def write(kryoI: Kryo, output: Output, `object`: PublicKey): Unit = {
      val enc = `object`.getEncoded
      //   println("PubKeyEnc " + enc.toSeq)
      // kryoI.writeClass(output, classOf[PublicKey])
      println("public key writer")
      kryoI.writeClassAndObject(output, EncodedPubKey(enc))
      //output.write(enc)
    }

    override def read(kryoI: Kryo, input: Input, `type`: Class[PublicKey]): PublicKey = {
      val encP = EncodedPubKey(null)
      kryoI.reference(encP)
      //val buf = input.getBuffer
      //println("PubKey reader: " + buf.toSeq.zipWithIndex)
      // kryo.getReferenceResolver()
      //bytesToPublicKey(buf.slice(64,88+64))
      println("public key reader")
      val enc = kryoI.readClassAndObject(input).asInstanceOf[EncodedPubKey]
      bytesToPublicKey(enc.pubKeyEncoded)
    }
  }
  /*

    class PrivKeyKryoSerializer extends Serializer[PrivateKey] {
      override def write(kryoI: Kryo, output: Output, `object`: PrivateKey): Unit = {
      //  `object`.json.getBytes()
        val enc = `object`.getEncoded
     //   println("Kryo write size " + enc.size)
    //    println("Kryo write buffer " + enc.toSeq)
        kryoI.writeClass(output, classOf[PrivateKey])
        output.write(enc)
      }

      override def read(kryoI: Kryo, input: Input, `type`: Class[PrivateKey]): PrivateKey = {
    //    val clazz = kryoI.readClass(input)
      //  kryoI.class
    //    println("Read class " + clazz)
        val buf = input.getBuffer
        bytesToPrivateKey(buf.slice(65,144+65))
        //    println("Kryo read size " + buf.size)
        //    println("Kryo read buffer " + buf.toSeq)
       // (new String(input.getBuffer)).x[PrivateKey]
      }
    }
  */

  val instantiator = new ScalaKryoInstantiator()
  instantiator.setRegistrationRequired(false)

/*
  val kryoRegister = new IKryoRegistrar {
    override def apply(k: Kryo): Unit = {
      // kryo.register(classOf[KeyPair], keyPairKryoSer)
      k.register(classOf[PublicKey], new PubKeyKryoSerializer())
      k.addDefaultSerializer(classOf[PublicKey], new PubKeyKryoSerializer())
      //  k.addDefaultSerializer(classOf[PrivateKey], new PrivKeyKryoSerializer())
      //      k.register(classOf[PrivateKey], new PrivKeyKryoSerializer())
    }
  }
  instantiator.withRegistrar(kryoRegister)
*/

  val kryo: KryoBase = instantiator.newKryo()
  // kryo.register(classOf[PrivateKey], new PrivKeyKryoSerializer())
  // kryo.addDefaultSerializer(classOf[PrivateKey], new PrivKeyKryoSerializer())
  kryo.register(classOf[PublicKey], new PubKeyKryoSerializer())
  kryo.addDefaultSerializer(classOf[PublicKey], new PubKeyKryoSerializer())


  implicit class KryoExt[T](a: T) {
    def kryoWrite: Array[Byte] = {
      val out = new Output(32, 1024*1024*100)
      kryo.writeClassAndObject(out, a)
      out.getBuffer
    }
  }

  implicit class KryoExtByte(a: Array[Byte]) {
    def kryoRead: AnyRef = {
      val kryoInput = new Input(a)
      val deser = kryo.readClassAndObject(kryoInput)
      deser
    }
  }


  implicit class UDPSerExt[T <: AnyRef](data: T)(implicit system: ActorSystem) {
    def udpSerialize(
                      packetGroup: Option[Long] = None,
                      packetGroupSize: Option[Long] = None, packetGroupId: Option[Int] = None
                    ): SerializedUDPMessage = {
      val serialization = SerializationExtension(system)
      val serializer = serialization.findSerializerFor(data)
      val bytes = serializer.toBinary(data)
      //val bos = new ByteArrayOutputStream()
   //   val out = new Output(500, 1024*1024*100)
  //    kryo.writeClassAndObject(out, data)
  //    val kryoBytes = out.getBuffer
      val serMsg = SerializedUDPMessage(bytes, serializer.identifier)
      serMsg
    }

    def udpSerializeGrouped(groupSize: Int = 500): Seq[SerializedUDPMessage] = {
      val serialization = SerializationExtension(system)
      val serializer = serialization.findSerializerFor(data)
      val bytes = serializer.toBinary(data)
   //   val out = new Output(groupSize, 1024*1024*100)
   //   kryo.writeClassAndObject(out, data)
   //   val kryoBytes = out.getBuffer

      if (bytes.length < groupSize) {
        Seq(SerializedUDPMessage(bytes, serializer.identifier))
      } else {
        val idx = bytes.grouped(groupSize).zipWithIndex.toSeq
        val pg = Random.nextLong()
        idx.map { case (b, i) =>
          SerializedUDPMessage(b, serializer.identifier,
            packetGroup = Some(pg), packetGroupSize = Some(idx.length), packetGroupId = Some(i))
        }
      }
    }

  }

  implicit class UDPActorExt(udpActor: ActorRef) {

    def udpSendToId[T <: AnyRef](data: T, remote: Id)(implicit system: ActorSystem): Unit = {
      udpActor ! UDPSendToID(data, remote)
    }

    def udpSend[T <: AnyRef](data: T, remote: InetSocketAddress)(implicit system: ActorSystem): Unit = {
      data.udpSerializeGrouped().foreach { d =>
        udpActor ! UDPSend(ByteString(d.json), remote)
   //     udpActor ! UDPSend(ByteString(d.kryoWrite), remote)
      }
    }

    def udpSign[T <: ProductHash](data: T, remote: InetSocketAddress, difficulty: Int = 0)
                                 (implicit system: ActorSystem, keyPair: KeyPair): Unit = {
      udpActor ! UDPSendTyped(data.signed(), remote)
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


}