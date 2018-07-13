
import java.net.InetSocketAddress
import java.security.{KeyPair, PrivateKey, PublicKey}
import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.ActorMaterializer
import akka.util.{ByteString, Timeout}
import com.google.common.hash.Hashing
import org.constellation.p2p._
import org.constellation.primitives.Schema._
import org.constellation.util.{HashSignature, POWExt, POWSignHelp, ProductHash}
import org.constellation.crypto.KeyUtilsExt
import org.json4s.JsonAST.{JInt, JString}
import org.json4s.native._
import org.json4s.{CustomSerializer, DefaultFormats, Extraction, Formats, JObject, JValue, ShortTypeHints, native}
import java.nio.ByteBuffer
import java.nio.ByteOrder

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
    def getOpt(t: Int = 30): Option[T] = {
      import scala.concurrent.duration._
      Try{Await.result(f, t.seconds)}.toOption
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
    new PublicKeySerializer + new PrivateKeySerializer + new KeyPairSerializer + new InetSocketAddressSerializer +
  ShortTypeHints(List(classOf[TransactionHash], classOf[ParentBundleHash], classOf[Bundle]))

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


  def intToByte(myInteger: Int): Array[Byte] =
    ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(myInteger).array

  def byteToInt(byteBarray: Array[Byte]): Int =
    ByteBuffer.wrap(byteBarray).order(ByteOrder.BIG_ENDIAN).getInt

/*

  def guessThreads: Int = {
    val cores = Runtime.getRuntime.availableProcessors
    val GUESS_THREADS_PER_CORE = 4
    GUESS_THREADS_PER_CORE * cores
  }

  val kryoPool: KryoPool = KryoPool.withBuffer(guessThreads,
    new ScalaKryoInstantiator().setRegistrationRequired(false), 32, 1024*1024*100)

  implicit class KryoExt[T](a: T) {
    def kryoWrite: Array[Byte] = {
      kryoPool.toBytesWithClass(a)
    }
  }

  implicit class KryoExtByte(a: Array[Byte]) {
    def kryoRead: AnyRef = {
      val deser = kryoPool.fromBytes(a)
      deser
    }
    def kryoExtract[T]: T = {
      kryoPool.fromBytes(a).asInstanceOf[T]
    }
  }
*/

 // Log.TRACE()

  implicit class HTTPHelp(httpResponse: HttpResponse)
                         (implicit val materialize: ActorMaterializer) {
    def unmarshal: Future[String] = Unmarshal(httpResponse.entity).to[String]
  }

  def pprintInet(inetSocketAddress: InetSocketAddress): String = {
    s"address: ${inetSocketAddress.getAddress}, hostname: ${inetSocketAddress.getHostName}, " +
      s"hostString: ${inetSocketAddress.getHostString}, port: ${inetSocketAddress.getPort}"
  }

  implicit def pubKeyToAddress(key: PublicKey): AddressMetaData =  AddressMetaData(publicKeyToAddressString(key))
  implicit def pubKeysToAddress(key: Seq[PublicKey]): AddressMetaData =  AddressMetaData(publicKeysToAddressString(key))

  implicit class KeyPairFix(kp: KeyPair) {

    // This is because the equals method on keypair relies on an object hash code instead of an actual check on the data.
    // The equals method for public and private keys is totally fine though.

    def dataEqual(other: KeyPair): Boolean = {
      kp.getPrivate == other.getPrivate &&
        kp.getPublic == other.getPublic
    }

    def address: AddressMetaData = pubKeyToAddress(kp.getPublic)

  }


  implicit class ActorQuery(a: ActorRef) {
    import akka.pattern.ask
    implicit val timeout: Timeout = Timeout(5, TimeUnit.SECONDS)
    def query[T: ClassTag](m: Any): T = (a ? m).mapTo[T].get()
  }

  def signHashWithKeyB64(hash: String, privateKey: PrivateKey): String = base64(signData(hash.getBytes())(privateKey))

  def hashSign(hash: String, keyPair: KeyPair): HashSignature = {
    HashSignature(hash, signHashWithKeyB64(hash, keyPair.getPrivate), keyPair.getPublic.encoded.b58Encoded)
  }


/*
  implicit def orderingByBundle[A <: Bundle]: Ordering[A] =
    Ordering.by(e =>
      (e.extractTX.size, e.extractIds.size, e.totalNumEvents, e.maxStackDepth, e.hash)
    )

*/

}