
import akka.actor.ActorRef
import com.google.common.hash.Hashing
import org.constellation.p2p.PeerToPeer.PeerRef
import org.constellation.wallet.KeyUtils.{KeyPairSerializer, PrivateKeySerializer, PublicKeySerializer}
import org.json4s.JsonAST.JString
import org.json4s.native._
import org.json4s.{CustomSerializer, DefaultFormats, Extraction, Formats, JObject, JValue}

import scala.concurrent.{Await, Future}
import scala.util.Try

/**
  * Project wide convenience functions.
  */
package object constellation {

  implicit class EasyFutureBlock[T](f: Future[T]) {
    def get(t: Int = 5): T = {
      import scala.concurrent.duration._
      Await.result(f, t.seconds)
    }
  }

  implicit val constellationFormats: Formats = DefaultFormats +
    new PublicKeySerializer + new PrivateKeySerializer + new KeyPairSerializer

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

}