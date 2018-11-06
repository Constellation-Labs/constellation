package org.constellation.util

import java.util.concurrent.TimeUnit

import akka.http.scaladsl.coding.Gzip
import akka.util.ByteString
import com.typesafe.config.ConfigFactory
import org.constellation.DAO
import org.constellation.consensus.{Snapshot, SnapshotInfo, StoredSnapshot}
import org.constellation.primitives.Schema.{Id, MetricsResult}
import org.json4s.native.Serialization
import org.json4s.{Formats, native}
import scalaj.http.{Http, HttpRequest, HttpResponse}

import scala.concurrent.{ExecutionContext, Future}

object APIClient {
  def apply(host: String = "127.0.0.1", port: Int, udpPort: Int = 16180)
           (
             implicit executionContext: ExecutionContext, dao: DAO = null
             //implicit system: ActorSystem, materialize: ActorMaterializer
  ): APIClient = {
    new APIClient(host, port)
  }
}

class APIClient(host: String = "127.0.0.1", port: Int, val peerHTTPPort: Int = 9001, val internalPeerHost: String = "")(
 // implicit val system: ActorSystem,
 // implicit val materialize: ActorMaterializer
 implicit val executionContext: ExecutionContext,
 dao: DAO = null
  ) {

  val daoOpt = Option(dao)

  val hostName: String = host
  var id: Id = _

  val udpPort: Int = 16180
  val apiPort: Int = port

  def udpAddress: String = hostName + ":" + udpPort

  def setExternalIP(): Boolean = postSync("ip", hostName + ":" + udpPort).isSuccess

  private def baseURI: String = {
    val uri = s"http://$hostName:$apiPort"
    uri
  }

  def base(suffix: String) = s"$baseURI/$suffix"

  private val config = ConfigFactory.load()

  private val authEnabled = config.getBoolean("auth.enabled")
  private val authId = config.getString("auth.id")
  private val authPassword = config.getString("auth.password")

  implicit class HttpRequestAuth(req: HttpRequest) {
    def addAuthIfEnabled(): HttpRequest = {
      if (authEnabled) {
        req.auth(authId, authPassword)
      } else req
    }
  }

  def timeoutMS(timeoutSeconds: Int): Int = {
    TimeUnit.SECONDS.toMillis(timeoutSeconds).toInt
  }

  def optHeaders: Map[String, String] = daoOpt.map{
    d =>
      Map("Remote-Address" -> d.externalHostString, "X-Real-IP" -> d.externalHostString)
  }.getOrElse(Map())

  def httpWithAuth(suffix: String, timeoutSeconds: Int = 30): HttpRequest = {
    val timeoutMs = timeoutMS(timeoutSeconds)
    Http(base(suffix)).addAuthIfEnabled().timeout(timeoutMs, timeoutMs).headers(optHeaders)
  }

  implicit val serialization: Serialization.type = native.Serialization

  def metrics: Map[String, String] = getBlocking[MetricsResult]("metrics").metrics

  def post(suffix: String, b: AnyRef, timeoutSeconds: Int = 5)
          (implicit f : Formats = constellation.constellationFormats): Future[HttpResponse[String]] = {
    Future(postSync(suffix, b))
  }

  def put(suffix: String, b: AnyRef, timeoutSeconds: Int = 5)
          (implicit f : Formats = constellation.constellationFormats): Future[HttpResponse[String]] = {
    Future(putSync(suffix, b))
  }

  def postEmpty(suffix: String, timeoutSeconds: Int = 15)(implicit f : Formats = constellation.constellationFormats)
  : HttpResponse[String] = {
    httpWithAuth(suffix).method("POST").asString
  }

  def postSync(suffix: String, b: AnyRef, timeoutSeconds: Int = 5)(
    implicit f : Formats = constellation.constellationFormats
  ): HttpResponse[String] = {
    val ser = Serialization.write(b)
    val gzipped = Gzip.encode(ByteString.fromString(ser)).toArray
    httpWithAuth(suffix)
      .postData(gzipped)
      .headers("content-type" -> "application/json", "Content-Encoding" -> "gzip")
      .asString
  }

  def putSync(suffix: String, b: AnyRef, timeoutSeconds: Int = 5)(
    implicit f : Formats = constellation.constellationFormats
  ): HttpResponse[String] = {
    val ser = Serialization.write(b)
    val gzipped = Gzip.encode(ByteString.fromString(ser)).toArray
    httpWithAuth(suffix)
      .put(gzipped)
      .headers("content-type" -> "application/json", "Content-Encoding" -> "gzip")
      .asString
  }

  def postBlocking[T <: AnyRef](suffix: String, b: AnyRef, timeoutSeconds: Int = 5)(implicit m : Manifest[T], f : Formats = constellation.constellationFormats): T = {
    val res: HttpResponse[String] = postSync(suffix, b)
    Serialization.read[T](res.body)
  }

  def postNonBlocking[T <: AnyRef](suffix: String, b: AnyRef, timeoutSeconds: Int = 5)(implicit m : Manifest[T], f : Formats = constellation.constellationFormats): Future[T] = {
    post(suffix, b).map { res =>
      Serialization.read[T](res.body)
    }
  }

  def read[T <: AnyRef](res: HttpResponse[String])(implicit m: Manifest[T], f: Formats = constellation.constellationFormats): T = {
    Serialization.read[T](res.body)
  }

  def postBlockingEmpty[T <: AnyRef](suffix: String, timeoutSeconds: Int = 5)(implicit m : Manifest[T], f : Formats = constellation.constellationFormats): T = {
    val res = postEmpty(suffix)
    Serialization.read[T](res.body)
  }

  def get(suffix: String, queryParams: Map[String,String] = Map(), timeoutSeconds: Int = 5): Future[HttpResponse[String]] = {
    Future(getSync(suffix, queryParams))
  }

  def getSync(suffix: String, queryParams: Map[String,String] = Map(), timeoutSeconds: Int = 5): HttpResponse[String] = {
    val req = httpWithAuth(suffix).params(queryParams)
    req.asString
  }

  def getBlocking[T <: AnyRef](suffix: String, queryParams: Map[String,String] = Map(), timeoutSeconds: Int = 5)
                              (implicit m : Manifest[T], f : Formats = constellation.constellationFormats): T = {
    Serialization.read[T](getBlockingStr(suffix, queryParams, timeoutSeconds))
  }

  def getNonBlocking[T <: AnyRef](suffix: String, queryParams: Map[String,String] = Map(), timeoutSeconds: Int = 5)
                              (implicit m : Manifest[T], f : Formats = constellation.constellationFormats): Future[T] = {
    Future(getBlocking[T](suffix, queryParams, timeoutSeconds))
  }

  def readHttpResponseEntity[T <: AnyRef](response: String)
                              (implicit m : Manifest[T], f : Formats = constellation.constellationFormats): T = {
    Serialization.read[T](response)
  }

  def getBlockingStr(suffix: String, queryParams: Map[String,String] = Map(), timeoutSeconds: Int = 5): String = {
    val resp: HttpResponse[String] = httpWithAuth(suffix, timeoutSeconds).params(queryParams).asString

    resp.body
  }

  def getSnapshotInfo(): SnapshotInfo = getBlocking[SnapshotInfo]("info")


  def getSnapshots(): Seq[Snapshot] = {

    val snapshotInfo = getSnapshotInfo()

    val startingSnapshot = snapshotInfo.snapshot

    def getSnapshots(hash: String, snapshots: Seq[Snapshot] = Seq()): Seq[Snapshot] = {
      val sn = getBlocking[Option[Snapshot]]("snapshot/" + hash)
      sn match {
        case Some(snapshot) =>
          if (snapshot.lastSnapshot == "" || snapshot.lastSnapshot == Snapshot.snapshotZeroHash) {
            snapshots :+ snapshot
          } else {
            getSnapshots(snapshot.lastSnapshot, snapshots :+ snapshot)
          }
        case None =>
          println("MISSING SNAPSHOT")
          snapshots
      }
    }

    val snapshots = getSnapshots(startingSnapshot.lastSnapshot, Seq(startingSnapshot))
    snapshots
  }


  def simpleDownload(): Seq[StoredSnapshot] = {

    val hashes = getBlocking[Seq[String]]("snapshotHashes")

    hashes.map{ h =>
      getBlocking[Option[StoredSnapshot]]("storedSnapshot/" + h).get
    }

  }

}
