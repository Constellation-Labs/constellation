package org.constellation.util

import java.util.concurrent.TimeUnit

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, PredefinedFromEntityUnmarshallers}
import de.heikoseeberger.akkahttpjson4s.Json4sSupport
import org.constellation.DAO
import org.json4s.native
import org.json4s.native.Serialization
import akka.http.scaladsl.marshalling.Marshaller._
import akka.http.scaladsl.model._
import constellation._
import org.constellation.consensus.{GetMemPool, MemPool, Snapshot, SnapshotInfo}
import akka.pattern.ask
import akka.util.Timeout
import org.constellation.LevelDB.DBGet

trait CommonEndpoints extends Json4sSupport {

  implicit val serialization: Serialization.type

  implicit val stringUnmarshaller: FromEntityUnmarshaller[String]

  implicit val _timeout: Timeout = Timeout(5, TimeUnit.SECONDS)

  val dao: DAO

  val commonEndpoints: Route = get {
    path("health") {
      complete(StatusCodes.OK)
    } ~
      path("id") {
        complete(dao.id)
      } ~
    path("tips") {
      val mp = (dao.edgeProcessor ? GetMemPool).mapTo[MemPool].get()
      complete(mp.thresholdMetCheckpoints.map{_._2.checkpointBlock})
    } ~
      path("info") {
      val mp = (dao.edgeProcessor ? GetMemPool).mapTo[MemPool].get()
      complete(SnapshotInfo(mp.snapshot, mp.acceptedCBSinceSnapshot))
    } ~
    path("snapshot" / Segment) {s =>
      complete((dao.dbActor ? DBGet(s)).mapTo[Option[Snapshot]].get())
    } ~
    path("genesis") {
      complete(dao.genesisObservation)
    }
  }
}
