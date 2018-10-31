package org.constellation

import akka.actor.{Actor, ActorSystem}
import akka.util.Timeout
import better.files._
import com.typesafe.scalalogging.Logger
import org.constellation.datastore.leveldb.LevelDB
import org.constellation.datastore.leveldb.LevelDB._
import org.constellation.serializer.KryoSerializer

import scala.concurrent.ExecutionContext
import scala.util.Try

class LevelDBActor(dao: DAO)(implicit timeoutI: Timeout, system: ActorSystem)
    extends Actor {

  implicit val executionContext: ExecutionContext =
    system.dispatchers.lookup("db-io-dispatcher")

  val logger = Logger("LevelDB")

  def tmpDirId = file"tmp/${dao.id.medium}/db"
  def mkDB: LevelDB = LevelDB(tmpDirId)

  override def receive: Receive = active(mkDB)

  def active(db: LevelDB): Receive = {

    case RestartDB =>
      Try { db.destroy() }
        .foreach(e => logger.warn("Exception while destroying LevelDB db", e))
      context become active(mkDB)

    case DBGet(key) =>
      dao.numDBGets += 1
      val res = Try { db.getBytes(key).map { KryoSerializer.deserialize } }.toOption.flatten
      sender() ! res

    case DBPut(key, obj) =>
      dao.numDBPuts += 1
      val bytes = KryoSerializer.serializeAnyRef(obj)
      sender() ! db.putBytes(key, bytes)

    case DBUpdate(key, func, empty) =>
      dao.numDBUpdates += 1
      val res = Try { db.getBytes(key).map { KryoSerializer.deserialize } }.toOption.flatten
      val option = res.map(func)
      val obj = option.getOrElse(empty)
      val bytes = KryoSerializer.serializeAnyRef(obj)
      db.putBytes(key, bytes)
      sender() ! obj

    case DBDelete(key) =>
      if (db.contains(key)) {
        dao.numDBDeletes += 1
        sender() ! db.delete(key).isSuccess
      } else sender() ! true
  }

}



// Only need to implement kryo get / put


