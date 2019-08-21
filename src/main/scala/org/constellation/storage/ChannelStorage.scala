package org.constellation.storage

import com.typesafe.scalalogging.StrictLogging
import constellation._
import org.constellation.{ConstellationExecutionContext, DAO}
import org.constellation.primitives.ChannelMessageMetadata
import slick.dbio.{DBIOAction, NoStream}
import slick.jdbc.H2Profile.api._
import slick.lifted.ProvenShape

case class MessageResponse(id: Long, channelId: String, hash: Option[String], body: String)

class ChannelStorage(implicit dao: DAO) extends StrictLogging {

  implicit class RunAction[R](a: DBIOAction[R, NoStream, Nothing]) {

    def dbRun(): R =
      db.run(a).get()
  }

  // Definition of the SUPPLIERS table
  private class Message(tag: Tag) extends Table[(Long, String, Option[String], String)](tag, "MESSAGE") {

    def id: Rep[Long] =
      column[Long]("MSG_ID", O.AutoInc, O.PrimaryKey) // This is the primary key column
    def channelId: Rep[String] = column[String]("CHANNEL_ID")

    def hash: Rep[Option[String]] = column[Option[String]]("HASH")

    def body: Rep[String] = column[String]("BODY")
    // Every table needs a * projection with the same type as the table's type parameter
    def * : ProvenShape[(Long, String, Option[String], String)] = (id, channelId, hash, body)
  }

  private val messages = TableQuery[Message]

  private val setup = DBIO.seq(
    messages.schema.createIfNotExists
  )

  private val db = Database.forConfig("h2mem1")
  db.run(setup).get()
  logger.info("Created!")

  def insert(messageMeta: ChannelMessageMetadata): Long = {
    dao.metrics.incrementMetric("storedMessageCount")
    val op =
      (messages.map { m =>
        (m.channelId, m.hash, m.body)
      }.returning(messages.map(_.id))) +=
        (messageMeta.channelMessage.signedMessageData.data.channelId,
        messageMeta.blockHash,
        messageMeta.channelMessage.signedMessageData.data.message)

    op.dbRun()
  }

  def getLastNMessages(n: Long, channelId: Option[String] = None): Seq[MessageResponse] = {
    val query = channelId.map(id => messages.filter(_.channelId === id)).getOrElse(messages)
    val res = query.sortBy(_.id.desc).take(n).result.dbRun()
    res.map { r =>
      (MessageResponse.apply _).tupled(r)
    }
  }

  def printMessages(): Unit =
    db.run(messages.result)
      .foreach { msgs =>
        msgs.foreach { m =>
          logger.info(m.toString)
        }
      }(ConstellationExecutionContext.bounded)

}

object ChannelStorage {

  def apply(implicit dao: DAO): ChannelStorage =
    new ChannelStorage()
}
