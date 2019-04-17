package org.constellation.primitives.storage

// Use H2Profile to connect to an H2 database
import slick.jdbc.H2Profile.api._

import scala.concurrent.ExecutionContext.Implicits.global

class ChannelStorage {
  // Definition of the SUPPLIERS table
  class Message(tag: Tag) extends Table[(Int, String, String, String)](tag, "MESSAGE") {
    def id = column[Int]("MSG_ID", O.PrimaryKey) // This is the primary key column
    def channelId = column[String]("CHANNEL_ID")
    def hash = column[String]("HASH")
    def body = column[String]("BODY")
    // Every table needs a * projection with the same type as the table's type parameter
    def * = (id, channelId, hash, body)
  }
  val messages = TableQuery[Message]

  messages.schema.createIfNotExists

}
