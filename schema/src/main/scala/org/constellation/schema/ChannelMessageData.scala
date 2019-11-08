package org.constellation.schema

case class ChannelMessageData(
  message: String,
  previousMessageHash: String,
  channelId: String
)(implicit hashGenerator: HashGenerator)
    extends Signable {

  override def hash: String = hashGenerator.hash(this)
}
