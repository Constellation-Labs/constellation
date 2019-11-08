package org.constellation.schema

case class CheckpointEdgeData(
  hashes: Seq[String],
  messageHashes: Seq[String] = Seq()
)(implicit hashGenerator: HashGenerator)
    extends Signable {

  override def hash: String = hashGenerator.hash(this)
}
