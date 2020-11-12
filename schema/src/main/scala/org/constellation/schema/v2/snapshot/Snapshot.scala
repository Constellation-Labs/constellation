package org.constellation.schema.v2.snapshot

import org.constellation.schema.v2.Id
import org.constellation.schema.v2.signature.Signable

import scala.collection.SortedMap

case class Snapshot(lastSnapshot: String, checkpointBlocks: Seq[String], publicReputation: SortedMap[Id, Double])
    extends Signable {
  override def toEncode = checkpointBlocks :+ lastSnapshot
}

object Snapshot {
  val snapshotZero: Snapshot = Snapshot("", Seq(), SortedMap.empty)
}
