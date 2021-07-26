package org.constellation.schema.snapshot

import org.constellation.schema.Id
import org.constellation.schema.signature.Signable

import scala.collection.SortedMap

case class Snapshot(
  lastSnapshot: String,
  checkpointBlocks: Seq[String],
  publicReputation: SortedMap[Id, Double],
  nextActiveNodes: NextActiveNodes,
  authorizedNodes: Set[Id]
) extends Signable {
  override def toEncode: Seq[String] =
    (checkpointBlocks :+ lastSnapshot) ++
      nextActiveNodes.light.toSeq.map(_.hex).sorted ++
      nextActiveNodes.full.toSeq.map(_.hex).sorted ++
      authorizedNodes.toSeq.map(_.hex).sorted
}

object Snapshot {
  val snapshotZero: Snapshot = Snapshot("", Seq(), SortedMap.empty, NextActiveNodes(Set.empty, Set.empty), Set.empty)
}

case class NextActiveNodes(light: Set[Id], full: Set[Id])
