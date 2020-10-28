package org.constellation.gossip

import org.constellation.schema.Id

case class GossipPath private (path: IndexedSeq[Id], cursor: Int = 0) {

  def accept: GossipPath =
    if (hasNext) GossipPath(path, cursor + 1) else this

  def isCompleted: Boolean = !hasNext

  def hasNext: Boolean = next.isDefined

  def next: Option[Id] = path.lift(cursor + 1)

  def isCurrent(id: Id): Boolean = curr.contains(id)

  def curr: Option[Id] = path.lift(cursor)

  def isNext(id: Id): Boolean = next.contains(id)

  def isPrev(id: Id): Boolean = prev.contains(id)

  def prev: Option[Id] = path.lift(cursor - 1)
}

object GossipPath {
  def apply(path: IndexedSeq[Id]): GossipPath = new GossipPath(path)
}
