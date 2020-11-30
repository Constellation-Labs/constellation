package org.constellation.gossip.sampling

import io.circe._
import io.circe.generic.semiauto._
import org.constellation.schema.Id

case class GossipPath private (path: IndexedSeq[Id], id: String, cursor: Int = 0) {

  def forward: GossipPath =
    if (hasNext) GossipPath(path, id, cursor + 1) else this

  def backward: GossipPath =
    if (hasPrev) GossipPath(path, id, cursor - 1) else this

  def isCompleted: Boolean = !hasNext

  def hasNext: Boolean = next.isDefined

  def hasPrev: Boolean = prev.isDefined

  def next: Option[Id] = path.lift(cursor + 1)

  def isCurrent(id: Id): Boolean = curr.contains(id)

  def curr: Option[Id] = path.lift(cursor)

  def isFirst(id: Id): Boolean = path.head == id

  def isLast(id: Id): Boolean = path.last == id

  def isNext(id: Id): Boolean = next.contains(id)

  def isPrev(id: Id): Boolean = prev.contains(id)

  def prev: Option[Id] = path.lift(cursor - 1)

  def first: Id = path.head

  def toIndexedSeq: IndexedSeq[Id] = path
}

object GossipPath {
  def apply(path: IndexedSeq[Id], id: String): GossipPath = new GossipPath(path, id)

  implicit val gossipPathEncoder: Encoder[GossipPath] = deriveEncoder
  implicit val gossipPathDecoder: Decoder[GossipPath] = deriveDecoder
}
