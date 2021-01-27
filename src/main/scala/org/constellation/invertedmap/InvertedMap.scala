package org.constellation.invertedmap

import scala.collection.generic.{CanBuildFrom, GenMapFactory}
import scala.collection.immutable.MapLike
import scala.collection.{GenTraversableOnce, mutable}

final class InvertedMap[A, B] private (m: Map[B, Set[A]])
    extends Map[A, B]
    with MapLike[A, B, InvertedMap[A, B]]
    with Serializable {

  override protected[this] def newBuilder: mutable.Builder[(A, B), InvertedMap[A, B]] =
    InvertedMap.newBuilder[A, B]

  override def size: Int = Inverted.size(m)

  def this() = this(Map.empty[B, Set[A]])

  override def empty: InvertedMap[A, B] = InvertedMap.empty[A, B]

  override def updated[B1 >: B](key: A, value: B1): InvertedMap[A, B1] =
    new InvertedMap(Inverted.update(m.toMap[B1, Set[A]], key, value))

  override def +[B1 >: B](kv: (A, B1)): InvertedMap[A, B1] =
    updated(kv._1, kv._2)

  override def +[B1 >: B](elem1: (A, B1), elem2: (A, B1), elems: (A, B1)*): InvertedMap[A, B1] =
    this + elem1 + elem2 ++ elems

  def -(key: A): InvertedMap[A, B] =
    if (!Inverted.contains(m, key)) this
    else new InvertedMap(Inverted.delete(m, key))

  override def get(key: A): Option[B] = Inverted.get(m, key)

  override def iterator: Iterator[(A, B)] = Inverted.iterator(m)
  override def keysIterator: Iterator[A] = Inverted.keysIterator(m)
  override def valuesIterator: Iterator[B] = Inverted.valuesIterator(m)

  override def contains(key: A): Boolean = Inverted.contains(m, key)
  override def isDefinedAt(key: A): Boolean = Inverted.contains(m, key)

  def isConverged: Boolean = Inverted.isConverged(m)
  def keysSize(value: B): Int = Inverted.keysSize(m, value)
  def underlyingMap: Map[B, Set[A]] = m
}

object InvertedMap extends GenMapFactory[InvertedMap] {
  def empty[A, B] = new InvertedMap[A, B]()

  def apply[A, B](m: Map[A, B]): InvertedMap[A, B] = InvertedMap(m.toSeq: _*)

  implicit def canBuildFrom[A, B]: CanBuildFrom[Coll, (A, B), InvertedMap[A, B]] = new MapCanBuildFrom[A, B]
}
