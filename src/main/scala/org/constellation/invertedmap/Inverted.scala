package org.constellation.invertedmap

object Inverted {
  def isEmpty(m: Map[_, _]): Boolean = m.isEmpty

  def contains[A](m: Map[_, Set[A]], x: A): Boolean =
    m.exists { case (_, v) => v.contains(x) }

  def get[A, B](m: Map[B, Set[A]], x: A): Option[B] =
    m.find { case (_, v) => v.contains(x) }.map { case (k, _) => k }

  def update[A, B, B1 >: B](m: Map[B1, Set[A]], k: A, v: B): Map[B1, Set[A]] = {
    val keys = m.getOrElse(v, Set.empty)
    this.delete(m, k).updated(v, keys ++ Set(k))
  }

  def delete[A, B](m: Map[B, Set[A]], x: A): Map[B, Set[A]] =
    m.find { case (_, v) => v.contains(x) }.map {
      case (v, keys) =>
        keys.filterNot(_ == x) match {
          case xs if xs.isEmpty => m - v
          case xs               => m.updated(v, xs)
        }
    }.getOrElse(m)

  def size(m: Map[_, _]): Int = m.size

  def isConverged(m: Map[_, _]): Boolean = m.keySet.size == 1

  def keysSize[A](m: Map[A, Set[_]], x: A) = m.get(x).map(_.size).getOrElse(0)

  def iterator[A, B](m: Map[B, Set[A]]): Iterator[(A, B)] =
    m.flatMap { case (v, keys) => keys.map(_ -> v) }.iterator

  def keysIterator[A, B](m: Map[B, Set[A]]): Iterator[A] =
    m.flatMap { case (v, keys) => keys.map(_ -> v) }.keysIterator

  def valuesIterator[A, B](m: Map[B, Set[A]]): Iterator[B] =
    m.flatMap { case (v, keys) => keys.map(_ -> v) }.valuesIterator
}
