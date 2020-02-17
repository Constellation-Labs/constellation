package org.constellation.domain.redownload

import cats.effect.Concurrent
import cats.implicits._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.domain.redownload.MajorityStateChooser.{MajorityState, Occurrences}
import org.constellation.schema.Id

class MajorityStateChooser {

  // TODO: Use RedownloadService type definitions
  def chooseMajorityState(
    createdSnapshots: Map[Long, String],
    peersProposals: Map[Id, Map[Long, String]]
  ): MajorityState = {
    val peersSize = peersProposals.size + 1 // +1 - it's an own node
    val proposals = mergeByHeights(createdSnapshots, peersProposals)

    val flat = proposals
      .mapValues(_.sorted)
      .mapValues(mapToOccurrences)
      .mapValues(_.find(a => isInMajority(a.n, peersSize)))
      .mapValues(_.map(_.value))

    for ((k, Some(v)) <- flat) yield k -> v
  }

  private def isInMajority(occurrences: Int, total: Int): Boolean =
    if (total > 0)
      occurrences / total.toDouble >= 0.5
    else false

  private def mapValuesToList[K, V](a: Map[K, V]): Map[K, List[V]] =
    a.mapValues(List(_))

  private def mergeByHeights(
    createdSnapshots: Map[Long, String],
    peersProposals: Map[Id, Map[Long, String]]
  ): Map[Long, List[String]] =
    (peersProposals.mapValues(mapValuesToList).values.toList ++ List(mapValuesToList(createdSnapshots)))
      .fold(Map.empty)(_ |+| _)

  private def mapToOccurrences(values: List[String]): Set[Occurrences[String]] =
    values.toSet.map((a: String) => Occurrences(a, values.count(_ == a)))

}

object MajorityStateChooser {
  def apply(): MajorityStateChooser = new MajorityStateChooser()

  type MajorityState = Map[Long, String]

  case class Occurrences[A](value: A, n: Int)
}
