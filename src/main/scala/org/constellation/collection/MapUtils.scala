package org.constellation.collection

import org.constellation.schema.snapshot.HeightRange
import cats.syntax.all._

object MapUtils {

  implicit class HeightMapOps[V](val map: Map[Long, V]) {

    def removeHeightsBelow(height: Long): Map[Long, V] =
      map.filterKeys(_ >= height).toSeq.toMap // `.toSeq.toMap` is used for strict (eager) filtering

    def removeHeightsAbove(height: Long): Map[Long, V] =
      map.filterKeys(_ <= height).toSeq.toMap // `.toSeq.toMap` is used for strict (eager) filtering

    def minHeight: Long = minHeightEntry.map(_._1).getOrElse(0)

    def maxHeight: Long = maxHeightEntry.map(_._1).getOrElse(0)

    def minHeightEntry: Option[(Long, V)] = map.toList.minimumByOption { case (height, _) => height }

    def maxHeightEntry: Option[(Long, V)] = map.toList.maximumByOption { case (height, _) => height }

    def minHeightValue: Option[V] = minHeightEntry.map(_._2)

    def maxHeightValue: Option[V] = maxHeightEntry.map(_._2)

    def heightRange: HeightRange = HeightRange(minHeight, maxHeight)
  }
}
