package org.constellation.primitives

import org.constellation.primitives.Schema.{Cell, CellKey, Sheaf}

import scala.collection.SortedSet
import scala.collection.concurrent.TrieMap

class ActiveDAGManager {

  @volatile var activeSheafs: Seq[Sheaf] = Seq()

  implicit val ordering: Ordering[Sheaf] = Ordering.by{ s: Sheaf => -1 * s.totalScore.getOrElse(0D)}

  val cellKeyToCell : TrieMap[CellKey, Cell] = TrieMap()

  // Put into appropriate cell
  def acceptSheaf(sheaf: Sheaf): Unit = {
    activeSheafs :+= sheaf
    if (!cellKeyToCell.contains(sheaf.cellKey)) {
      cellKeyToCell(sheaf.cellKey) = Cell(SortedSet(sheaf))
    } else {
      val cell = cellKeyToCell(sheaf.cellKey)
      cellKeyToCell(sheaf.cellKey) = cell.copy(members = cell.members + sheaf)
    }
  }

  def removeSheaf(sheaf: Sheaf): Unit = {
    cellKeyToCell.get(sheaf.cellKey).foreach{
      c =>
        val newMembers = c.members.filter {_ != sheaf}
        if (newMembers.isEmpty) cellKeyToCell.remove(sheaf.cellKey)
        else {
          cellKeyToCell(sheaf.cellKey) = c.copy(members = newMembers)
        }
    }
    activeSheafs = activeSheafs.filterNot(_ == sheaf)
  }

  def cleanup(height: Int): Unit = {

    // Add stuff in here to cleanup from bundleToSheaf and DB including their sub-bundles.
    val lowHeightSheafs = activeSheafs.filter(j => j.height.get < (height - 2))
    lowHeightSheafs.foreach(removeSheaf)

    var toRemove: Set[Sheaf] = Set()

    cellKeyToCell.foreach{
      case (ck, c) =>
        if (ck.height < (height - 2)) {
          cellKeyToCell.remove(ck)
        }
        if (c.members.size > 10) cellKeyToCell(ck) = c.copy(members =
          SortedSet(c.members.toSeq.zipWithIndex.sorted.filter{_._2 < 5}.map{_._1} : _*))
    }


    //activeSheafs.groupBy(z => z.bundle.ex z.bundle.maxStackDepth)
    
    if (activeSheafs.size > 150) {
      activeSheafs.sortBy(z => -1*z.totalScore.get).zipWithIndex.filter{_._2 > 65}.map{_._1}.foreach{
        removeSheaf
      }
    }

  }

}
