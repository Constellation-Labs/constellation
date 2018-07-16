package org.constellation.primitives

import org.constellation.primitives.Schema.{Cell, CellKey, Sheaf}

import scala.collection.concurrent.TrieMap

class ActiveDAGManager {


  @volatile var activeSheafs: Seq[Sheaf] = Seq()

  val cellKeyToCell : TrieMap[CellKey, Cell] = TrieMap()

  // Put into appropriate cell
  def acceptSheaf(sheaf: Sheaf): Unit = {
    activeSheafs :+= sheaf
    // cellKeyToCell(sheaf.cellKey) =
  }

  def cleanup(height: Int): Unit = {


    // Add stuff in here to cleanup from bundleToSheaf and DB including their sub-bundles.
    activeSheafs = activeSheafs.filter(j => j.height.get > (height - 4))

    if (activeSheafs.size > 80) {
      activeSheafs = activeSheafs.sortBy(z => -1*z.totalScore.get).zipWithIndex.filter{_._2 < 65}.map{_._1}
    }

  }

}
