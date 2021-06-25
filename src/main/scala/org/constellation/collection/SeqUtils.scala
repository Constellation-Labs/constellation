package org.constellation.collection

import org.constellation.schema.Id

object SeqUtils {

  implicit class PathSeqOps(val seq: Seq[Id]) {
    def formatPath: String = seq.map(_.short).mkString(" -> ")
  }

}
