package org.constellation.util

import org.constellation.primitives.Schema

import scala.io.Source

trait AccountBalanceReader {

  def read(): Seq[AccountBalance]
}

class AccountBalanceCSVReader(val filePath: String, normalized: Boolean) extends AccountBalanceReader {

  override def read(): Seq[AccountBalance] = {
    val source = Source.fromFile(filePath)
    val normalizationFactor = if (normalized) 1 else Schema.NormalizationFactor

    val values = for {
      line <- source.getLines().filter(_.nonEmpty).toVector
      values = line.split(",").map(_.trim)
      // Data needs to be provided in non-normalized manner
    } yield AccountBalance(values(0), (values(1).toDouble * normalizationFactor).toLong)

    source.close()
    values
  }
}

case class AccountBalance(accountHash: String, balance: Long)
