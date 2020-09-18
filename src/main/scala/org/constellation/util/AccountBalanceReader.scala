package org.constellation.util

import org.constellation.schema.Schema

import scala.io.Source

trait AccountBalanceReader {

  def read(): Seq[AccountBalance]
}

class AccountBalanceCSVReader(val filePath: String, normalized: Boolean) extends AccountBalanceReader {

  override def read(): Seq[AccountBalance] = {
    val source = Source.fromFile(filePath)
    val normalizationFactor = if (normalized) 1 else Schema.NormalizationFactor

    val values = for {
      line <- source.getLines().filter(_.nonEmpty).toList
      values = line.split(",").map(_.trim)
    } yield AccountBalance(values(0), (values(1).toDouble * normalizationFactor).toLong)

    source.close()

    val addresses = values.map(_.accountHash).toSet

    addresses.map { address =>
      (address -> values.filter(_.accountHash == address).map(_.balance).sum)
    }.map {
      case (a, b) => AccountBalance(a, b)
    }.toSeq

  }
}

case class AccountBalance(accountHash: String, balance: Long)
