package org.constellation.util

import scala.io.Source

trait AccountBalanceReader {

  def read(): Seq[AccountBalance]
}

class AccountBalanceCSVReader(val filePath: String) extends AccountBalanceReader {

  override def read(): Seq[AccountBalance] = {
    val source = Source.fromFile(filePath)

    val values = for {
      line <- source.getLines().filter(_.nonEmpty).toVector
      values = line.split(",").map(_.trim)
    } yield AccountBalance(values(0), values(1).toLong)

    source.close()
    values
  }
}

case class AccountBalance(accountHash: String, balance: Long)
