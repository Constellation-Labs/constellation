package org.constellation

import better.files.File
import com.codahale.shamir.Scheme
import org.scalatest.FlatSpec
import scala.collection.JavaConverters._

import constellation._
import org.constellation.crypto.KeyUtils._

case class ShamirOutput(fileName: String, part: Int, hex: String)

class ShamirETHTest extends FlatSpec {

  val encodedStore = File(System.getenv("HOME"), "yourfile.txt")
  val ethKeyStore = File(System.getenv("HOME"), "Library/Ethereum/keystore")

  "Shamir java" should "work" in {

    val scheme = Scheme.of(5, 3)
    val input = "hello there"
    val secret = input.getBytes()
    val parts = scheme.split(secret)
    val recovered = scheme.join(parts)
    val output = new String(recovered)
    assert(input == output)

  }

  "Shamir" should "work" ignore {

    val files = ethKeyStore.list.filter {
      _.name != ".DS_Store"
    }.flatMap{
        f =>
          val scheme = Scheme.of(3, 2)
          val secret = f.byteArray
          val parts = scheme.split(secret)
          val done = parts.asScala.map{ case (index, bytes) =>
            ShamirOutput(f.name, index, bytes2hex(bytes))
          }.toSeq
          done.foreach{z => println(z.json)}
          done
      }
    encodedStore.writeText(files.map{_.json}.mkString("\n"))
  }

  "Shamir read in" should "work" ignore {

    val data = encodedStore.lines.map{_.x[ShamirOutput]}

    val scheme = Scheme.of(3, 2)

    data.groupBy(
      _.fileName
    ).foreach{
      case (fileName, parts) =>

        val partsOut = parts.map{p => p.part.asInstanceOf[Integer] -> hex2bytes(p.hex)}.toMap.asJava
        val recovered = scheme.join(partsOut)
        val output = new String(recovered)
        println(output)
        val file = File(ethKeyStore, fileName)
        file.writeText(output)
    }

  }

}
