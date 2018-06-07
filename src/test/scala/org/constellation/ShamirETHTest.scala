package org.constellation

import java.io.File
import java.nio.file.{Files, Paths}

import org.scalatest.FlatSpec
import com.codahale.shamir.Scheme
import constellation._

import scala.collection.JavaConverters._

case class ShamirOutput(fileName: String, part: Int, hex: String)

class ShamirETHTest extends FlatSpec {

  val encodedStore = new File(System.getenv("HOME"), "yourfile.txt")
  val ethKeyStore = new File(System.getenv("HOME"), "Library/Ethereum/keystore")


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

    val files = ethKeyStore.listFiles().filter {
      _.getName != ".DS_Store"
    }.flatMap{
        f =>
          val scheme = Scheme.of(3, 2)
          val secret = Files.readAllBytes(f.toPath)
          val parts = scheme.split(secret)
          val done = parts.asScala.map{ case (index, bytes) =>
            ShamirOutput(f.getName, index, bytes2hex(bytes))
          }.toSeq
          done.foreach{z => println(z.json)}
          done
      }
    scala.tools.nsc.io.File(encodedStore).writeAll(files.map{_.json}.mkString("\n"))
  }

  "Shamir read in" should "work" ignore {

    val data = scala.io.Source.fromFile(encodedStore).getLines().toSeq.map{_.x[ShamirOutput]}

    val scheme = Scheme.of(3, 2)

    data.groupBy(
      _.fileName
    ).foreach{
      case (fileName, parts) =>

        val partsOut = parts.map{p => p.part.asInstanceOf[Integer] -> hex2bytes(p.hex)}.toMap.asJava
        val recovered = scheme.join(partsOut)
        val output = new String(recovered)
        println(output)
        val file = new File(ethKeyStore, fileName)
        scala.tools.nsc.io.File(file).writeAll(output)
    }


  }

}
