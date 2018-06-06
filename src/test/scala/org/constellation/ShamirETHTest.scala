package org.constellation

import java.io.File

import org.scalatest.FlatSpec
import com.codahale.shamir.Scheme
import constellation._

import scala.collection.JavaConverters._

case class ShamirOutput(fileName: String, part: Int, b64Bytes: String)

class ShamirETHTest extends FlatSpec {

  "Shamir java" should "work" in {

    val scheme = Scheme.of(5, 3)
    val input = "hello there"
    val secret = input.getBytes()
    val parts = scheme.split(secret)
    val recovered = scheme.join(parts)
    val output = new String(recovered)
    assert(input == output)

  }

  "Shamir" should "work" in {

    new File(System.getenv("HOME"), "Library/Ethereum/keystore").listFiles().map{
      f =>
        val data = scala.io.Source.fromFile(f).getLines().mkString
        val scheme = Scheme.of(3, 2)
        val secret = data.getBytes
        val parts = scheme.split(secret)
        val done = parts.asScala.map{ case (index, bytes) =>
          ShamirOutput(f.getName, index, base64(bytes))
        }.toSeq
        done.foreach{z => println(z.json)}
        done
    }

  }

  "Shamir read in" should "work" in {

    val input = new File(System.getenv("HOME"), "yourfile.txt")

    val data = scala.io.Source.fromFile(input).getLines().toSeq.map{_.x[ShamirOutput]}

    val scheme = Scheme.of(3, 2)

    data.groupBy(
      _.fileName
    ).foreach{
      case (fileName, parts) =>

        val partsOut = parts.map{p => p.part.asInstanceOf[Integer] -> fromBase64(p.b64Bytes)}.toMap.asJava
        val recovered = scheme.join(partsOut)
        val output = new String(recovered)
        val file = new File(System.getenv("HOME"), "Library/Ethereum/keystore/" + fileName)
        scala.tools.nsc.io.File(file).writeAll(output)
    }


  }

}
