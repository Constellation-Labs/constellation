package org.constellation

import org.constellation.blockchain.{Block, Chain, GenesisBlock}
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.FlatSpec
import org.scalatest.prop.GeneratorDrivenPropertyChecks


class ChainTest extends FlatSpec with GeneratorDrivenPropertyChecks {

  implicit val arbitraryChain: Arbitrary[Chain] = Arbitrary {
    for {
      length <- Gen.choose(0, 30)
      text <- Gen.listOfN(length, Gen.alphaNumStr)
    } yield {
      val chain = Chain("id")
      text.foreach { data => chain.addBlock(chain.generateNextBlock(data)) }
      chain
    }
  }

  "Generated chains" should "always be correct" in forAll { chain: Chain =>
    assert( Chain.validChain( chain.blocks ) )
  }

  "For any given chain, the first block" must "be the Genesis block" in forAll { chain: Chain =>
    assertResult(GenesisBlock)(chain.firstBlock)
  }

  "Adding an invalid block" should "never work" in forAll { (chain: Chain, firstName: String, secondName: String) =>

    val firstNewBlock = chain.generateNextBlock(firstName)
    val secondNewBlock = chain.generateNextBlock(secondName)

    val currentBlockLength = chain.blocks.length

    val newChain = chain.addBlock(firstNewBlock).getOrElse( throw new IllegalStateException("Should succeed"))

    assert( ! newChain.validBlock(secondNewBlock) )
    newChain.addBlock(secondNewBlock)

    assertResult(newChain.blocks.length)(currentBlockLength +1)
    assertResult(newChain.latestBlock)(firstNewBlock)
  }

    "Second signature" must "Be linked to first" in forAll { chain: Chain =>
    val res = Block(0, "", 0, "testdata:node1:node2:node2", "", Some("node1"),  Some("node2"), Some("node2"))
    val test = chain.generateNextBlock("testdata:node1:node2:node2")
    assertResult(res.id1)(test.id1)
    assertResult(res.recipient)(test.recipient)
    assertResult(res.d2)(test.d2)
  }
}
