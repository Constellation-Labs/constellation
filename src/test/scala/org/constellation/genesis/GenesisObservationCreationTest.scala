package org.constellation.genesis

import org.constellation.primitives.{Genesis, Schema}
import org.constellation.util.AccountBalance
import org.constellation.{DAO, TestHelpers}
import org.mockito.ArgumentMatchersSugar
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FreeSpec, Matchers}

class GenesisObservationCreationTest
    extends FreeSpec
    with ArgumentMatchersSugar
    with BeforeAndAfter
    with BeforeAndAfterAll
    with Matchers {

  implicit var dao: DAO = _

  before {
    dao = TestHelpers.prepareRealDao()
  }

  after {
    dao.unsafeShutdown()
  }

  "genesis block should have coinbase tips" in {
    val go = Genesis.createGenesisObservation(Seq.empty)
    go.genesis.checkpoint.edge.observationEdge.parents.foreach(tip => {
      tip.hash shouldBe Genesis.Coinbase
    })
  }

  "with initial balances" - {
    "should create initial transactions from coinbase in genesis block" in {
      val balances = Seq(
        AccountBalance("a", 10L),
        AccountBalance("b", 30L)
      )
      val go: Schema.GenesisObservation = Genesis.createGenesisObservation(balances)
      go.genesis.transactions.size shouldBe balances.size

      balances.forall { ab =>
        go.genesis.transactions.exists { tx =>
          tx.edge.observationEdge.parents match {
            case Seq(a, b) => a.hash == Genesis.Coinbase && b.hash == ab.accountHash
          }
        }
      } shouldBe true

    }

    "should create 2 child blocks from genesis with 1 empty transaction" in {
      val balances = Seq(
        AccountBalance("a", 10L),
        AccountBalance("b", 30L)
      )
      val go = Genesis.createGenesisObservation(balances)

      val genesisHash = go.genesis.soe.hash

      go.initialDistribution.transactions.size shouldBe 1
      go.initialDistribution2.transactions.size shouldBe 1
      go.initialDistribution.checkpoint.edge.observationEdge.parents.foreach(parent => {
        parent.hash shouldBe genesisHash
      })
      go.initialDistribution2.checkpoint.edge.observationEdge.parents.foreach(parent => {
        parent.hash shouldBe genesisHash
      })
    }
  }

  "without initial balances" - {
    "should create empty genesis block" in {
      val go: Schema.GenesisObservation = Genesis.createGenesisObservation(Seq.empty)
      go.genesis.transactions.size shouldBe 0
    }

    "should create 2 child blocks from genesis with 1 empty transaction" in {
      val go = Genesis.createGenesisObservation(Seq.empty)

      val genesisHash = go.genesis.soe.hash

      go.initialDistribution.transactions.size shouldBe 1
      go.initialDistribution2.transactions.size shouldBe 1
      go.initialDistribution.checkpoint.edge.observationEdge.parents.foreach(parent => {
        parent.hash shouldBe genesisHash
      })
      go.initialDistribution2.checkpoint.edge.observationEdge.parents.foreach(parent => {
        parent.hash shouldBe genesisHash
      })
    }
  }
}
