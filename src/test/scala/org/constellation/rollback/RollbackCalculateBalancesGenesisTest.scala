package org.constellation.rollback

import constellation._
import org.constellation.{DAO, TestHelpers}
import org.constellation.keytool.KeyUtils
import org.constellation.primitives.Genesis
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalatest.{BeforeAndAfter, FreeSpec, Matchers}

class RollbackCalculateBalancesGenesisTest
    extends FreeSpec
    with ArgumentMatchersSugar
    with BeforeAndAfter
    with Matchers {

  implicit val dao: DAO = TestHelpers.prepareRealDao()

  private val rollbackAccountBalances: RollbackAccountBalances = new RollbackAccountBalances
  private val distributionTransactionAmount = 100000000000000L
  private val genesisTransactionAmount = 400000000000000000L

  private val genesisKeyPair = KeyUtils.makeKeyPair()
  private val tipKeyPair = KeyUtils.makeKeyPair()
  private val genesisObservation = Genesis.createGenesisAndInitialDistributionDirect(
    genesisKeyPair.getPublic.toId.address,
    Set(tipKeyPair.getPublic.toId),
    genesisKeyPair
  )

  "Genesis Observation" - {
    "should proceed calculations" in {
      val balances = rollbackAccountBalances.calculate(genesisObservation)

      balances.isRight shouldBe true
    }
    "should return error" in {
      val balances = rollbackAccountBalances.calculate(null)

      balances.isLeft shouldBe true
      balances.left.get shouldBe CannotCalculate
    }
    "should have two transaction (genesis and distribution)" in {
      val balances = rollbackAccountBalances.calculate(genesisObservation)

      balances.right.get.size shouldBe 2
    }
    "should return right balance for genesis address" in {
      val balances = rollbackAccountBalances.calculate(genesisObservation)
      val expectedBalance = genesisTransactionAmount - 2 * distributionTransactionAmount

      balances.right.get.get(genesisKeyPair.getPublic.toId.address) shouldBe Some(expectedBalance)
    }
    "should return right balance for distribution address" in {
      val balances = rollbackAccountBalances.calculate(genesisObservation)
      val expectedBalance = 2 * distributionTransactionAmount

      balances.right.get.get(tipKeyPair.getPublic.toId.address) shouldBe Some(expectedBalance)
    }
  }
}
