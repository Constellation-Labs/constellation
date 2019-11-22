package org.constellation.rollback

import org.constellation.primitives.Genesis
import org.constellation.util.AccountBalance
import org.constellation.{DAO, TestHelpers}
import org.mockito.ArgumentMatchersSugar
import org.scalatest.{BeforeAndAfter, FreeSpec, Matchers}

class RollbackCalculateBalancesGenesisTest
    extends FreeSpec
    with ArgumentMatchersSugar
    with BeforeAndAfter
    with Matchers {

  implicit val dao: DAO = TestHelpers.prepareRealDao()

  private val rollbackAccountBalances: RollbackAccountBalances = new RollbackAccountBalances
  private val initialBalances = Seq(
    AccountBalance("a", 10),
    AccountBalance("b", 20),
    AccountBalance("c", 30),
    AccountBalance("d", 50)
  )

  private val genesisObservationWithInitialBalances = Genesis.createGenesisObservation(initialBalances)
  private val emptyGenesisObservation = Genesis.createGenesisObservation(Seq.empty)

  "Genesis Observation" - {
    "should proceed calculations" in {
      val balances = rollbackAccountBalances.calculate(emptyGenesisObservation)

      balances.isRight shouldBe true
    }
    "should return error" in {
      val balances = rollbackAccountBalances.calculate(null)

      balances.isLeft shouldBe true
      balances.left.get shouldBe CannotCalculate
    }
    "should have transactions for each account" in {
      val balances = rollbackAccountBalances.calculate(genesisObservationWithInitialBalances)
      val genesisEmptyBlocksBalancesSize = 2 * 2 // 2 empty accounts participated in 2 empty transactions
      balances.right.get.size shouldBe initialBalances.size + genesisEmptyBlocksBalancesSize
    }
    "should return right balance for distribution address" in {
      val balances = rollbackAccountBalances.calculate(genesisObservationWithInitialBalances)
      initialBalances.foreach(ab => {
        balances.right.get.get(ab.accountHash) shouldBe Some(ab.balance * 100000000)
      })
    }
  }
}
