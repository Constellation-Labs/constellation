package org.constellation.rollback

import org.constellation.primitives.Genesis
import org.constellation.primitives.Schema.GenesisObservation
import org.constellation.util.AccountBalance
import org.constellation.{DAO, TestHelpers}
import org.mockito.ArgumentMatchersSugar
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FreeSpec, Matchers}

class RollbackCalculateBalancesGenesisTest
    extends FreeSpec
    with ArgumentMatchersSugar
    with BeforeAndAfter
    with BeforeAndAfterAll
    with Matchers {

  implicit var dao: DAO = _

  private var rollbackAccountBalances: RollbackAccountBalances = _
  private val initialBalances = Seq(
    AccountBalance("a", 10),
    AccountBalance("b", 20),
    AccountBalance("c", 30),
    AccountBalance("d", 50)
  )

  private var genesisObservationWithInitialBalances: GenesisObservation = _
  private var emptyGenesisObservation: GenesisObservation = _

  before {
    dao = TestHelpers.prepareRealDao()
    rollbackAccountBalances = new RollbackAccountBalances
    genesisObservationWithInitialBalances = Genesis.createGenesisObservation(initialBalances)
    emptyGenesisObservation = Genesis.createGenesisObservation(Seq.empty)
  }

  after {
    dao.unsafeShutdown()
  }

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
