package org.constellation.session

import cats.effect.{ContextShift, IO}
import cats.syntax.all._
import io.chrisdavenport.fuuid.FUUID
import org.constellation.ConstellationExecutionContext
import org.constellation.session.SessionTokenService.{
  EmptyHeaderToken,
  EmptyPeerToken,
  Token,
  TokenValid,
  TokensDontMatch
}
import org.scalatest.{BeforeAndAfter, FreeSpec, Matchers}

class SessionTokenServiceTest extends FreeSpec with Matchers with BeforeAndAfter {

  private implicit val contextShift: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.bounded)

  var sessionTokenService: SessionTokenService[IO] = _

  before {
    sessionTokenService = new SessionTokenService[IO]
  }

  "createToken" - {
    "should create new token using UUID" in {
      val result = sessionTokenService.createToken().unsafeRunSync

      FUUID.fromStringOpt(result.value).get.isInstanceOf[FUUID] shouldBe true
    }
  }

  "createAndSetNewOwnToken" - {
    "should create and set new own token" in {
      val token = sessionTokenService.createAndSetNewOwnToken().unsafeRunSync
      val result = sessionTokenService.ownToken.get.unsafeRunSync

      result shouldBe token.some
    }
  }

  "getOwnToken" - {
    "should get current value of node's token" in {
      val token = Token("863450f0-5ff9-4495-be02-937d012379f3")
      sessionTokenService.ownToken.set(token.some).unsafeRunSync()
      sessionTokenService.getOwnToken().unsafeRunSync() shouldBe token.some
    }
  }

  "updatePeerToken" - {
    "should update peer's token" in {
      val ip1 = "1.2.3.4"
      val ip2 = "5.6.7.8"
      val oldIp1Token = Token("1006c12f-f79e-42ef-8156-acca80e05175")
      val token1 = Token("863450f0-5ff9-4495-be02-937d012379f3")
      val token2 = Token("ba06d084-34a5-499f-9bad-cf7ed216dbeb")

      (sessionTokenService.peersTokens.set(Map(ip1 -> oldIp1Token)) >>
        sessionTokenService.updatePeerToken(ip1, token1) >>
        sessionTokenService.updatePeerToken(ip2, token2)).unsafeRunSync

      val result = sessionTokenService.peersTokens.get.unsafeRunSync

      result shouldBe Map(ip1 -> token1, ip2 -> token2)
    }
  }

  "getPeerToken" - {
    "should return token value for a given ip if token exists" in {
      val ip = "1.2.3.4"
      val token = Token("863450f0-5ff9-4495-be02-937d012379f3")
      sessionTokenService.peersTokens.set(Map(ip -> token)).unsafeRunSync
      val result = sessionTokenService.getPeerToken(ip).unsafeRunSync

      result shouldBe token.some
    }

    "should return None if no token is stored for a given ip" in {
      val ip = "1.2.3.4"
      val result = sessionTokenService.getPeerToken(ip).unsafeRunSync

      result shouldBe None
    }
  }

  "clearOwnToken" - {
    "should clear own token value" in {
      val token = Token("863450f0-5ff9-4495-be02-937d012379f3")
      (sessionTokenService.ownToken.set(token.some) >>
        sessionTokenService.clearOwnToken()).unsafeRunSync
      val result = sessionTokenService.ownToken.get.unsafeRunSync

      result shouldBe None
    }
  }

  "clearPeerToken" - {
    "should clear peer's token" in {
      val ip1 = "1.2.3.4"
      val ip2 = "5.6.7.8"
      val token1 = Token("863450f0-5ff9-4495-be02-937d012379f3")
      val token2 = Token("ba06d084-34a5-499f-9bad-cf7ed216dbeb")

      (sessionTokenService.peersTokens.set(Map(ip1 -> token1, ip2 -> token2)) >>
        sessionTokenService.clearPeerToken(ip1)).unsafeRunSync

      val result = sessionTokenService.peersTokens.get.unsafeRunSync

      result shouldBe Map(ip2 -> token2)
    }
  }

  "clear" - {
    "should clear both peers and owner token" in {
      val ip1 = "1.2.3.4"
      val token1 = Token("863450f0-5ff9-4495-be02-937d012379f3")
      val token2 = Token("ba06d084-34a5-499f-9bad-cf7ed216dbeb")

      (sessionTokenService.peersTokens.set(Map(ip1 -> token1)) >>
        sessionTokenService.ownToken.set(token2.some) >>
        sessionTokenService.clear()).unsafeRunSync

      val peersTokensResult = sessionTokenService.peersTokens.get.unsafeRunSync
      val ownTokenResult = sessionTokenService.ownToken.get.unsafeRunSync

      (ownTokenResult, peersTokensResult) shouldBe (None, Map.empty)
    }
  }

  "verifyPeerToken" - {
    "should pass validation" - {
      "if token stored for a peer and token in header are the same" in {
        val ip1 = "1.2.3.4"
        val token = Token("863450f0-5ff9-4495-be02-937d012379f3")
        val headerToken = token.some
        sessionTokenService.peersTokens.set(Map(ip1 -> token)).unsafeRunSync()

        val result = sessionTokenService.verifyPeerToken(ip1, headerToken).unsafeRunSync()

        result shouldBe TokenValid
      }
    }

    "should fail validation" - {
      "if token stored for a peer is different than token in header" in {
        val ip1 = "1.2.3.4"
        val token = Token("863450f0-5ff9-4495-be02-937d012379f3")
        val headerToken = Token("ba06d084-34a5-499f-9bad-cf7ed216dbeb").some
        sessionTokenService.peersTokens.set(Map(ip1 -> token)).unsafeRunSync()

        val result = sessionTokenService.verifyPeerToken(ip1, headerToken).unsafeRunSync()

        result shouldBe TokensDontMatch
      }

      "if there is no token stored for a given peer" in {
        val ip1 = "1.2.3.4"
        val headerToken = Token("ba06d084-34a5-499f-9bad-cf7ed216dbeb").some

        val result = sessionTokenService.verifyPeerToken(ip1, headerToken).unsafeRunSync()

        result shouldBe EmptyPeerToken
      }

      "if there is no token in header" in {
        val ip1 = "1.2.3.4"
        val token = Token("863450f0-5ff9-4495-be02-937d012379f3")
        val headerToken = None
        sessionTokenService.peersTokens.set(Map(ip1 -> token)).unsafeRunSync()

        val result = sessionTokenService.verifyPeerToken(ip1, headerToken).unsafeRunSync()

        result shouldBe EmptyHeaderToken
      }
    }
  }
}
