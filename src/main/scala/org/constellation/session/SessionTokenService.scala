package org.constellation.session

import cats.effect.Concurrent
import cats.effect.concurrent.Ref
import cats.implicits._
import io.chrisdavenport.fuuid.FUUID
import io.circe.generic.semiauto._
import io.circe.{Decoder, Encoder}
import org.constellation.session.SessionTokenService._
import org.http4s.util.CaseInsensitiveString

class SessionTokenService[F[_]: Concurrent] {

  private[session] val peersTokens: Ref[F, Map[String, Token]] = Ref.unsafe(Map.empty)

  private[session] val ownToken: Ref[F, Option[Token]] = Ref.unsafe(None)

  def createAndSetNewOwnToken(): F[Token] =
    for {
      token <- createToken()
      _ <- ownToken.modify(_ => (token.some, ()))
    } yield token

  def getOwnToken(): F[Option[Token]] =
    ownToken.get

  def updatePeerToken(ip: String, token: Token): F[Unit] =
    peersTokens.modify(pTokens => (pTokens + (ip -> token), ()))

  def getPeerToken(ip: String): F[Option[Token]] =
    peersTokens.get.map(_.get(ip))

  def verifyPeerToken(ip: String, headerToken: Option[Token]): F[TokenVerificationResult] =
    for {
      peerToken <- getPeerToken(ip)
      verificationResult = (peerToken, headerToken) match {
        case (None, _)                            => EmptyPeerToken
        case (_, None)                            => EmptyHeaderToken
        case (pToken, hToken) if pToken == hToken => TokenValid
        case _                                    => TokensDontMatch
      }
    } yield verificationResult

  def clearOwnToken(): F[Unit] =
    ownToken.modify(_ => (None, ()))

  def clearPeerToken(ip: String): F[Unit] =
    peersTokens.modify(tokens => (tokens - ip, ()))

  def clear(): F[Unit] =
    for {
      _ <- clearOwnToken()
      _ <- peersTokens.modify(_ => (Map.empty, ()))
    } yield ()

  private[session] def createToken(): F[Token] =
    for {
      uuid <- FUUID.randomFUUID
    } yield Token(uuid.toString)
}

object SessionTokenService {
  case class Token(value: String) extends AnyVal

  object Token {
    implicit val tokenEncoder: Encoder[Token] = deriveEncoder
    implicit val tokenDecoder: Decoder[Token] = deriveDecoder
  }

  val `X-Session-Token` = CaseInsensitiveString("X-Session-Token")

  trait TokenVerificationResult
  case object EmptyPeerToken extends TokenVerificationResult
  case object EmptyHeaderToken extends TokenVerificationResult
  case object TokensDontMatch extends TokenVerificationResult
  case object TokenValid extends TokenVerificationResult
}
