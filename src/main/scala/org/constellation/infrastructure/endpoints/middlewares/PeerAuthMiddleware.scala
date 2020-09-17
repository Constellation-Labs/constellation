package org.constellation.infrastructure.endpoints.middlewares

import java.io.ByteArrayOutputStream
import java.security.{KeyPair, MessageDigest, PrivateKey, PublicKey}

import cats.data.{Kleisli, OptionT}
import cats.effect.concurrent.Ref
import cats.effect.{Concurrent, ContextShift, Resource, Sync}
import cats.syntax.all._
import fs2.{Chunk, Stream}
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.keytool.KeyUtils
import org.constellation.p2p.Cluster
import org.constellation.primitives.IPManager.IP
import org.constellation.schema.Id
import org.constellation.session.SessionTokenService
import org.constellation.session.Registration.`X-Id`
import org.constellation.session.SessionTokenService.{Token, TokenValid, `X-Session-Token`}
import org.http4s._
import org.http4s.client.Client
import org.http4s.dsl.io._
import pl.abankowski.httpsigner.{HttpCryptoConfig, SignatureValid}
import pl.abankowski.httpsigner.http4s.{
  Http4sRequestSigner,
  Http4sRequestVerifier,
  Http4sResponseSigner,
  Http4sResponseVerifier
}
import pl.abankowski.httpsigner.signature.{Generator, Verifier}
import pl.abankowski.httpsigner.signature.generic.{GenericGenerator, GenericVerifier}

object PeerAuthMiddleware {

  def enforceKnownPeersMiddleware[F[_]: Sync](
    knownPeer: IP => F[Option[Id]],
    whitelistedId: Id => Boolean
  )(http: HttpRoutes[F]): HttpRoutes[F] =
    Kleisli { req: Request[F] =>
      val ip = req.remoteAddr.getOrElse("unknown")
      val logger = Slf4jLogger.getLogger[F]
      val unauthorizedResponse = OptionT.pure[F](Response[F](status = Unauthorized))

      val isKnownAndWhitelisted = for {
        _ <- OptionT.liftF(
          logger.debug(
            s"Middleware (Enforce known peers) ip=$ip, uri=${req.uri.path}, method=${req.method.name}, query=${req.uri.query.renderString}, headers=${req.headers}"
          )
        )
        _ <- OptionT(knownPeer(ip).map(_.filter(whitelistedId)))
        res <- http(req)
      } yield res

      isKnownAndWhitelisted.orElse(unauthorizedResponse)
    }

  def responseSignerMiddleware[F[_]: Sync](
    privateKey: PrivateKey,
    sessionTokenService: SessionTokenService[F]
  )(http: HttpRoutes[F])(implicit C: ContextShift[F]): HttpRoutes[F] =
    Kleisli { req: Request[F] =>
      for {
        res <- http(req)
        headerToken <- sessionTokenService
          .getOwnToken()
          .map(_.map(t => Header(`X-Session-Token`.value, t.value)))
          .attemptT
          .toOption
        newHeaders = headerToken.fold(res.headers)(h => res.headers.put(h))
        resWithHeader = res.copy(headers = newHeaders)
        signedResponse <- new Http4sResponseSigner[F](getGenerator(privateKey), new ConstellationHttpCryptoConfig {})
          .sign(resWithHeader)
          .attemptT
          .toOption
      } yield signedResponse
    }

  def responseTokenVerifierMiddleware[F[_]: Concurrent](
    client: Client[F],
    sessionTokenService: SessionTokenService[F]
  ): Client[F] =
    Client { (req: Request[F]) =>
      client.run(req).flatMap { response =>
        Resource.liftF {
          val peerIp = req.uri.authority.map(_.host.value).getOrElse("unknown")
          val headerToken = response.headers
            .get(`X-Session-Token`)
            .map(h => Token(h.value))

          sessionTokenService.verifyPeerToken(peerIp, headerToken) >>= {
            case TokenValid => response.pure[F]
            case _          => Response[F](status = Unauthorized).pure[F]
          }
        }
      }
    }

  def responseVerifierMiddleware[F[_]](peerId: Id)(
    client: Client[F]
  )(implicit F: Concurrent[F], C: ContextShift[F]): Client[F] =
    Client { (req: Request[F]) =>
      val verifier =
        new Http4sResponseVerifier[F](getVerifier(peerId.toPublicKey), new ConstellationHttpCryptoConfig {})

      import fs2._

      client.run(req).flatMap { response =>
        Resource.suspend {
          Ref[F].of(Vector.empty[Chunk[Byte]]).map { vec =>
            Resource.liftF {

              val copiedBody = Stream
                .eval(vec.get)
                .flatMap(v => Stream.emits(v).covary[F])
                .flatMap(c => Stream.chunk(c).covary[F])

              F.pure {
                response.copy(
                  body = response.body
                    .observe(_.chunks.flatMap(s => Stream.eval_(vec.update(_ :+ s))))
                )
              }.flatMap(verifier.verify).flatMap {
                case SignatureValid => F.pure(response.withBodyStream(copiedBody))
                case _ =>
                  F.pure(Response[F](status = Unauthorized))
              }
            }
          }
        }
      }
    }

  def requestSignerMiddleware[F[_]](
    client: Client[F],
    privateKey: PrivateKey,
    selfIp: String,
    sessionTokenService: SessionTokenService[F],
    selfId: Id
  )(
    implicit F: Concurrent[F],
    C: ContextShift[F]
  ): Client[F] =
    Client { req: Request[F] =>
      import fs2._

      val logger = Slf4jLogger.getLogger[F]
      val signer = new Http4sRequestSigner(getGenerator(privateKey), new ConstellationHttpCryptoConfig {})

      Resource.suspend {
        Ref[F].of(Vector.empty[Chunk[Byte]]).map { vec =>
          Resource.liftF {

            val copiedBody = Stream
              .eval(vec.get)
              .flatMap(v => Stream.emits(v).covary[F])
              .flatMap(c => Stream.chunk(c).covary[F])

            for {
              _ <- logger.debug(
                s"Middleware (Signer) ip=$selfIp, uri=${req.uri.path}, method=${req.method.name}, query=${req.uri.query.renderString}, headers=${req.headers}}"
              )
              tokenHeader: Option[Header.Raw] <- sessionTokenService
                .getOwnToken()
                .map(_.map(t => Header(`X-Session-Token`.value, t.value)))
              newHeaders = tokenHeader
                .fold(req.headers)(h => req.headers.put(h))
                .put(Header(`X-Id`.value, selfId.hex))
              newReq = req
                .withBodyStream(req.body.observe(_.chunks.flatMap(s => Stream.eval_(vec.update(_ :+ s)))))
                .withHeaders(newHeaders)
              signedRequest <- signer.sign(newReq).map(r => req.withBodyStream(copiedBody).withHeaders(r.headers))
            } yield signedRequest
          }
        }
      } >>= client.run
    }

  def requestTokenVerifierMiddleware[F[_]: Concurrent](
    sessionTokenService: SessionTokenService[F]
  )(http: HttpRoutes[F]): HttpRoutes[F] =
    Kleisli { req: Request[F] =>
      val ip = req.remoteAddr.getOrElse("unknown")
      val unauthorizedResponse: OptionT[F, Response[F]] = OptionT.pure[F](Response[F](status = Unauthorized))

      val headerToken = req.headers
        .get(`X-Session-Token`)
        .map(h => Token(h.value))

      for {
        tokenVerificationResult <- sessionTokenService.verifyPeerToken(ip, headerToken).attemptT.toOption
        response <- tokenVerificationResult match {
          case TokenValid => http(req)
          case _          => unauthorizedResponse
        }
      } yield response
    }

  def requestVerifierMiddleware[F[_]](
    knownPeer: IP => F[Option[Id]],
    whitelistedId: Id => Boolean
  )(http: HttpRoutes[F])(implicit C: ContextShift[F], F: Concurrent[F]): HttpRoutes[F] =
    Kleisli { req: Request[F] =>
      val logger = Slf4jLogger.getLogger[F]
      val ip = req.remoteAddr.getOrElse("unknown")
      val idFromHeaders = req.headers.get(`X-Id`).map(_.value).map(Id(_))
      val unauthorizedResponse: OptionT[F, Response[F]] = OptionT.pure[F](Response[F](status = Unauthorized))

      val verify: OptionT[F, Response[F]] = for {
        _ <- OptionT.liftF(
          logger.debug(
            s"Middleware (Verifier) ip=$ip, uri=${req.uri.path}, method=${req.method.name}, query=${req.uri.query.renderString}, headers=${req.headers}"
          )
        )

        tuple <- Ref[F]
          .of(Vector.empty[Chunk[Byte]])
          .map { vec =>
            val newBody = Stream
              .eval(vec.get)
              .flatMap(v => Stream.emits(v).covary[F])
              .flatMap(c => Stream.chunk(c).covary[F])
            val newReq = req.withBodyStream(req.body.observe(_.chunks.flatMap(s => Stream.eval_(vec.update(_ :+ s)))))
            (newBody, newReq)
          }
          .attemptT
          .toOption

        id <- knownPeer(ip)
          .map(_.orElse(idFromHeaders.filter(whitelistedId)))
          .attemptT
          .toOption
          .flatMap(_.toOptionT)

        crypto = getVerifier(id.toPublicKey)
        verifier = new Http4sRequestVerifier[F](crypto, new ConstellationHttpCryptoConfig {})

        verifierResult <- verifier
          .verify(tuple._2)
          .attemptT
          .toOption

        response <- verifierResult match {
          case SignatureValid => http(req.withBodyStream(tuple._1))
          case _              => unauthorizedResponse
        }
      } yield response

      verify.orElse(unauthorizedResponse)
    }

  private def getVerifier(publicKey: PublicKey): Verifier =
    GenericVerifier(KeyUtils.DefaultSignFunc, KeyUtils.provider, publicKey)

  private def getGenerator(privateKey: PrivateKey): Generator =
    GenericGenerator(KeyUtils.DefaultSignFunc, KeyUtils.provider, privateKey)

  trait ConstellationHttpCryptoConfig extends HttpCryptoConfig {
    override val protectedHeaders = Set(
      "Content-Type",
      "Cookie",
      "Referer",
      `X-Session-Token`.value
    )
  }
}
