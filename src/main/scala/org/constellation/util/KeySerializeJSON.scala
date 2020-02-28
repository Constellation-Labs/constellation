package org.constellation.util

import java.security.{KeyPair, PrivateKey, PublicKey}

import org.constellation.domain.redownload.MajorityStateChooser.SnapshotProposal
import org.constellation.keytool.KeyUtils.{hexToPrivateKey, hexToPublicKey, privateKeyToHex, publicKeyToHex}
import org.constellation.schema.Id
import org.json4s.JsonAST.{JDouble, JString, JValue}
import org.json4s.{CustomKeySerializer, CustomSerializer, Extraction, Formats, JObject}

import scala.collection.SortedMap

trait KeySerializeJSON {

  implicit val constellationFormats: Formats

  class PrivateKeySerializer
      extends CustomSerializer[PrivateKey](
        format =>
          ({
            case jObj: JObject =>
              // implicit val f: Formats = format
              hexToPrivateKey((jObj \ "key").extract[String])
          }, {
            case key: PrivateKey =>
              JObject("key" -> JString(privateKeyToHex(key)))
          })
      )

  class PublicKeySerializer
      extends CustomSerializer[PublicKey](
        format =>
          ({
            case jstr: JObject =>
              // implicit val f: Formats = format
              hexToPublicKey((jstr \ "key").extract[String])
          }, {
            case key: PublicKey =>
              JObject("key" -> JString(publicKeyToHex(key)))
          })
      )

  class KeyPairSerializer
      extends CustomSerializer[KeyPair](
        format =>
          ({
            case jObj: JObject =>
              //  implicit val f: Formats = format
              val pubKey = (jObj \ "publicKey").extract[PublicKey]
              val privKey = (jObj \ "privateKey").extract[PrivateKey]
              val kp = new KeyPair(pubKey, privKey)
              kp
          }, {
            case key: KeyPair =>
              //  implicit val f: Formats = format
              JObject(
                "publicKey" -> JObject("key" -> JString(publicKeyToHex(key.getPublic))),
                "privateKey" -> JObject("key" -> JString(privateKeyToHex(key.getPrivate)))
              )
          })
      )

  class IdSerializer
      extends CustomKeySerializer[Id](
        format =>
          ({
            case s: String =>
              Id(s)
          }, {
            case id: Id =>
              id.hex
          })
      )

  class SnapshotProposalSerializer
      extends CustomSerializer[SnapshotProposal](
        format =>
          ({
            case jObj: JObject =>
              val hash = (jObj \ "hash").extract[String]
              val reputation = (jObj \ "reputation").extract[Map[Id, Double]]
              SnapshotProposal(hash, SortedMap(reputation.toSeq: _*))
          }, {
            case proposal: SnapshotProposal =>
              JObject(
                "hash" -> JString(proposal.hash),
                "reputation" -> {
                  JObject(
                    proposal.reputation
                      .mapValues(JDouble)
                      .toList
                      .map {
                        case (id, trust) => (id.hex, trust)
                      }
                  )
                }
              )
          })
      )

}
