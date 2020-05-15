package io.gatling.simulation

import java.util

import io.confluent.kafka.serializers.KafkaAvroSerializer
import io.gatling.core.Predef._
import io.gatling.data.generator.RandomDataGenerator
import io.gatling.kafka.{KafkaProducerBuilder, KafkaProducerProtocol}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.ProducerConfig

class SimulationWithAvroSchema extends Simulation {
  val kafkaTopic = "event_ma_banque"
  val kafkaBrokers = "35.180.127.210:9092"

  val props = new util.HashMap[String, Object]()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers)
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer])
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer])
  props.put("schema.registry.url", "http://35.180.127.210:8081")

//  val user_schema =
//    s"""
//       | {
//       |    "fields": [
//       |        { "name": "int1", "type": "int" }
//       |    ],
//       |    "name": "myrecord",
//       |    "type": "record"
//       |}
//     """.stripMargin
  val user_schema = """{
  "doc": "fields[1] représente le header de l'evenement, fields[2] représente la partie businessContext",
  "fields": [
    {
      "name": "EventHeader",
      "type": {
        "fields": [
          {
            "name": "eventId",
            "type": "string"
          },
          {
            "doc": "Au format Timestamp UNIX",
            "logicalType": "timestamp-millis",
            "name": "dateTimeRef",
            "type": "long"
          },
          {
            "doc": "Code Nomenclature de l'événement",
            "name": "nomenclatureEv",
            "type": "string"
          },
          {
            "name": "canal",
            "type": "int"
          },
          {
            "name": "media",
            "type": "int"
          },
          {
            "name": "schemaVersion",
            "type": "string"
          },
          {
            "name": "headerVersion",
            "type": "string"
          },
          {
            "name": "serveur",
            "type": "string"
          },
          {
            "name": "acteurDeclencheur",
            "type": {
              "fields": [
                {
                  "name": "adresseIP",
                  "type": "string"
                },
                {
                  "name": "idTelematique",
                  "type": "string"
                },
                {
                  "name": "idPersonne",
                  "type": "string"
                }
              ],
              "name": "ActeurDeclencheur",
              "type": "record"
            }
          }
        ],
        "name": "EventHeader",
        "type": "record"
      }
    },
    {
      "name": "EventBusinessContext",
      "type": [
        {
          "doc": "Schéma pour l'événement 00000008H CANALNET",
          "fields": [
            {
              "name": "grilleIdent",
              "type": "string"
            },
            {
              "name": "codeRetourServiceMetier",
              "type": "string"
            },
            {
              "name": "referer",
              "type": [
                "string",
                "null"
              ]
            },
            {
              "name": "browserVersion",
              "type": [
                "string",
                "null"
              ]
            },
            {
              "name": "androidUDID",
              "type": [
                "string",
                "null"
              ]
            },
            {
              "name": "iosIDFA",
              "type": [
                "string",
                "null"
              ]
            },
            {
              "name": "appVersion",
              "type": [
                "string",
                "null"
              ]
            },
            {
              "doc": "Format UUID",
              "name": "idTmx",
              "type": [
                "string",
                "null"
              ]
            }
          ],
          "name": "CanalnetEventBusinessContext",
          "type": "record"
        },
        {
          "doc": "Schéma pour l'événement 00000008C CANALRIB",
          "fields": [
            {
              "name": "numeroCompteBeneficiaire",
              "type": "string"
            },
            {
              "name": "codePaysResidence",
              "type": [
                "string",
                "null"
              ]
            },
            {
              "name": "codePaysResidenceIso",
              "type": [
                "string",
                "null"
              ]
            },
            {
              "name": "adresseBeneficiaire",
              "type": [
                "string",
                "null"
              ]
            },
            {
              "name": "nomCompletBeneficiaire",
              "type": "string"
            },
            {
              "name": "idListeBeneficiaire",
              "type": "string"
            },
            {
              "name": "idBeneficiaire",
              "type": "string"
            },
            {
              "doc": "0: courrier; 1: SMS; 2: cle digitale",
              "name": "modeValidation",
              "type": "int"
            },
            {
              "name": "bicBeneficiaire",
              "type": [
                "string",
                "null"
              ]
            },
            {
              "doc": "Format UUID",
              "name": "idTmx",
              "type": [
                "string",
                "null"
              ]
            }
          ],
          "name": "CanalribEventBusinessContext",
          "type": "record"
        },
        {
          "doc": "Schéma pour l'événement 00000008D CANALMODRIB",
          "fields": [
            {
              "name": "numeroCompteBeneficiaire",
              "type": "string"
            },
            {
              "name": "idListeBeneficiaire",
              "type": "string"
            },
            {
              "name": "idBeneficiaire",
              "type": "string"
            },
            {
              "doc": "0: courrier; 1: SMS; 2: cle digitale",
              "name": "modeValidation",
              "type": "int"
            },
            {
              "name": "bicBeneficiaire",
              "type": [
                "string",
                "null"
              ]
            },
            {
              "doc": "Format UUID",
              "name": "idTmx",
              "type": [
                "string",
                "null"
              ]
            }
          ],
          "name": "CanalmodribEventBusinessContext",
          "type": "record"
        },
        {
          "doc": "Schéma pour l'événement 00000008E CANALVALID",
          "fields": [
            {
              "name": "numeroCompteBeneficiaire",
              "type": "string"
            },
            {
              "doc": "0: correct; 1: errone; 2: bloque",
              "name": "codeRetourServiceMetier",
              "type": "int"
            },
            {
              "doc": "0: courrier; 1: SMS; 2: cle digitale",
              "name": "modeValidation",
              "type": "int"
            },
            {
              "name": "bicBeneficiaire",
              "type": [
                "string",
                "null"
              ]
            },
            {
              "doc": "Format UUID",
              "name": "idTmx",
              "type": [
                "string",
                "null"
              ]
            }
          ],
          "name": "CanalvalidEventBusinessContext",
          "type": "record"
        },
        {
          "doc": "Schéma pour l'événement 00000008F EXEVIRINTER",
          "fields": [
            {
              "name": "numeroCompteDebiteur",
              "type": "string"
            },
            {
              "name": "numeroCompteCrediteur",
              "type": "string"
            },
            {
              "doc": "0 : CAC ; 1 : externe",
              "name": "titularite",
              "type": [
                "int",
                "null"
              ]
            },
            {
              "doc": "Immediat ou Differe",
              "name": "choixImmediateteExecution",
              "type": "string"
            },
            {
              "name": "montant",
              "type": "float"
            },
            {
              "name": "devise",
              "type": "string"
            },
            {
              "name": "libelleMotif",
              "type": "string"
            },
            {
              "doc": "vide si OK",
              "name": "codeRetourServiceMetier",
              "type": [
                "string",
                "null"
              ]
            },
            {
              "doc": "1 : execute ; 2 : rejete)",
              "name": "resultatOperation",
              "type": [
                "int",
                "null"
              ]
            },
            {
              "doc": "Au format Timestamp UNIX",
              "logicalType": "timestamp-millis",
              "name": "dateExecDemandee",
              "type": [
                "long",
                "null"
              ]
            },
            {
              "name": "referenceOperation",
              "type": [
                "string",
                "null"
              ]
            },
            {
              "name": "bicCrediteur",
              "type": [
                "string",
                "null"
              ]
            },
            {
              "doc": "Format UUID",
              "name": "idTmx",
              "type": [
                "string",
                "null"
              ]
            },
            {
              "doc": "0 : le virement n'a pas fait l'objet d'un declenchement d'AF - 1 :le virement a fait l'objet d'un declenchement d'AF",
              "name": "declenchementAF",
              "type": [
                "string",
                "null"
              ]
            },
            {
              "doc": "0 : AF demande KO - 1 : AF demande OK - vide si declenchementAF = 0",
              "name": "validationAF",
              "type": [
                "string",
                "null"
              ]
            },
            {
              "doc": "1 : SMS - 2 : Clé digitale - vide si declenchementAF = 0",
              "name": "modeValidationAF",
              "type": [
                "string",
                "null"
              ]
            }
          ],
          "name": "ExevirinterEventBusinessContext",
          "type": "record"
        },
        {
          "doc": "Schéma pour l'événement 00000008G EXEVIRSEPA",
          "fields": [
            {
              "name": "numeroCompteDebiteur",
              "type": "string"
            },
            {
              "name": "numeroCompteCrediteur",
              "type": "string"
            },
            {
              "doc": "0 : CAC ; 1 : externe)",
              "name": "titularite",
              "type": [
                "int",
                "null"
              ]
            },
            {
              "doc": "Immediat ou Differe",
              "name": "choixImmediateteExecution",
              "type": "string"
            },
            {
              "name": "montant",
              "type": "float"
            },
            {
              "name": "devise",
              "type": "string"
            },
            {
              "name": "libelleMotif",
              "type": "string"
            },
            {
              "doc": "vide si OK",
              "name": "codeRetourServiceMetier",
              "type": [
                "string",
                "null"
              ]
            },
            {
              "doc": "1 : execute ; 2 : rejete",
              "name": "resultatOperation",
              "type": [
                "int",
                "null"
              ]
            },
            {
              "doc": "Au format Timestamp UNIX",
              "logicalType": "timestamp-millis",
              "name": "dateExecDemandee",
              "type": [
                "long",
                "null"
              ]
            },
            {
              "name": "referenceOperation",
              "type": [
                "string",
                "null"
              ]
            },
            {
              "name": "bicCrediteur",
              "type": [
                "string",
                "null"
              ]
            },
            {
              "doc": "Format UUID",
              "name": "idTmx",
              "type": [
                "string",
                "null"
              ]
            },
            {
              "doc": "0 : le virement n'a pas fait l'objet d'un declenchement d'AF - 1 :le virement a fait l'objet d'un declenchement d'AF",
              "name": "declenchementAF",
              "type": [
                "string",
                "null"
              ]
            },
            {
              "doc": "0 : AF demande KO - 1 : AF demande OK - vide si declenchementAF = 0",
              "name": "validationAF",
              "type": [
                "string",
                "null"
              ]
            },
            {
              "doc": "1 : SMS - 2 : Clé digitale - vide si declenchementAF = 0",
              "name": "modeValidationAF",
              "type": [
                "string",
                "null"
              ]
            }
          ],
          "name": "ExevirsepaEventBusinessContext",
          "type": "record"
        },
        {
          "doc": "Schéma pour l'événement 00000008J CANALMOBVAL",
          "fields": [
            {
              "name": "numeroMobile",
              "type": "string"
            },
            {
              "doc": "Nombre d'essai d'activation du numéro de téléphone - 0 si activation OK",
              "name": "nbTentativeActivation",
              "type": "int"
            }
          ],
          "name": "CanalmobvalEventBusinessContext",
          "type": "record"
        },
        {
          "doc": "Schéma pour l'événement 00000008K CANALMOBMOD",
          "fields": [
            {
              "name": "numeroMobile",
              "type": "string"
            },
            {
              "doc": "0:Courrier;2:Cle digitale",
              "name": "modeValidation",
              "type": "int"
            }
          ],
          "name": "CanalmobmodEventBusinessContext",
          "type": "record"
        },
        {
          "doc": "Schéma pour l'événement 00000008L MODIFPLAFONDSCARTES",
          "fields": [
            {
              "name": "numeroContratCarte",
              "type": "string"
            },
            {
              "name": "pourcentageDispoRetrait",
              "type": "int"
            },
            {
              "name": "pourcentageDispoPaiement",
              "type": "int"
            },
            {
              "doc": "Au format Timestamp UNIX",
              "logicalType": "timestamp-millis",
              "name": "dateEcheanceCarte",
              "type": "long"
            },
            {
              "name": "ancienPlafondRetrait",
              "type": "int"
            },
            {
              "name": "nouveauPlafondRetrait",
              "type": "int"
            },
            {
              "name": "ancienPlafondPaiement",
              "type": "int"
            },
            {
              "name": "nouveauPlafondPaiement",
              "type": "int"
            },
            {
              "name": "codeRetourServiceMetier",
              "type": "string"
            }
          ],
          "name": "ModifplafondscartesEventBusinessContext",
          "type": "record"
        },
        {
          "doc": "Schéma pour l'événement 00000008M ENROLSTAF",
          "fields": [
            {
              "doc": "Soft Token Authentication Forte - id d'enrolement auprès de Atos",
              "name": "idSTAF",
              "type": "int"
            },
            {
              "name": "nomPersonnalise",
              "type": "string"
            },
            {
              "name": "terminalConnection",
              "type": [
                "string",
                "null"
              ]
            },
            {
              "name": "terminalOsName",
              "type": "string"
            },
            {
              "name": "terminalOsVersion",
              "type": "string"
            },
            {
              "name": "terminalAppVersion",
              "type": "string"
            },
            {
              "name": "terminalLang",
              "type": "string"
            },
            {
              "name": "deviceModel",
              "type": "string"
            },
            {
              "doc": "1 : Mode OTP SMS ; 2 : Mode OTP vocal",
              "name": "modeValidation",
              "type": "int"
            }
          ],
          "name": "EnrolstafEventBusinessContext",
          "type": "record"
        },
        {
          "doc": "Schéma pour l'événement 00000008N REMPLSTAF",
          "fields": [
            {
              "doc": "Soft Token Authentication Forte - id d'enrolement auprès de Atos",
              "name": "idSTAF",
              "type": "int"
            },
            {
              "name": "idSTAFPrecedent",
              "type": "int"
            },
            {
              "name": "nomPersonnalise",
              "type": "string"
            },
            {
              "name": "terminalConnection",
              "type": [
                "string",
                "null"
              ]
            },
            {
              "name": "terminalOsName",
              "type": "string"
            },
            {
              "name": "terminalOsVersion",
              "type": "string"
            },
            {
              "name": "terminalAppVersion",
              "type": "string"
            },
            {
              "name": "terminalLang",
              "type": "string"
            },
            {
              "name": "deviceModel",
              "type": "string"
            }
          ],
          "name": "RemplstafEventBusinessContext",
          "type": "record"
        },
        {
          "doc": "Schéma pour l'événement 00000008O RETRAITSTAF",
          "fields": [
            {
              "doc": "Soft Token Authentication Forte - id d'enrolement auprès de Atos",
              "name": "idSTAF",
              "type": "int"
            }
          ],
          "name": "RetraitstafEventBusinessContext",
          "type": "record"
        },
        {
          "doc": "Schéma pour l'événement 00000008P ADRFISC",
          "fields": [
            {
              "name": "nouveauCodePostal",
              "type": "string"
            },
            {
              "name": "nouveauCommune",
              "type": "string"
            },
            {
              "name": "nouveauVoie",
              "type": [
                "string",
                "null"
              ]
            },
            {
              "name": "nouveauLieuDit",
              "type": [
                "string",
                "null"
              ]
            },
            {
              "name": "nouveauComplementAdresse",
              "type": [
                "string",
                "null"
              ]
            },
            {
              "name": "ancienCodePostal",
              "type": "string"
            },
            {
              "name": "ancienCommune",
              "type": "string"
            },
            {
              "name": "ancienVoie",
              "type": [
                "string",
                "null"
              ]
            },
            {
              "name": "ancienLieuDit",
              "type": [
                "string",
                "null"
              ]
            },
            {
              "name": "ancienComplementAdresse",
              "type": [
                "string",
                "null"
              ]
            }
          ],
          "name": "AdrfiscEventBusinessContext",
          "type": "record"
        },
        {
          "doc": "Schéma pour l'événement 00000008Q ADRCOUR",
          "fields": [
            {
              "name": "nouveauPays",
              "type": "string"
            },
            {
              "name": "nouveauCodePostal",
              "type": "string"
            },
            {
              "name": "nouveauCommune",
              "type": "string"
            },
            {
              "name": "nouveauVoie",
              "type": [
                "string",
                "null"
              ]
            },
            {
              "name": "nouveauLieuDit",
              "type": [
                "string",
                "null"
              ]
            },
            {
              "name": "nouveauComplementAdresse",
              "type": [
                "string",
                "null"
              ]
            },
            {
              "name": "ancienPays",
              "type": "string"
            },
            {
              "name": "ancienCodePostal",
              "type": "string"
            },
            {
              "name": "ancienCommune",
              "type": "string"
            },
            {
              "name": "ancienVoie",
              "type": [
                "string",
                "null"
              ]
            },
            {
              "name": "ancienLieuDit",
              "type": [
                "string",
                "null"
              ]
            },
            {
              "name": "ancienComplementAdresse",
              "type": [
                "string",
                "null"
              ]
            }
          ],
          "name": "AdrcourEventBusinessContext",
          "type": "record"
        },
        {
          "doc": "Schéma pour l'événement 00000008R TRANSAGENCE",
          "fields": [
            {
              "name": "nouveauCodeAgence",
              "type": "string"
            },
            {
              "name": "ancienCodeAgence",
              "type": "string"
            }
          ],
          "name": "TransagenceEventBusinessContext",
          "type": "record"
        },
        {
          "doc": "Schéma pour l'événement 00000008S IDENTIFICATION-FINGERPRINT",
          "fields": [
            {
              "name": "codeRetourServiceMetier",
              "type": "string"
            },
            {
              "name": "idDevice",
              "type": "string"
            },
            {
              "name": "androidUDID",
              "type": [
                "string",
                "null"
              ]
            },
            {
              "name": "iosIDFA",
              "type": [
                "string",
                "null"
              ]
            },
            {
              "name": "appVersion",
              "type": [
                "string",
                "null"
              ]
            },
            {
              "doc": "Format UUID",
              "name": "idTmx",
              "type": [
                "string",
                "null"
              ]
            }
          ],
          "name": "IdentificationfingerprintEventBusinessContext",
          "type": "record"
        },
        {
          "doc": "Schéma pour l'événement 00000008T ENROLEMENT-FINGERPRINT",
          "fields": [
            {
              "name": "idDevice",
              "type": "string"
            },
            {
              "name": "codeRetourServiceMetier",
              "type": "string"
            }
          ],
          "name": "EnrolementfingerprintEventBusinessContext",
          "type": "record"
        },
        {
          "doc": "Schéma pour l'événement 00000008U DESENROLEMENT-FINGERPRINT",
          "fields": [
            {
              "name": "idDevice",
              "type": "string"
            },
            {
              "name": "codeRetourServiceMetier",
              "type": "string"
            }
          ],
          "name": "DesenrolementfingerpritEventBusinessContext",
          "type": "record"
        },
        {
          "doc": "Schéma pour l'événement 00000009D VALIDATION-MDP-TOUCHID",
          "fields": [
            {
              "name": "idDevice",
              "type": "string"
            },
            {
              "name": "codeRetourServiceMetier",
              "type": "string"
            }
          ],
          "name": "ValidationmdptouchidEventBusinessContext",
          "type": "record"
        },
        {
          "doc": "Schéma pour l'événement 00000009E VPP-CREATION",
          "fields": [
            {
              "name": "numeroCompteDebiteur",
              "type": "string"
            },
            {
              "name": "numeroCompteCrediteur",
              "type": "string"
            },
            {
              "name": "montant",
              "type": "float"
            },
            {
              "doc": "code periodicite : par exemple 1T pour trimestriel ou 1A pour annuel",
              "name": "periodicite",
              "type": "string"
            },
            {
              "doc": "Au format Timestamp UNIX",
              "logicalType": "timestamp-millis",
              "name": "prochaineEcheance",
              "type": "long"
            },
            {
              "doc": "Au format Timestamp UNIX, vide si pas de date de Fin",
              "logicalType": "timestamp-millis",
              "name": "dateFin",
              "type": [
                "long",
                "null"
              ]
            },
            {
              "name": "libelle1",
              "type": [
                "string",
                "null"
              ]
            },
            {
              "name": "libelle2",
              "type": [
                "string",
                "null"
              ]
            },
            {
              "name": "codeRetourServiceMetier",
              "type": [
                "string",
                "null"
              ]
            },
            {
              "doc": "vide si OK",
              "name": "libelleRetourServiceMetier",
              "type": [
                "string",
                "null"
              ]
            },
            {
              "name": "devise",
              "type": "string"
            },
            {
              "name": "refOperation",
              "type": [
                "string",
                "null"
              ]
            }
          ],
          "name": "VppcreationEventBusinessContext",
          "type": "record"
        },
        {
          "doc": "Schéma pour l'événement 00000009F VPP-MODIFICATION",
          "fields": [
            {
              "name": "numeroCompteDebiteur",
              "type": "string"
            },
            {
              "name": "numeroCompteCrediteur",
              "type": "string"
            },
            {
              "name": "montant",
              "type": "float"
            },
            {
              "doc": "code periodicite : par exemple 1T pour trimestriel ou 1A pour annuel",
              "name": "periodicite",
              "type": "string"
            },
            {
              "doc": "Au format Timestamp UNIX",
              "logicalType": "timestamp-millis",
              "name": "prochaineEcheance",
              "type": "long"
            },
            {
              "doc": "Au format Timestamp UNIX, vide si pas de date de Fin",
              "logicalType": "timestamp-millis",
              "name": "dateFin",
              "type": [
                "long",
                "null"
              ]
            },
            {
              "name": "libelle1",
              "type": [
                "string",
                "null"
              ]
            },
            {
              "name": "libelle2",
              "type": [
                "string",
                "null"
              ]
            },
            {
              "name": "codeRetourServiceMetier",
              "type": [
                "string",
                "null"
              ]
            },
            {
              "doc": "vide si OK",
              "name": "libelleRetourServiceMetier",
              "type": [
                "string",
                "null"
              ]
            },
            {
              "name": "devise",
              "type": "string"
            },
            {
              "name": "refOperation",
              "type": [
                "string",
                "null"
              ]
            }
          ],
          "name": "VppmodificationEventBusinessContext",
          "type": "record"
        },
        {
          "doc": "Schéma pour l'événement 00000009G VPP-SUPPRESSION",
          "fields": [
            {
              "name": "numeroCompteDebiteur",
              "type": "string"
            },
            {
              "name": "numeroCompteCrediteur",
              "type": "string"
            },
            {
              "name": "montant",
              "type": "float"
            },
            {
              "doc": "code periodicite : par exemple 1T pour trimestriel ou 1A pour annuel",
              "name": "periodicite",
              "type": "string"
            },
            {
              "doc": "Au format Timestamp UNIX",
              "logicalType": "timestamp-millis",
              "name": "prochaineEcheance",
              "type": "long"
            },
            {
              "doc": "Au format Timestamp UNIX, vide si pas de date de Fin",
              "logicalType": "timestamp-millis",
              "name": "dateFin",
              "type": [
                "long",
                "null"
              ]
            },
            {
              "name": "codeRetourServiceMetier",
              "type": [
                "string",
                "null"
              ]
            },
            {
              "doc": "vide si OK",
              "name": "libelleRetourServiceMetier",
              "type": [
                "string",
                "null"
              ]
            },
            {
              "name": "devise",
              "type": "string"
            }
          ],
          "name": "VppsuppressionEventBusinessContext",
          "type": "record"
        },
        {
          "doc": "Schéma pour l'événement 00000009J RIB-EDITION",
          "fields": [
            {
              "name": "numeroCompte",
              "type": "string"
            }
          ],
          "name": "RibeditionEventBusinessContext",
          "type": "record"
        },
        {
          "doc": "Schéma pour l'événement 00000009L CODESECRET-MODIFICATION",
          "fields": [
            {
              "name": "codeRetourServiceMetier",
              "type": [
                "string",
                "null"
              ]
            }
          ],
          "name": "CodesecretmodificationEventBusinessContext",
          "type": "record"
        },
        {
          "doc": "Schéma pour l'événement 00000009M CHEQ-COMMANDE",
          "fields": [
            {
              "name": "typeEnvoi",
              "type": "string"
            },
            {
              "name": "typeChequier",
              "type": "string"
            },
            {
              "name": "nbChequier",
              "type": "int"
            },
            {
              "name": "adresseLigne1",
              "type": "string"
            },
            {
              "name": "adresseLigne2",
              "type": "string"
            },
            {
              "name": "codeRetourServiceMetier",
              "type": [
                "string",
                "null"
              ]
            }
          ],
          "name": "CheqcommandeEventBusinessContext",
          "type": "record"
        },
        {
          "doc": "Schéma pour l'événement 00000009N OPPOSITION-CHEQUE",
          "fields": [
            {
              "doc": "chèques signés ou chèques vierges",
              "name": "typeOpposition",
              "type": "string"
            },
            {
              "name": "numPremierCheque",
              "type": "string"
            },
            {
              "doc": "vide si chèques signés",
              "name": "numDernierCheque",
              "type": [
                "string",
                "null"
              ]
            },
            {
              "doc": "vide si chèques vierges",
              "name": "montant",
              "type": [
                "float",
                "null"
              ]
            },
            {
              "name": "codeMotif",
              "type": "string"
            },
            {
              "name": "refOpposition",
              "type": "string"
            },
            {
              "name": "codeRetourServiceMetier",
              "type": [
                "string",
                "null"
              ]
            }
          ],
          "name": "OppositionchequeEventBusinessContext",
          "type": "record"
        },
        {
          "doc": "Schéma pour l'événement 00000009O OPPOSITION-CARTE",
          "fields": [
            {
              "name": "numCarteAnonymise",
              "type": "string"
            },
            {
              "name": "numeroCompte",
              "type": "string"
            },
            {
              "name": "codeMotif",
              "type": "string"
            },
            {
              "name": "codeRetourServiceMetier",
              "type": [
                "string",
                "null"
              ]
            }
          ],
          "name": "OppositioncarteEventBusinessContext",
          "type": "record"
        },
        {
          "doc": "Schéma pour l'événement 00000009P OPPOSITION-PRELEVEMENT",
          "fields": [
            {
              "name": "numeroCompteCrypte",
              "type": "string"
            },
            {
              "name": "numEmetteur",
              "type": "string"
            },
            {
              "name": "nomEmetteur",
              "type": [
                "string",
                "null"
              ]
            },
            {
              "name": "referenceContrat",
              "type": "string"
            },
            {
              "name": "echeanceContrat",
              "type": [
                "string",
                "null"
              ]
            },
            {
              "name": "montant",
              "type": [
                "float",
                "null"
              ]
            }
          ],
          "name": "OppositionprelevementEventBusinessContext",
          "type": "record"
        },
        {
          "doc": "Schéma pour l'événement 00000009Q BMM",
          "fields": [],
          "name": "BmmEventBusinessContext",
          "type": "record"
        },
        {
          "doc": "Schéma pour l'événement 00000009H DEMAT",
          "fields": [
            {
              "name": "idDocument",
              "type": "string"
            },
            {
              "name": "codeRetourServiceMetier",
              "type": [
                "string",
                "null"
              ]
            }
          ],
          "name": "DematEventBusinessContext",
          "type": "record"
        },
        {
          "doc": "Schéma pour l'événement 00000009R BOURSE-PASSATION-ORDRE",
          "fields": [
            {
              "name": "referenceOrdre",
              "type": "string"
            },
            {
              "name": "libelleOrdre",
              "type": "string"
            },
            {
              "name": "codeIsin",
              "type": "string"
            },
            {
              "name": "typeValeur",
              "type": "string"
            },
            {
              "name": "typeOrdre",
              "type": "string"
            },
            {
              "name": "place",
              "type": "string"
            },
            {
              "name": "statut",
              "type": "string"
            },
            {
              "name": "sens",
              "type": "string"
            },
            {
              "name": "devise",
              "type": "string"
            },
            {
              "name": "qteInitiale",
              "type": "string"
            },
            {
              "doc": "Au format Timestamp UNIX",
              "logicalType": "timestamp-millis",
              "name": "dateEnregistrement",
              "type": "long"
            },
            {
              "name": "origine",
              "type": "string"
            },
            {
              "name": "validite",
              "type": "string"
            },
            {
              "name": "ibanCompteTitre",
              "type": "string"
            },
            {
              "name": "ibanCompteEspece",
              "type": "string"
            },
            {
              "name": "montantBrutEstime",
              "type": "float"
            },
            {
              "name": "montantNetEstime",
              "type": "float"
            }
          ],
          "name": "BoursepassationordreEventBusinessContext",
          "type": "record"
        },
        {
          "doc": "Schéma pour l'événement 0001D00000000014I PAYLIB-ACTIVATION-PRODUIT ",
          "fields": [
            {
              "name": "idConnexion",
              "type": "string"
            },
            {
              "name": "idContrat",
              "type": "string"
            },
            {
              "doc": "email du compte client",
              "name": "idPaylib",
              "type": "string"
            },
            {
              "doc": "Au format Timestamp UNIX",
              "logicalType": "timestamp-millis",
              "name": "dateActivation",
              "type": "long"
            }
          ],
          "name": "PaylibProduitEventBusinessContext",
          "type": "record"
        },
        {
          "doc": "Schéma pour l'événement 0001D00000000014J PAYLIB-ACTIVATION-VAD",
          "fields": [
            {
              "name": "idContrat",
              "type": "string"
            },
            {
              "name": "idPrestation",
              "type": "string"
            },
            {
              "doc": "Au format Timestamp UNIX",
              "logicalType": "timestamp-millis",
              "name": "dateActivation",
              "type": "long"
            },
            {
              "name": "listeCartes",
              "type": {
                "items": {
                  "fields": [
                    {
                      "name": "numeroCarte",
                      "type": "string"
                    },
                    {
                      "name": "cartePreferentielle",
                      "type": "boolean"
                    },
                    {
                      "doc": "Au format MM/AAAA",
                      "logicalType": "date",
                      "name": "dateFinValidite",
                      "type": "string"
                    }
                  ],
                  "name": "carte",
                  "type": "record"
                },
                "type": "array"
              }
            }
          ],
          "name": "PaylibVADEventBusinessContext",
          "type": "record"
        },
        {
          "doc": "Schéma pour l'événement 0001D00000000014K PAYLIB-ACTIVATION-HCE",
          "fields": [
            {
              "name": "idContrat",
              "type": "string"
            },
            {
              "name": "idPrestation",
              "type": "string"
            },
            {
              "doc": "Au format Timestamp UNIX",
              "logicalType": "timestamp-millis",
              "name": "dateActivation",
              "type": "long"
            },
            {
              "name": "topActivationManuelle",
              "type": "boolean"
            }
          ],
          "name": "PaylibHCEEventBusinessContext",
          "type": "record"
        },
        {
          "doc": "Schéma pour l'événement 0001Q00000000017L EXEVIRSEPAIP",
          "fields": [
            {
              "name": "numeroCompteDebiteur",
              "type": "string"
            },
            {
              "name": "numeroCompteCrediteur",
              "type": "string"
            },
            {
              "name": "bicCrediteur",
              "type": [
                "string",
                "null"
              ]
            },
            {
              "doc": "Immediat ou Differe",
              "name": "choixImmediateteExecution",
              "type": "string"
            },
            {
              "name": "montant",
              "type": "float"
            },
            {
              "name": "devise",
              "type": "string"
            },
            {
              "name": "libelleMotif",
              "type": "string"
            },
            {
              "doc": "1 : execute ; 2 : rejete",
              "name": "resultatOperation",
              "type": [
                "int",
                "null"
              ]
            },
            {
              "doc": "vide si OK",
              "name": "libelleRetourServiceMetier",
              "type": [
                "string",
                "null"
              ]
            },
            {
              "doc": "0 : CAC ; 1 : externe",
              "name": "titularite",
              "type": [
                "int",
                "null"
              ]
            },
            {
              "doc": "Au format Timestamp UNIX",
              "logicalType": "timestamp-millis",
              "name": "dateExecDemandee",
              "type": [
                "long",
                "null"
              ]
            },
            {
              "name": "referenceOperation",
              "type": [
                "string",
                "null"
              ]
            },
            {
              "doc": "Format UUID",
              "name": "idTmx",
              "type": [
                "string",
                "null"
              ]
            }
          ],
          "name": "ExevirsepaipEventBusinessContext",
          "type": "record"
        },
        {
          "doc": "Schéma pour l'événement 0002000000000019X DEMEXEVIRSEPA",
          "fields": [
            {
              "name": "numeroCompteDebiteur",
              "type": "string"
            },
            {
              "name": "numeroCompteCrediteur",
              "type": "string"
            },
            {
              "name": "bicCrediteur",
              "type": [
                "string",
                "null"
              ]
            },
            {
              "name": "nomBeneficiaire",
              "type": [
                "string",
                "null"
              ]
            },
            {
              "doc": "Immediat ou Differe",
              "name": "choixImmediateteExecution",
              "type": "string"
            },
            {
              "doc": "0 : CAC ; 1 : externe",
              "name": "titularite",
              "type": "int"
            },
            {
              "name": "montant",
              "type": "float"
            },
            {
              "name": "devise",
              "type": "string"
            },
            {
              "name": "libelleMotif",
              "type": "string"
            },
            {
              "doc": "Au format Timestamp UNIX",
              "logicalType": "timestamp-millis",
              "name": "dateExecDemandee",
              "type": [
                "long",
                "null"
              ]
            },
            {
              "doc": "Format UUID",
              "name": "idTmx",
              "type": [
                "string",
                "null"
              ]
            }
          ],
          "name": "DemexevirsepaEventBusinessContext",
          "type": "record"
        }
      ]
    }
  ],
  "name": "Event",
  "namespace": "com.bnpparibas.dsibddf.event",
  "type": "record"
}"""



  val schema = new Schema.Parser().parse(user_schema)

  val dataGenerator = new RandomDataGenerator[GenericRecord, GenericRecord]()
  val kafkaProducerProtocol = new KafkaProducerProtocol[GenericRecord, GenericRecord](props, kafkaTopic, dataGenerator)
  val scn = scenario("Kafka Producer Call").exec(KafkaProducerBuilder[GenericRecord, GenericRecord](Some(schema)))

  // constantUsersPerSec(100000) during (1 minute)
  setUp(scn.inject(atOnceUsers(1))).protocols(kafkaProducerProtocol)
}