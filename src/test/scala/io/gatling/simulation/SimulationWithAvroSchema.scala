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
  val user_schema =
    s"""
   |{
   |  "namespace" : "io.gatling",
   |  "type" : "record",
   |  "name" : "EnrichedEvent",
   |  "doc" : "fields[1] représente le header de l'evenement, fields[2] représente la partie businessContext, fields[3] représente la partie enrichie",
   |  "fields" : [
   |    {"name" : "EventHeader",
   |      "type" : {
   |        "type" : "record",
   |        "name" : "EventHeader",
   |        "fields" : [
   |          {"name": "eventId", "type": "string"},
   |          {"name": "dateTimeRef",  "type": "long", "logicalType" : "timestamp-millis", "doc" : "Au format Timestamp UNIX"},
   |          {"name": "nomenclatureEv",  "type": "string", "doc" : "Code Nomenclature de l'événement"},
   |          {"name": "canal",  "type": "int"},
   |          {"name": "media",  "type": "int"},
   |          {"name": "schemaVersion",  "type": "string"},
   |          {"name": "headerVersion",  "type": "string"},
   |          {"name": "serveur",  "type": "string"},
   |          {"name" : "acteurDeclencheur",
   |            "type" : {
   |              "type" : "record",
   |              "name" : "ActeurDeclencheur",
   |              "fields" : [
   |                {"name": "adresseIP",  "type": "string"},
   |                {"name": "idTelematique",  "type": "string"},
   |                {"name": "idPersonne",  "type": "string"}
   |              ]
   |            }
   |          }
   |        ]
   |      }
   |    },
   |    {"name" : "EventBusinessContext",
   |      "type" : [
   |        {
   |          "type" : "record",
   |          "name" : "CanalnetEventBusinessContext",
   |          "doc" : "Schéma pour l'événement 00000008H CANALNET",
   |          "fields" : [
   |            {"name": "grilleIdent", "type": "string"},
   |            {"name": "codeRetourServiceMetier", "type": "string"},
   |            {"name": "referer",  "type": ["string","null"]},
   |            {"name": "browserVersion",  "type": ["string","null"]},
   |            {"name": "androidUDID",  "type": ["string","null"]},
   |            {"name": "iosIDFA",  "type": ["string","null"]},
   |            {"name": "appVersion",  "type": ["string","null"]},
   |            {"name": "idTmx",  "type": ["string","null"], "doc" : "Format UUID"}
   |          ]
   |        },
   |        {
   |          "type" : "record",
   |          "name" : "CanalribEventBusinessContext",
   |          "doc" : "Schéma pour l'événement 00000008C CANALRIB",
   |          "fields" : [
   |            {"name": "numeroCompteBeneficiaire", "type": "string"},
   |            {"name": "codePaysResidence",  "type": ["string","null"]},
   |            {"name": "codePaysResidenceIso",  "type": ["string","null"]},
   |            {"name": "adresseBeneficiaire",  "type": ["string","null"]},
   |            {"name": "nomCompletBeneficiaire", "type": "string"},
   |            {"name": "idListeBeneficiaire", "type": "string"},
   |            {"name": "idBeneficiaire", "type": "string"},
   |            {"name": "modeValidation", "type": "int",  "doc" : "0: courrier; 1: SMS; 2: cle digitale"},
   |            {"name": "bicBeneficiaire",  "type": ["string","null"]},
   |            {"name": "idTmx",  "type": ["string","null"], "doc" : "Format UUID"}
   |          ]
   |        }
   |      ]
   |    }
   |  ]
   |}
   |""".stripMargin



  val schema = new Schema.Parser().parse(user_schema)

  val dataGenerator = new RandomDataGenerator[GenericRecord, GenericRecord]()
  val kafkaProducerProtocol = new KafkaProducerProtocol[GenericRecord, GenericRecord](props, kafkaTopic, dataGenerator)
  val scn = scenario("Kafka Producer Call").exec(KafkaProducerBuilder[GenericRecord, GenericRecord](Some(schema)))

  // constantUsersPerSec(100000) during (1 minute)
  setUp(scn.inject(atOnceUsers(1))).protocols(kafkaProducerProtocol)
}