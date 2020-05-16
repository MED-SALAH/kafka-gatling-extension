package io.gatling.kafka

import io.gatling.{ActeurDeclencheur, EventHeader}
import io.gatling.core.Predef.Session
import io.gatling.core.protocol.Protocol
import io.gatling.data.generator.RandomDataGenerator
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}



class KafkaProducerProtocol[K: Manifest, V: Manifest](props: java.util.HashMap[String, Object],
                                  topics: String,
                                  dataGenerator: RandomDataGenerator[K, V] = null)
  extends Protocol {
  private final val kafkaProducer = new KafkaProducer[String, Record](props)

  private var key: K = _
  private var value: V = _




  def generateRecord(schema:Schema): Record = {
    val eventRecord: Record = new GenericData.Record(schema)
    val eventHeader: Record = new GenericData.Record(schema)

    val acteurDeclencheur: Record = new Record(schema)
    val canalnetEventBusinessContext: Record = new Record(schema)


    eventHeader.put("eventid","time")
    eventRecord.put(eventHeader, )

    acteurDeclencheur.put(2,"acteur declancheur")
    canalnetEventBusinessContext.put(3,"canal net")

    eventRecord.put("dd","sds")
    eventRecord
  }

  def call(session: Session,
           schema: Option[Schema] = None): Unit = {
    val attributes = session.attributes

    val avroRecord = generateRecord(schema.get)

    val record = new ProducerRecord(topics, "key", avroRecord)
    kafkaProducer.send(record)
  }

  private def createRecordForAvroSchema(attributes: Map[String, Any]): GenericRecord = {
    if (attributes.isEmpty) {
      throw new RuntimeException("attributes is empty. Cannot generate record")
    }

    val length = attributes.size

    var schemaBuilder = SchemaBuilder.record("testdata")
        .namespace("org.apache.avro").fields()

    for ((key, value) <- attributes) {
      schemaBuilder = schemaBuilder
        .name(key).`type`().nullable().stringType().noDefault()
    }
    val schema = schemaBuilder.endRecord()

    val avroRecord = new Record(schema)

    var count = 0
    for ((key, value) <- attributes) {
      avroRecord.put(count, value.toString)
      count += 1
    }
    avroRecord
  }
}