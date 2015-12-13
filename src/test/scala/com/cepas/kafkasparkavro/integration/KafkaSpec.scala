package com.cepas.kafkasparkavro.integration

import java.io.{ByteArrayOutputStream, File}
import java.util.Properties

import com.cepas.avro.Person
import com.cepas.kafkasparkavro.avro.GenericConverter
import com.cepas.kafkasparkavro.kafka.ConsumerTaskContext
import com.cepas.kafkasparkavro.logging.LazyLogging
import com.cepas.kafkasparkavro.testing.{EmbeddedKafkaZooKeeperCluster, KafkaTopic}
import kafka.message.MessageAndMetadata
import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.apache.avro.specific.{SpecificData, SpecificDatumWriter, SpecificDatumReader}
import org.scalatest.{BeforeAndAfterEach, FunSpec, GivenWhenThen, Matchers}

import scala.collection.mutable
import scala.concurrent.duration._
import scala.language.reflectiveCalls

/**
  * Created by scepas on 13/12/15.
  */
class KafkaSpec extends FunSpec with Matchers with BeforeAndAfterEach with GivenWhenThen with LazyLogging {
    private val topic = KafkaTopic("testing")
    private val kafkaZkCluster = new EmbeddedKafkaZooKeeperCluster(topics = Seq(topic))
    val schemaPath = "src/main/avro/person.avsc"
    private val schema: Schema = new Parser().parse(new File(schemaPath))

    override def beforeEach() {
        kafkaZkCluster.start()
    }

    override def afterEach() {
        kafkaZkCluster.stop()
    }

    val fixture = {
        val separator = "|"

        val converter: GenericConverter = new GenericConverter(schema, '|', false)
        new {
            val l1 = "F|1234|01234567A|1980-01-01|John|Maynard|Keynes"
            val l2 = "F|4567|01234567B||John||Smith"
            val l3 = "F|89|01234567C|1970-12-31|Rafa||Nadal"
            val p1 = converter.convertToSpecific[Person](l1, 0)
            val p2 = converter.convertToSpecific[Person](l2, 0)
            val p3 = converter.convertToSpecific[Person](l3, 0)

            val messages = Seq(p1, p2, p3).filter(!_.isEmpty).map(_.get)
        }
    }



    describe("Kafka") {

        it("should synchronously send and receive a Tweet in Avro format", IntegrationTest) {
            Given("a ZooKeeper instance")
            And("a Kafka broker instance")
            And("some persons")
            val persons = fixture.messages
            And("a single-threaded Kafka consumer group")
            // The Kafka consumer group must be running before the first messages are being sent to the topic.
            val actualPersons = new mutable.SynchronizedQueue[GenericRecord]
            def consume(m: MessageAndMetadata[Array[Byte], Array[Byte]], c: ConsumerTaskContext): Unit = {
                val person: Person = new SpecificDatumReader[Person](classOf[Person]).read(
                    null, DecoderFactory.get.binaryDecoder(m.message, null));
                logger.info(s"Consumer thread ${c.threadId}: received person $person from ${m.topic}:${m.partition}:${m.offset}")
                actualPersons += person
            }
            kafkaZkCluster.createAndStartConsumer(topic.name, consume)

            When("I start a synchronous Kafka producer that sends the tweets in Avro binary format")
            val producerApp = {
                val c = new Properties
                c.put("producer.type", "sync")
                c.put("client.id", "test-sync-producer")
                c.put("request.required.acks", "1")
                kafkaZkCluster.createProducer(topic.name, c).get
            }

            val out = new ByteArrayOutputStream
            val enc = EncoderFactory.get.binaryEncoder(out, null)
            val writer = new GenericDatumWriter[GenericRecord](schema)
            persons foreach { person =>
                val bytes = {
                    out.reset()
                    writer.write(person, enc)
                    enc.flush()
                    out.close()
                    out.toByteArray
                }
                logger.info(s"Synchronously sending Person $person to topic ${producerApp.defaultTopic}")
                producerApp.send(bytes)
            }

            Then("the consumer app should receive the persons")
            val waitForConsumerToReadOutput = 300.millis
            logger.debug(s"Waiting $waitForConsumerToReadOutput for Kafka consumer threads to read messages")
            Thread.sleep(waitForConsumerToReadOutput.toMillis)
            logger.debug("Finished waiting for Kafka consumer threads to read messages")
            actualPersons.toSeq should be(persons)

        }
    }
}