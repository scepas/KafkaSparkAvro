package com.cepas.kafkasparkavro.spark

import java.io.{ByteArrayOutputStream, File}
import java.util.Properties

import com.cepas.kafkasparkavro.integration.IntegrationTest
import com.cepas.kafkasparkavro.kafka.{KafkaProducerApp, ConsumerTaskContext}
import com.cepas.kafkasparkavro.logging.LazyLogging
import com.cepas.kafkasparkavro.testing.{EmbeddedKafkaZooKeeperCluster, KafkaTopic}
import com.cepas.kafkasparkavro.avro.GenericConverter
import kafka.message.MessageAndMetadata
import kafka.serializer.DefaultDecoder
import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{EncoderFactory, DecoderFactory}
import org.apache.avro.specific.{SpecificDatumWriter, SpecificDatumReader}
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.scalatest.{GivenWhenThen, BeforeAndAfterEach, Matchers, FeatureSpec}
import scala.collection.mutable
import com.cepas.avro.Person
import scala.concurrent.duration._
import scala.language.reflectiveCalls

/**
  * Created by scepas on 9/12/15.
  */
class KafkaSparkSpec extends FeatureSpec with Matchers with BeforeAndAfterEach with GivenWhenThen with LazyLogging {
    private val inputTopic = KafkaTopic("testing-input")
    private val outputTopic = KafkaTopic("testing-output")

    private val kafkaZkCluster = new EmbeddedKafkaZooKeeperCluster(topics = Seq(inputTopic, outputTopic))
    private val sparkCheckpointRootDir = {
        val r = (new scala.util.Random).nextInt()
        val path = Seq(System.getProperty("java.io.tmpdir"), "spark-test-checkpoint-" + r).mkString(File.separator)
        new File(path)
    }

    val schemaPath = "src/main/avro/person.avsc"
    private val schema: Schema = new Parser().parse(new File(schemaPath))

    private var ssc: StreamingContext = _

    override def beforeEach() {
        kafkaZkCluster.start()
        prepareSparkStreaming()
    }

    private def prepareSparkStreaming(): Unit = {
        val sparkConf = {
            val conf = new SparkConf().setAppName("kafka-spark-avro")
            // Make sure you give enough cores to your Spark Streaming application.  You need cores for running "receivers"
            // and for powering the actual the processing.  In Spark Streaming, each receiver is responsible for 1 input
            // DStream, and each receiver occupies 1 core.  If all your cores are occupied by receivers then no data will be
            // processed!
            // https://spark.apache.org/docs/1.1.0/streaming-programming-guide.html
            val cores = inputTopic.partitions + 1
            conf.setMaster(s"local[$cores]")
            // Use Kryo to speed up serialization, recommended as default setup for Spark Streaming
            // http://spark.apache.org/docs/1.1.0/tuning.html#data-serialization
            conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            //conf.set("spark.kryo.registrator", classOf[KafkaSparkStreamingRegistrator].getName)
            // Enable experimental sort-based shuffle manager that is more memory-efficient in environments with small
            // executors, such as YARN.  Will most likely become the default in future Spark versions.
            // https://spark.apache.org/docs/1.1.0/configuration.html#shuffle-behavior
            conf.set("spark.shuffle.manager", "SORT")
            // Force RDDs generated and persisted by Spark Streaming to be automatically unpersisted from Spark's memory.
            // The raw input data received by Spark Streaming is also automatically cleared.  (Setting this to false will
            // allow the raw data and persisted RDDs to be accessible outside the streaming application as they will not be
            // cleared automatically.  But it comes at the cost of higher memory usage in Spark.)
            // http://spark.apache.org/docs/1.1.0/configuration.html#spark-streaming
            conf.set("spark.streaming.unpersist", "true")
            conf
        }
        val batchInterval = Seconds(20)
        ssc = new StreamingContext(sparkConf, batchInterval)


        ssc.checkpoint(sparkCheckpointRootDir.toString)
    }

    private def terminateSparkStreaming() {
        ssc.stop(stopSparkContext = true, stopGracefully = true)
        ssc = null
        FileUtils.deleteQuietly(sparkCheckpointRootDir)
    }

    override def afterEach() {
        kafkaZkCluster.stop()
        terminateSparkStreaming()
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

    info("As a user of Spark Streaming")
    info("I want to read Avro-encoded data from Kafka")
    info("so that I can quickly build Kafka<->Spark Streaming data flows")

    feature("Basic functionality") {
        scenario("User creates a Spark Streaming job that reads from and writes to Kafka", IntegrationTest) {
            Given("a ZooKeeper instance")
            And("a Kafka broker instance")
            And("some persons")
            val persons = fixture.messages

            And(s"a synchronous Kafka producer app that writes to the topic $inputTopic")
            val producerApp = {
                val c = new Properties
                c.put("producer.type", "sync")
                c.put("client.id", "kafka-spark-avro-test-sync-producer")
                c.put("request.required.acks", "1")
                kafkaZkCluster.createProducer(inputTopic.name, c).get
            }
            And(s"a single-threaded Kafka consumer app that reads from topic $outputTopic and Avro-decodes the incoming data")
            val actualPersons = new mutable.SynchronizedQueue[GenericRecord]
            def consume(m: MessageAndMetadata[Array[Byte], Array[Byte]], c: ConsumerTaskContext): Unit = {
                val person: Person = new SpecificDatumReader[Person](classOf[Person]).read(
                    null, DecoderFactory.get.binaryDecoder(m.message, null));
                logger.info(s"Consumer thread ${c.threadId}: received person $person from ${m.topic}:${m.partition}:${m.offset}")
                actualPersons += person
            }
            kafkaZkCluster.createAndStartConsumer(outputTopic.name, consume)
            val waitForConsumerStartup = 1000.millis
            logger.debug(s"Waiting $waitForConsumerStartup for the Kafka consumer to start up")
            Thread.sleep(waitForConsumerStartup.toMillis)

            When("I Avro-encode the persons and use the Kafka producer app to send them to Kafka")

            val out = new ByteArrayOutputStream
            val enc = EncoderFactory.get.binaryEncoder(out, null)
            val writer = new GenericDatumWriter[GenericRecord](schema)
            persons foreach {
                case person =>
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

            And(s"I run a streaming job that reads persons from topic $inputTopic and writes them as-is to topic $outputTopic")

            val zookeeperConnect = ssc.sparkContext.broadcast(kafkaZkCluster.zookeeper.connectString)
            val brokerList = ssc.sparkContext.broadcast(kafkaZkCluster.kafka.brokerList)

            val sparkStreamingConsumerGroup = "spark-avro-consumer-group"
            val kafkaParams = Map[String, String](
                //"zookeeper.connect" -> kafkaZkCluster.zookeeper.connectString,
                //"group.id" -> sparkStreamingConsumerGroup,
                // CAUTION: Spark's use of auto.offset.reset is DIFFERENT from Kafka's behavior!
                // https://issues.apache.org/jira/browse/SPARK-2383
                "auto.offset.reset" -> "smallest", // must be compatible with when/how we are writing the input data to Kafka
                "zookeeper.connection.timeout.ms" -> "5000",
                "metadata.broker.list" -> kafkaZkCluster.kafka.brokerList
            )

            val kafkaStream0 = KafkaUtils.createDirectStream[Array[Byte], Array[Byte], DefaultDecoder, DefaultDecoder](
                ssc,
                kafkaParams,
                Set(inputTopic.name)
            )

            val kafkaStream = kafkaStream0.map(_._2)

            printValues(kafkaStream, ssc)

            /**
              * Simple Print function, for printing all elements of RDD
              */
            def printValues(stream:DStream[Array[Byte]],streamCtx: StreamingContext){
                stream.foreachRDD(foreachFunc)
                def foreachFunc = (rdd: RDD[Array[Byte]]) => {
                    val array = rdd.collect()
                    println("---------Start Printing Results----------")
                    for(res<-array){
                        println(res)
                    }
                    println("---------Finished Printing Results----------")
                }
            }


            /*
            val kafkaStream = {
                val sparkStreamingConsumerGroup = "spark-avro-consumer-group"
                val kafkaParams = Map[String, String](
                    "zookeeper.connect" -> kafkaZkCluster.zookeeper.connectString,
                    "group.id" -> sparkStreamingConsumerGroup,
                    // CAUTION: Spark's use of auto.offset.reset is DIFFERENT from Kafka's behavior!
                    // https://issues.apache.org/jira/browse/SPARK-2383
                    "auto.offset.reset" -> "smallest", // must be compatible with when/how we are writing the input data to Kafka
                    "zookeeper.connection.timeout.ms" -> "5000",
                    "metadata.broker.list" -> kafkaZkCluster.kafka.brokerList
                )
                val streams = (1 to inputTopic.partitions) map { _ =>
                    KafkaUtils.createStream[Array[Byte], Array[Byte], DefaultDecoder, DefaultDecoder](
                        ssc,
                        kafkaParams,
                        Map(inputTopic.name -> 1),
                        storageLevel = StorageLevel.MEMORY_ONLY_SER // or: StorageLevel.MEMORY_AND_DISK_SER
                    ).map(_._2)
                }
                val unifiedStream = ssc.union(streams)
                val sparkProcessingParallelism = 1
                unifiedStream.repartition(sparkProcessingParallelism)
            }
*/
            //val numInputMessages = ssc.sparkContext.accumulator(0L, "Kafka messages consumed")
            //val numOutputMessages = ssc.sparkContext.accumulator(0L, "Kafka messages produced")
            //info(brokerList.value.toString)
            //info(zookeeperConnect.value.toString)
            //info("Aquí Kafka Stream:")

            //kafkaStream.print()
            //val numEvents = kafkaStream.countByWindow(Seconds(40), Seconds(40))
            //numEvents.print()
            //numEvents.foreachRDD(rdd => print(rdd.first()))
            /*
            kafkaStream.foreachRDD(rdd => {

                rdd.foreachPartition(partitionOfRecords => {
                    import java.io._
                    val pw = new PrintWriter(new File("/tmp/logSanti.txt" ))
                    pw.write("para cada partición...")
                    pw.write(partitionOfRecords.take(3).toString())
                    pw.close

                    //val loggerF = org.slf4j.LoggerFactory.getLogger(getClass.getName)
                    //loggerF.info("para cada partición...")
                    //loggerF.info(partitionOfRecords.take(3).toString())
                })
            })
            */
/*
            kafkaStream.foreachRDD(rdd => {

                rdd.foreachPartition(partitionOfRecords => {
                    val config = {
                        val c = new Properties
                        c.put("metadata.broker.list", brokerList.value)
                        c.put("zookeeper.connect", zookeeperConnect.value)
                        c.put("auto.offset.reset", "smallest" )
                        c
                    }
                    val producerApp = new KafkaProducerApp(zookeeperConnect.value, config, defaultTopic = Option("testing-output"))
                    partitionOfRecords.foreach { rec =>
                        //val bytes = converter.value.apply(tweet)
                        producerApp.send(rec)
                        numOutputMessages += 1
                    }
                })
            })
*/
            //seguir por aquí. Aunque no se cumple el test parece que no peta...

            ssc.start()
            ssc.awaitTerminationOrTimeout(2)

            Then("the Spark Streaming job should consume all persons from Kafka")
            //numInputMessages.value should be(persons.size)
            And("the job should write back all persons to Kafka")
            //numOutputMessages.value should be(persons.size)
            And("the Kafka consumer app should receive the original persons from the Spark Streaming job")

            val waitToReadSparkOutput = 1000.millis
            logger.debug(s"Waiting $waitToReadSparkOutput for Kafka consumer to read Spark Streaming output from Kafka")
            Thread.sleep(waitToReadSparkOutput.toMillis)
            actualPersons.toSeq should be(persons.toSeq)


            // Cleanup
            producerApp.shutdown()
        }
    }

}
