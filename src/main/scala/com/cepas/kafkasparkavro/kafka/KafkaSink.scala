package com.cepas.kafkasparkavro.kafka

import java.util.Properties

/**
  * Created by scepas on 3/12/15.
  */
class KafkaSink(createProducer: () => KafkaProducerApp) extends Serializable {
    lazy val p = createProducer()

    def send(key: Array[Byte], value: Array[Byte], topic: Option[String] = None): Unit = {
        p.send(key, value, topic)
    }
}
object KafkaSink {
    def apply(brokerList: String, config: Map[String, String], defaultTopic: Option[String]): KafkaSink = {
        val f = () => {
            val prop = new Properties
            config.foreach(entry => prop.setProperty(entry._1, entry._2))
            val p = new KafkaProducerApp(brokerList, prop, defaultTopic)
            sys.addShutdownHook {
                p.shutdown()
            }
            p
        }
        new KafkaSink(f)
    }
}
