package com.cepas.kafkasparkavro.avro

import java.io.File
import com.cepas.avro.Person
import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.GenericRecord

/**
  * Created by scepas on 14/12/15.
  */
object AvroPerformanceTest extends App {


    private val line = "F|1234|01234567A|1980-01-01|John|Maynard|Keynes"
    private val line2 = "F|1234|01234567A||John||Keynes"

    private val separator = "|"
    private val schemaPath = "src/main/avro/person.avsc"
    private val schema: Schema = new Parser().parse(new File(schemaPath))
    private val converter: GenericConverter = new GenericConverter(schema, '|', false)
    private val numLines = 10000

    def lines(num: Int) = {
        (1 to num).toList.map(i => if (i % 2 == 0) line else line2)
    }

    def measure(name: String, f: => Unit) = {
        val t0 = System.nanoTime()
        f
        val t1 = System.nanoTime()
        println(s"Elapsed time executing ${name}: " + (t1 - t0) / 1e6 + "ms")
    }

    val specific = {
        lines(numLines).foreach { l =>
            val p = converter.convertToSpecific[Person](l, 0)
            assert(p.get.getDocument == "01234567A" )
        }
    }

    def generic: Unit = {
        lines(numLines).foreach { l =>
            val p = converter.convert(l, 0)
            assert(p.get.get("document") == "01234567A" )
            //println(p.get)
        }
    }



    measure("Generic", generic)
    measure("Specific", specific)

}
