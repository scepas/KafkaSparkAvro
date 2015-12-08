package com.cepas.kafkasparkavro.avro

import com.cepas.kafkasparkavro.logging.LazyLogging
import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.scalatest.{GivenWhenThen, Matchers, FunSpec}
import java.io.File

/**
  * Created by scepas on 8/12/15.
  */
class GenericConverterSpec extends FunSpec with Matchers with GivenWhenThen with LazyLogging {
    private val line = "F|1234|01234567A|1980-01-01|John|Maynard|Keynes"
    private val separator = "|"
    private val schemaPath = "src/main/avro/person.avsc"
    val schema: Schema = new Parser().parse(new File(schemaPath))

    describe("A line of | separated data representing a person")    {
        it("should be convertible to avro") {
            Given("the line")
            And("an avro schema path")
            And("a separator character")
            And("A converter")
            val converter: GenericConverter = new GenericConverter(schema, '|', false)
            When("I convert the line")
            val person = converter.convert(line, 0)
            Then("The person type should be F")
            person.get.get("type") should be("F")
            And("The id should be 1234")
            person.get.get("id") should be(1234)
        }
    }
}
