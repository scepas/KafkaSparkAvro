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
    private val line2 = "F|1234|01234567A||John||Keynes"
    private val separator = "|"
    private val schemaPath = "src/main/avro/person.avsc"
    private val schema: Schema = new Parser().parse(new File(schemaPath))
    private val converter: GenericConverter = new GenericConverter(schema, '|', false)

    describe("A line of | separated data representing a person")    {
        it("should be convertible to avro") {
            Given("the line")
            And("an avro schema path")
            And("a separator character")
            And("A converter")

            When("I convert the line")
            val person = converter.convert(line, 0)
            Then("The person type should be F")
            person.get.get("type") should be("F")
            And("The id should be 1234")
            person.get.get("id") should be(1234)
        }
        it ("should handle null values")    {
            Given("the line with empty fields")
            And("an avro schema path")
            And("a separator character")
            And("a converter")
            When("I convert the line")
            val person2 = converter.convert(line2, 0)
            Then("The person type should be F")
            person2.get.get("type") should be("F")
            And("The id should be 1234")
            person2.get.get("id") should be(1234)
        }

        it ("should work with specific types")    {
            import com.cepas.avro.Person
            Given("the line with empty fields")
            And("an avro schema path")
            And("a separator character")
            And("a converter")
            When("I convert the line")
            val person = converter.convertToSpecific[Person](line, 0)
            Then("The person type should be F")
            person.get.getType should be("F")
            And("The id should be 1234")
            person.get.getId should be(1234)
        }
    }
}
