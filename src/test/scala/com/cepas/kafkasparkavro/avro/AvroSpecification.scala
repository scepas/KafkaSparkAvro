package com.cepas.kafkasparkavro.avro

import java.io.File

import com.cepas.avro.Person
import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.scalacheck.{Gen, Prop, Properties}
import org.scalacheck.Prop.collect
/**
  * Created by scepas on 14/12/15.
  */
object AvroSpecification extends Properties("Avro line") {
    private val separator = "|"
    private val schemaPath = "src/main/avro/person.avsc"
    private val schema: Schema = new Parser().parse(new File(schemaPath))
    private val converter: GenericConverter = new GenericConverter(schema, '|', false)


    property("line with generic API") =
        Prop.forAll (Gen.alphaStr, Gen.oneOf(Gen.posNum[Int].sample, None), Gen.alphaStr, Gen.alphaStr, Gen.alphaStr,
                Gen.oneOf(Gen.alphaStr.sample, None), Gen.alphaStr, Gen.posNum[Double]) {
            (typePerson: String, id: Option[Int], document: String, birthDate: String, firstName: String,
                middleName: Option[String], surname: String, savings: Double) =>
                val line = typePerson + "|" + id.getOrElse("") + "|" + document + "|" + birthDate + "|" + firstName +
                    "|" + middleName.getOrElse("") + "|" + surname + "|" + savings
                collect(converter.convert(line, 0).get.get("savings"))(!converter.convert(line, 0).isEmpty)
                //!converter.convert(line, 0).isEmpty
        }

    property("line with specific API") =
        Prop.forAll (Gen.alphaStr, Gen.oneOf(Gen.posNum[Int].sample, None), Gen.alphaStr, Gen.alphaStr, Gen.alphaStr,
            Gen.oneOf(Gen.alphaStr.sample, None), Gen.alphaStr) {
            (typePerson: String, id: Option[Int], document: String, birthDate: String, firstName: String,
             middleName: Option[String], surname: String) =>
                val line = typePerson + "|" + id.getOrElse("") + "|" + document + "|" + birthDate + "|" + firstName +
                    "|" + middleName.getOrElse("") + "|" + surname
                //collect(line)(!converter.convertToSpecific[Person](line, 0).isEmpty)
                !converter.convertToSpecific[Person](line, 0).isEmpty
        }

}
