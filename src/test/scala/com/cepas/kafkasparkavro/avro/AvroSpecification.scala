package com.cepas.kafkasparkavro.avro

import java.io.File

import com.cepas.avro.Person
import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.scalacheck.{Gen, Prop, Properties}
import org.scalacheck.Prop.collect
import org.scalacheck.Prop.forAll
/**
  * Created by scepas on 14/12/15.
  */
object AvroSpecification extends Properties("Avro line") {
    private val separator = "|"
    private val schemaPath = "src/main/avro/person.avsc"
    private val schema: Schema = new Parser().parse(new File(schemaPath))
    private val converter: GenericConverter = new GenericConverter(schema, '|', false)


    property("line with generic API") =
        Prop.forAll (Gen.alphaStr, Gen.posNum[Int], Gen.alphaStr, Gen.alphaStr, Gen.alphaStr, Gen.alphaStr, Gen.alphaStr) {
            (typePerson: String, id: Int, document: String, birthDate: String, firstName: String, middleName: String, surname: String) =>
                val line = typePerson + "|" + id + "|" + document + "|" + birthDate + "|" + firstName + "|" + middleName + "|" + surname
                //collect(line)(!converter.convert(line, 0).isEmpty)
                !converter.convert(line, 0).isEmpty
        }

    property("line with specific API") =
        Prop.forAll (Gen.alphaStr, Gen.posNum[Int], Gen.alphaStr, Gen.alphaStr, Gen.alphaStr, Gen.alphaStr, Gen.alphaStr) {
            (typePerson: String, id: Int, document: String, birthDate: String, firstName: String, middleName: String, surname: String) =>
                val line = typePerson + "|" + id + "|" + document + "|" + birthDate + "|" + firstName + "|" + middleName + "|" + surname
                //collect(line)(!converter.convertToSpecific[Person](line, 0).isEmpty)
                !converter.convertToSpecific[Person](line, 0).isEmpty
        }

}
