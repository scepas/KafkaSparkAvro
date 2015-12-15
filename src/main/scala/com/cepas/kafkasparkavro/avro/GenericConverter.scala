package com.cepas.kafkasparkavro.avro

import java.math.MathContext

import org.apache.avro.Schema
import org.apache.avro.Schema.Type
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.specific.SpecificData

import scala.collection.JavaConverters._

/**
  * Created by scepas on 08/12/2015.
  */
case class GenericConverter(schema: Schema, separator: Char, dropDelims: Boolean = false) {

    lazy val regex: String = createRegex(separator, dropDelims)

    /**
      * Converts a line of text into an avro record
      *
      * @param line: a text line with delimited values
      * @param firstField: position (zero-based) of the first field in the line from which to perform the conversion
      * @return Option of parsed avro generic record
      */
    def convert(line: String, firstField: Int = 0): Option[GenericRecord] = {
        val values = line.split(regex).toList.drop(firstField)
        val record: GenericRecord = new GenericData.Record(schema)
        val fields = schema.getFields.asScala
        try {
            (fields, values).zipped.toList.foreach {
                (pair) => {
                    //skip if value is ""
                    if (!pair._2.isEmpty)
                        record.put(pair._1.name, castField(pair._1, pair._2))
                }
            }
            Some(record)
        }
        catch {
            case ex: Exception =>
                println("Error parsing line: " + line.toString)
                println()
                ex.printStackTrace()
                None
        }
    }

    /**
      * Split line by a separator, optionally removing quotes
      * @param separator: separator character used to split the line
      * @param dropQuotes: remove double quotes in fields
      * @return
      */
    def createRegex(separator: Char, dropQuotes: Boolean = false): String  = {
        val specialCharacters: List[Char] = """~`!@#\$^%&*()_-\+={}[]|;:"'<,>.?/""".toList
        val sep: String = (if (specialCharacters.contains(separator)) "\\" else "") + separator
        val regex: String = {
            if (!dropQuotes) sep
            else """\"{0,1}""" + sep + """\"{0,1}|\"$\"{0,1}"""
        }
        regex
    }

    /**
      * Cast a string value to a type defined in an avro field
      * @param field: avro field
      * @param value: string value to cast
      * @return value casted to the proper type
      */
    //TODO: consider more data types
    def castField(field: Schema.Field, value: String): Any = {
        if (field.getProp("logicalType") == "decimal")  {
            castDecimal(field, value)
        }
        else {
            val schema = field.schema()
            val types = schema.getType match {
                case Type.UNION => field.schema().getTypes.asScala.toList
                case _ => List(schema)
            }
            @scala.annotation.tailrec
            def getTypeConversor(types: List[Schema]): (String => Any) = types.head.getType match {
                case Type.INT => s => s.toInt
                case Type.FLOAT => s => s.toFloat
                case Type.UNION => getTypeConversor(types.tail)
                case _ => s => s //otherwise, return value in original type
            }
            getTypeConversor(types)(value)
        }
    }

    private def castDecimal(field: Schema.Field, value: String): Any = {
        val defaultPrecision = 10
        val precision = field.getJsonProp("precision").asInt(defaultPrecision)
        new java.math.BigDecimal(value, new java.math.MathContext(precision))
    }

    private def toSpecific[T](rec: GenericRecord): T = {
        SpecificData.get().deepCopy(rec.getSchema, rec).asInstanceOf[T]
    }

    def convertToSpecific[T](line: String, firstField: Int = 0): Option[T] =    {
        val rec = convert(line, firstField)
        rec match {
            case Some(r) => Some(toSpecific(r))
            case _ => None
        }
    }
}

