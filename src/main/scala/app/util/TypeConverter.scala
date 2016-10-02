package app.util

import java.util.UUID

import org.apache.spark.sql.Row

object TypeConverter {
  private def getValue = (row:Row, field: String, typeString: String) => {
    typeString match {
      case "boolean" => row.getAs[Boolean](field)
      case "long" => row.getAs[Long](field)
      case "byte" => row.getAs[Byte](field)
      case "date" => row.getAs[Long](field)
      case "decimal" => row.getAs[java.math.BigDecimal](field)
      case "double" => row.getAs[Double](field)
      case "float" => row.getAs[Float](field)
      case "integer" => row.getAs[Int](field)
      case "short" => row.getAs[Short](field)
      case "string" => row.getAs[String](field)
      case "timestamp" => row.getAs[Long](field)
      case "uuid" => UUID.fromString(row.getAs[String](field))
    }
  }
  def fieldToString = (row: Row, field: String, typeString: String, uuidFields: Seq[String]) => {
    val value = getValue(row, field, if (uuidFields.contains(field)) {"uuid"} else {typeString})
    (field, value)
  }
}
