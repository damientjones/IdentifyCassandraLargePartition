package org.apache.spark.sql.cassandra

import org.apache.spark.sql.sources.{LogicalRelation, ResolvedDataSource}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, DataFrameReader}

class CustomDataframeReader(sqlContext: SQLContext) extends DataFrameReader(sqlContext) {
  override def load(): DataFrame = {
    val resolved = ResolvedDataSource(
      sqlContext,
      userSpecifiedSchema = userSpecifiedSchema,
      partitionColumns = partitionColumns,
      provider = source,
      options = extraOptions.toMap)
    DataFrame(sqlContext, LogicalRelation(resolved.relation))
  }

  override def format(source: String): DataFrameReader = {
    this.source = source
    this
  }

  override def schema(schema: StructType): DataFrameReader = {
    this.userSpecifiedSchema = Option(schema)
    this
  }

  override def option(key: String, value: String): DataFrameReader = {
    this.extraOptions += (key -> value)
    this
  }

  override def options(options: scala.collection.Map[String, String]): DataFrameReader = {
    this.extraOptions ++= options
    this
  }

  def partitionColumns(partitionColumns: List[String]): DataFrameReader = {
    this.partitionColumns ++= partitionColumns
    this
  }

  private var partitionColumns: Array[String] = Array.empty[String]

  private var source: String = sqlContext.conf.defaultDataSourceName

  private var userSpecifiedSchema: Option[StructType] = None

  private var extraOptions = new scala.collection.mutable.HashMap[String, String]
}
