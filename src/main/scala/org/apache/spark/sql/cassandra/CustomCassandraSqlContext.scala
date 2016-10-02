package org.apache.spark.sql.cassandra

import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrameReader

class CustomCassandraSqlContext(sc: SparkContext) extends CassandraSQLContext(sc) {
  override def read: DataFrameReader = new CustomDataframeReader(this)
}
