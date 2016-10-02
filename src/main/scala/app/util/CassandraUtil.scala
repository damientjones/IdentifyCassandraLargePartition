package app.util

object CassandraUtil {
  private def stripValues = (string:String) =>{
    string.replace("[","").replace("\"","").replace("]","")
  }
  def getDataframe(tableName: String, keySpace: String, pushDown: String = "false") = {
    SparkContextUtil.cassandraContext.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> tableName,
        "keyspace" -> keySpace,
        "pushdown" -> pushDown))
      .load()
  }
  def getPartKeys(tableName: String, keySpace: String) : String = {
    getDataframe("schema_columnfamilies", "system", "true")
      .filter(s"keyspace_name = '${keySpace}' AND columnfamily_name = '${tableName}'")
      .limit(1)
      .select("key_aliases")
      .map(x=>stripValues(x.getString(0)))
      .collect()(0)
  }
  def getUUIDKeys(tableName: String, keySpace: String) : Seq[String] = {
    getDataframe("schema_columns", "system", "true")
      .filter(s"keyspace_name = '${keySpace}' AND columnfamily_name = '${tableName}'")
      .filter("validator IN ('org.apache.cassandra.db.marshal.UUIDType','org.apache.cassandra.db.marshal.TimeUUIDType')")
      .select("column_name")
      .collect
      .toSeq
      .map(x => x.getAs[String]("column_name"))
  }
}
