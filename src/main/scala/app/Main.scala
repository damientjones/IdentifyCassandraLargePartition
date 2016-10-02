package app

import app.util._

object Main {

  def build(fileName: String) {
    YamlUtil.parseYaml(fileName)
  }

  def main(args: Array[String]) {
    val (keySpace, tableName, fileName) = (args(0), args(1), args(2))
    build(fileName)
    val (targetTable, targetKeyspace) = (YamlUtil.getConfigs.outputTable, YamlUtil.getConfigs.outputKeyspace)
    val minPartSize = YamlUtil.getConfigs.minPartSize
    val converter = TypeConverter.fieldToString
    val cc = SparkContextUtil.cassandraConnector
    val keys = CassandraUtil.getPartKeys(tableName, keySpace).split(",").toList
    val (df, uuidFields) = (CassandraUtil.getDataframe(tableName, keySpace, "false"), CassandraUtil.getUUIDKeys(tableName, keySpace))
    val schema = df.schema.fields.map(x => (x.name, x.dataType.typeName)).toMap
    val keySchema = keys.map(x => {
      (x, schema.get(x).get)
    })
    df.foreachPartition(x => {
      FunctionUtil().setKeySpace(keySpace)
        .setTableName(tableName)
        .setTargetKeySpace(targetKeyspace)
        .setTargetTableName(targetTable)
        .setUuidFields(uuidFields)
        .setKeySchema(keySchema)
        .setConverter(converter)
        .setCassandraConnector(cc)
        .setMinPartSize(minPartSize)
        .rowsToTuple(x)
    })
    SparkContextUtil.sparkContext.stop
    System.exit(0)
  }

}
