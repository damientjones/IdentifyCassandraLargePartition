package app.util

import com.datastax.spark.connector.cql.{CassandraConnectorConf, CassandraConnector}
import org.apache.spark.sql.cassandra.{CustomCassandraSqlContext, CassandraSQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object SparkContextUtil {
  lazy private val sparkConf : SparkConf = {
    val conf = new SparkConf().setAppName(YamlUtil.getConfigs.appName)//.setMaster("local[8]") --used for testing locally
    YamlUtil.getSparkConfigs.foreach(x => conf.set(x._1, x._2))
    conf
  }
  lazy private val sc : SparkContext = new SparkContext(sparkConf)
  lazy private val csc : CassandraSQLContext = new CustomCassandraSqlContext(sc)
  lazy private val cc : CassandraConnector = new CassandraConnector(CassandraConnectorConf(sparkConf))

  def sparkContext = {
    sc
  }

  def cassandraContext = {
    csc.setKeyspace(YamlUtil.getConfigs.defaultKeyspace)
    csc
  }

  def cassandraConnector = {
    cc
  }
}