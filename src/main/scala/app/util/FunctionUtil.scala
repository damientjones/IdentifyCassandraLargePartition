package app.util

import java.util.Date

import app.custom.list.PairList
import com.datastax.driver.core.{BatchStatement, Session}
import com.datastax.driver.core.ConsistencyLevel.LOCAL_QUORUM
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.Row
import org.joda.time.DateTime

class FunctionUtil {
  private var keySpace: String = _
  private var tableName: String = _
  private var targetKeySpace: String = _
  private var targetTableName: String = _
  private var uuidFields: Seq[String] = _
  private var keySchema: List[(String, String)] = _
  private var converter: (Row, String, String, Seq[String]) => (String, Any) = _
  private var cc: CassandraConnector = _
  private var minPartSize: Int = _
  private val now: Date = DateTime.now.toDate

  def setKeySpace(keySpace: String) = {
    this.keySpace = keySpace
    this
  }

  def setTableName(tableName: String) = {
    this.tableName = tableName
    this
  }

  def setTargetKeySpace(targetKeySpace: String) = {
    this.targetKeySpace = targetKeySpace
    this
  }

  def setTargetTableName(targetTableName: String) = {
    this.targetTableName = targetTableName
    this
  }

  def setUuidFields(uuidFields: Seq[String]) = {
    this.uuidFields = uuidFields
    this
  }

  def setKeySchema(keySchema: List[(String, String)]) = {
    this.keySchema = keySchema
    this
  }

  def setConverter(converter: (Row, String, String, Seq[String]) => (String, Any)) = {
    this.converter = converter
    this
  }

  def setCassandraConnector(cc: CassandraConnector) = {
    this.cc = cc
    this
  }

  def setMinPartSize(minPartSize: Int) = {
    this.minPartSize = minPartSize
    this
  }

  private def getToken = (kvPair: List[(String, Any)], sess: Session) => {
    val tokenField = "token(" + kvPair.map(_._1).mkString(",") + ")"
    val where = QueryBuilder.select(tokenField).from(keySpace, tableName).where
    kvPair.map(x => where.and(QueryBuilder.eq(x._1, x._2)))
    where.setConsistencyLevel(LOCAL_QUORUM)
    where.limit(1)
    sess.execute(where).all().get(0).getPartitionKeyToken.getValue.toString
  }

  private def tupleToObject(iteratorPair: Iterator[(List[(String, Any)], (Long, Long))]): Unit = {
    val token = getToken
    val session = cc.openSession()
    val stmt = session.prepare(s"INSERT INTO ${targetKeySpace}.${targetTableName} (table_name, token_val, key, rec_count, size_mb, time) VALUES (?, ?, ?, ?, ?, ?)")
    val batch = new BatchStatement
    iteratorPair.foreach(x => {
      val pk = x._1.map(x => x._1 + ": " + x._2.toString).mkString(",")
      batch.add(stmt.bind(tableName.asInstanceOf[Object],
        token(x._1, session).asInstanceOf[Object],
        pk.asInstanceOf[Object],
        x._2._2.asInstanceOf[Object],
        x._2._1.asInstanceOf[Object],
        now.asInstanceOf[Object]))
      if (batch.size >= 10000) {
        session.execute(batch)
        batch.clear
      }
    })
    if (batch.size() > 0) {
      session.execute(batch)
    }
    session.close
  }

  def rowsToTuple(rows: Iterator[Row]) = {
    val mapOutput = rows.map(x => {
      val key = keySchema.map(z => converter(x, z._1, z._2, uuidFields))
      val value = (x.toString.getBytes("UTF-8").length.toLong, 1L)
      (key, value)
    })
    tupleToObject(PairList((0L, 0L), mapOutput.toSeq: _*)
      .reduceByKey((x: (Long, Long), y: (Long, Long)) => (x._1 + y._1, x._2 + y._2))
      .filter(x => x._2._1 > minPartSize)
      .toIterator)
  }
}

object FunctionUtil {
  def apply() = {
    new FunctionUtil
  }
}
