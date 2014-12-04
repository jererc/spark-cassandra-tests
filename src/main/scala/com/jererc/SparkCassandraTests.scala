package com.jererc

import java.io.File
import scala.io.Source
import scala.util.matching.Regex

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.cassandra.CassandraSQLContext

// PairRDDFunctions
import org.apache.spark.SparkContext._

import net.liftweb.json._

case class TestData(id: Int, data: String)

object SparkCassandraTests {

  val defaultConf = """{
    "cassandraHost" : "",
    "cassandraRpcPort" : "9160"
    "cassandraKeyspace" : "test_keyspace"
    "cassandraInputTable" : "test_table_input"
    "cassandraOutputTable" : "test_table_output"
    "sparkMasterHost" : ""
    "sparkMasterPort" : "7077"
  }"""

  implicit val formats = DefaultFormats
  case class Conf(
    cassandraHost: String,
    cassandraRpcPort: String,
    cassandraKeyspace: String,
    cassandraInputTable: String,
    cassandraOutputTable: String,
    sparkMasterHost: String,
    sparkMasterPort: String
  )

  def recursiveListFiles(f: File, r: Regex): Array[File] = {
    val these = f.listFiles
    val good = these.filter(f => r.findFirstIn(f.getName).isDefined)
    good ++ these.filter(_.isDirectory).flatMap(recursiveListFiles(_,r))
  }

  def main(args: Array[String]) {

    val jsonConf = parse(defaultConf) merge parse(Source.fromFile("local_settings.json").getLines.mkString)
    val conf = jsonConf.extract[Conf]

    val sparkConf = new SparkConf(true)
    sparkConf.set("spark.cassandra.connection.host", conf.cassandraHost)
    sparkConf.set("spark.cassandra.connection.rpc.port", conf.cassandraRpcPort)
    // We push this assembly into spark
    val sparkJars = recursiveListFiles(new File("target"), """\bassembly\b.*\.jar$""".r).map(f => f.getAbsolutePath)
    sparkConf.setJars(sparkJars)

    // Init cassandra keyspace and tables
    val connector = CassandraConnector(sparkConf)
    connector.withSessionDo { session =>
      session.execute(s"DROP KEYSPACE IF EXISTS ${conf.cassandraKeyspace};")
      session.execute(s"CREATE KEYSPACE ${conf.cassandraKeyspace} WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
    }
    for (tableName <- List(conf.cassandraInputTable, conf.cassandraOutputTable)) {
      connector.withSessionDo { session =>
        session.execute(s"DROP TABLE IF EXISTS ${conf.cassandraKeyspace}.$tableName;")
        session.execute(s"CREATE TABLE ${conf.cassandraKeyspace}.$tableName (id int PRIMARY KEY, data text);")
      }
    }

    val sc = new SparkContext(s"spark://${conf.sparkMasterHost}:${conf.sparkMasterPort}",
      "TestCassandra", sparkConf)

    var errors = List[String]()

    //
    // Test table read and write
    //
    val rdd = sc.cassandraTable(conf.cassandraKeyspace, conf.cassandraInputTable)
    val count = 10000
    val data = (0 to count - 1) map (n => (n, s"value $n"))
    val collection = sc.parallelize(data)
    collection.saveToCassandra(conf.cassandraKeyspace, conf.cassandraInputTable,
      SomeColumns("id", "data"))
    val result = rdd.map(row => (row.getInt("id"), row.getString("data"))).collect.sortBy(_._1)
    val expected = data.toArray
    if (result.deep != expected.deep) {
      errors :+= "invalid cassandra input table content"
    }

    // Test filter
    val rdd2 = sc.cassandraTable(conf.cassandraKeyspace, conf.cassandraInputTable)
    val rdd3 = rdd2.filter(row => row.getInt("id") < 10)
    val result2 = rdd3.map(row => (row.getInt("id"), row.getString("data"))).collect.sortBy(_._1)
    val expected2 = (0 to 9).toArray.map(n => (n, s"value $n"))
    if (result2.deep != expected2.deep) {
      errors :+= "invalid filter result"
    }

    // Test output table
    val rdd4 = sc.cassandraTable(conf.cassandraKeyspace, conf.cassandraInputTable)
    val rdd5 = rdd4.filter(row => row.getInt("id") < 10)
    rdd5.saveToCassandra(conf.cassandraKeyspace, conf.cassandraOutputTable,
      SomeColumns("id", "data"))
    val result3 = sc.cassandraTable[(Int, String)](conf.cassandraKeyspace, conf.cassandraOutputTable)
      .select("id", "data").collect.sortBy(_._1)
    val expected3 = (0 to 9).toArray.map(n => (n, s"value $n"))
    if (result3.deep != expected3.deep) {
      errors :+= "invalid cassandra output table content"
    }

    // Test select / where
    val rdd6 = sc.cassandraTable(conf.cassandraKeyspace, conf.cassandraInputTable)
    val rdd7 = rdd6.select("id", "data").where("id = ?", "11")
    val result4 = rdd7.map(row => (row.getInt("id"), row.getString("data"))).collect
    val expected4 = Array((11, "value 11"))
    if (result4.deep != expected4.deep) {
      errors :+= "invalid where result"
    }

    //
    // Test SQL
    //
    val cc = new CassandraSQLContext(sc)
    val rdd8: SchemaRDD = cc.sql(s"SELECT * from ${conf.cassandraKeyspace}.${conf.cassandraInputTable} WHERE id < 10")
    val result5 = rdd8.map(e => (e.getInt(0), e.getString(1))).collect.sortBy(_._1)
    val expected5 = (0 to 9).toArray.map(n => (n, s"value $n"))
    if (result5.deep != expected5.deep) {
      errors :+= "invalid sql result"
    }

    //
    // Test common transformations and actions
    //
    val rdd9 = sc.cassandraTable(conf.cassandraKeyspace, conf.cassandraInputTable)
    val rdd10 = rdd9
      .filter(row => (row.getInt("id") >= 100 && row.getInt("id") < 110) || (row.getInt("id") >= 1000 && row.getInt("id") < 1010))
      .map(row => (row.getInt("id"), s"new ${row.getString("data")}"))
    if (rdd10.count != 20) {
      errors :+= "invalid count result"
    }
    val result6 = rdd10.collect.sortBy(_._1)
    val expected6 = ((100 to 109) ++ (1000 to 1009)).toArray.map(n => (n, s"new value $n"))
    if (result6.deep != expected6.deep) {
      errors :+= "invalid map result"
    }
    val rdd11 = rdd9
      .filter(row => row.getInt("id") >= 100 && row.getInt("id") < 110)
      .map(row => row.getInt("id"))
    val result7 = rdd11.fold(0)((x, y) => x + y)
    val expected7 = (100 to 109) reduce ((x, y) => x + y)
    if (result7 != expected7) {
      errors :+= "invalid fold result"
    }

    // Test custom type
    val rdd12 = sc.cassandraTable[TestData](conf.cassandraKeyspace, conf.cassandraInputTable)
    val result8 = rdd12.filter(r => r.id < 10).collect.sortBy(_.id)
    val expected8 = (0 to 9).toArray.map(n => TestData(n, s"value $n"))
    if (result8.deep != expected8.deep) {
      errors :+= "invalid custom type result"
    }

    //
    // Test pair RDD
    //
    connector.withSessionDo { session =>
      session.execute(s"DROP TABLE IF EXISTS ${conf.cassandraKeyspace}.${conf.cassandraOutputTable};")
      session.execute(s"CREATE TABLE ${conf.cassandraKeyspace}.${conf.cassandraOutputTable} (id int PRIMARY KEY, data text);")
    }
    val data2 = Seq((1, "v1"), (2, "v2"), (3, "v1"), (4, "v2"), (5, "v3"))
    val collection2 = sc.parallelize(data2)
    collection2.saveToCassandra(conf.cassandraKeyspace, conf.cassandraOutputTable,
      SomeColumns("id", "data"))
    val rdd13 = sc.cassandraTable[TestData](conf.cassandraKeyspace, conf.cassandraOutputTable)
      .map(r => (r.data, r.id))

    // Test groupByKey
    val rdd14 = rdd13.groupByKey()
    val result9 = rdd14.sortBy(_._1).collect
    val expected9 = Array(("v1", Seq(1, 3)), ("v2", Seq(2, 4)), ("v3", Seq(5)))
    if (result9.deep != expected9.deep) {
      errors :+= "invalid groupByKey result"
    }
    val result10 = rdd14.keys.collect.sorted
    val expected10 = Array("v1", "v2", "v3")
    if (result10.deep != expected10.deep) {
      errors :+= "invalid groupByKey keys"
    }
    val result11 = rdd14.values.collect.sorted
    val expected11 = Array(Seq(1, 3), Seq(2, 4), Seq(5))
    if (result11.deep != expected11.deep) {
      errors :+= "invalid groupByKey values"
    }

    // Test reduceByKey
    val rdd15 = rdd13.reduceByKey((x, y) => x + y)
    val result12 = rdd15.sortBy(_._1).collect
    val expected12 = Array(("v1", 4), ("v2", 6), ("v3", 5))
    if (result12.deep != expected12.deep) {
      errors :+= "invalid reduceByKey result"
    }
    val result13 = rdd15.keys.collect.sorted
    val expected13 = Array("v1", "v2", "v3")
    if (result13.deep != expected13.deep) {
      errors :+= "invalid reduceByKey keys"
    }
    val result14 = rdd15.values.collect.sorted
    val expected14 = Array(4, 5, 6)
    if (result14.deep != expected14.deep) {
      errors :+= "invalid reduceByKey values"
    }

    sc.stop()

    if (errors.length == 0) {
      println("[OK] all tests passed")
    } else {
      errors.map(e => s"[KO] $e").foreach(println)
    }

  }
}
