package com.jererc

import java.io.File
import scala.io.Source
import scala.util.matching.Regex

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.cassandra.CassandraSQLContext

import net.liftweb.json._

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

    // Test input table
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
      errors :+= "invalid where content"
    }

    // Test SQL
    val cc = new CassandraSQLContext(sc)
    val rdd8: SchemaRDD = cc.sql(s"SELECT * from ${conf.cassandraKeyspace}.${conf.cassandraInputTable} WHERE id < 10")
    val result5 = rdd8.map(e => (e.getInt(0), e.getString(1))).collect.sortBy(_._1)
    val expected5 = (0 to 9).toArray.map(n => (n, s"value $n"))
    if (result5.deep != expected5.deep) {
      errors :+= "invalid sql result"
    }

    // Test common transformations and actions
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

    sc.stop()

    if (errors.length == 0) {
      println("[OK] all tests passed")
    } else {
      errors.map(e => s"[KO] $e").foreach(println)
    }

  }
}
