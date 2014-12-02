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

    var errors = List[String]()

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

    // Test input table
    val rdd = sc.cassandraTable(conf.cassandraKeyspace, conf.cassandraInputTable)
    val count = 10000
    val data = (0 to count - 1) map (n => (n, s"value $n"))
    val collection = sc.parallelize(data)
    collection.saveToCassandra(conf.cassandraKeyspace, conf.cassandraInputTable,
      SomeColumns("id", "data"))
    if (rdd.count != count) {
      errors :+= "invalid cassandra input table content"
    }

    // Test filter
    val expected = (0 to 9).toArray.map(n => (n, s"value $n"))
    val rdd2 = rdd.filter(row => row.getInt("id") < 10)
    val res1 = rdd2.map(row => (row.getInt("id"), row.getString("data"))).collect.sortBy(_._1)
    if (res1.deep != expected.deep) {
      errors :+= "invalid filter result"
    }

    // Test output table
    rdd2.saveToCassandra(conf.cassandraKeyspace, conf.cassandraOutputTable,
      SomeColumns("id", "data"))
    val res2 = sc.cassandraTable[(Int, String)](conf.cassandraKeyspace, conf.cassandraOutputTable)
      .select("id", "data").collect.sortBy(_._1)
    if (res2.deep != expected.deep) {
      errors :+= "invalid cassandra output table content"
    }

    // Test select / where
    val expected2 = Array((11, "value 11"))
    val rdd3 = rdd.select("id", "data").where("id = ?", "11")
    val res3 = rdd3.map(row => (row.getInt("id"), row.getString("data"))).collect
    if (res3.deep != expected2.deep) {
      errors :+= "invalid where content"
    }

    // Test spark SQL
    val cc = new CassandraSQLContext(sc)
    val rdd4: SchemaRDD = cc.sql(s"SELECT * from ${conf.cassandraKeyspace}.${conf.cassandraInputTable} WHERE id < 10")
    val res4 = rdd4.map(e => (e.getInt(0), e.getString(1))).collect.sortBy(_._1)
    if (res4.deep != expected.deep) {
      errors :+= "invalid sql result"
    }

    sc.stop()

    if (errors.length == 0) {
      println("[OK] all tests passed")
    } else {
      errors.map(e => s"[KO] $e").foreach(println)
    }

  }
}
