package org.apache.griffin.measure

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

import scala.reflect.io.File

class ApplicationTest extends FunSuite{
  test(""){
//_accuracy-batch-griffindsl.json
    genJob("_uniqueness-batch-griffindsl-hive.json")
  }
  //有效性
  test("validity"){
    genJob("test//validity.json")
  }
  //记录数一致性
  test("recordcount"){
    genJob("test//recordcount.json")
  }

  def genJob(fileName: String): Unit = {
    /*D:\workspace\github\tangpengcsu\griffin\measure\src\test*/
     System.setProperty("user.dir","D:\\workspace\\github\\tangpengcsu\\griffin\\measure")
    println(System.getProperty("user.dir"))
  val dir = System.getProperty("user.dir") + File.separator + "src\\test" +
      s"\\resources\\"
//D:\workspace\github\tangpengcsu\griffin.git\tags\griffin-0.4.0

    var envFileDir = dir +"env-batch.json"

    val envSource = scala.io.Source.fromFile(envFileDir)
    val env = envSource.mkString

    val dqFileDir = dir+fileName
    val dqSource = scala.io.Source.fromFile(dqFileDir)
    val dq = dqSource.mkString

    Application.main(Array(env,dq))
  }

  test("sql"){
    val conf = new SparkConf().setAppName("test")
    //conf.set("spark.sql.warehouse.dir", "hdfs://192.168.50.88:9000/user/hive/warehouse")
    conf.set("hive.metastore.warehouse.dir","hdfs://192.168.50.88:9000/user/hive/warehouse")
    //conf.set("fs.defaultFS", "hdfs://192.168.50.88:9000")
    val spark =   SparkSession.builder().master("local[2]").config(conf).enableHiveSupport().getOrCreate()
    spark.sql("SELECT * FROM default.src_info").show

  }
}
