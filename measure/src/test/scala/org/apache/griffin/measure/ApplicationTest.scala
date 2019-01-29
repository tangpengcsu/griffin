package org.apache.griffin.measure

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

import scala.reflect.io.File

/** 版权声明：本程序模块属于大数据分析平台（KDBI）的一部分
  * 金证科技股份有限公司 版权所有
  *
  * 模块名称：${DESCRIPTION}
  * 模块描述：${DESCRIPTION}
  * 开发作者：tang.peng
  * 创建日期：2019/1/28
  * 模块版本：1.0.1.0
  * ----------------------------------------------------------------
  * 修改日期        版本        作者          备注
  * 2019/1/28     1.0.1.0       tang.peng     创建
  * ----------------------------------------------------------------
  */
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
     System.setProperty("user.dir","D:\\workspace\\github\\tangpengcsu\\griffin.git\\tags\\griffin-0.4.0\\measure")
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
