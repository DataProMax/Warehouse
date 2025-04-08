import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.sql.DriverManager
import java.util.Properties

object ods_MysqlToHive {
  /**
   * 读取Mysql数据导入Hive
   * 小脚本
   * @param args
   */

  def main(args: Array[String]): Unit = {


    //临时设置日志打印级别
    Logger.getLogger("org").setLevel(Level.WARN)

    //初始化SparkSession，整个程序的入口
    val spark = SparkSession.builder()
      .enableHiveSupport()
      .appName("读取Mysql导入Hive")
      .config("spark.default.parallelism", 1)
      .master("local")
      .getOrCreate()
    //调大SparkSQL的默认打印字段个数，避免打印的json数据过长(好像没多大用)
    spark.conf.set("spark.sql.debug.maxToStringFields", 1000)

    //初始化jdbc数据库连接信息
    val URL = "jdbc:mysql://node1:3306/jrxd?zeroDateTimeBehavior=convertToNull"
    val properties = new Properties()
    properties.put("user", "****")
    properties.put("password", "******")

    readTableIncrement( spark, URL, properties,"dict_product",s" date(updated_at) = '2024-09-24'")

    spark.stop()


  }


  //全表全量导入
  def readAllTables(spark: SparkSession, URL: String, properties: Properties): Unit = {
    // 手动获取mysql中的表名储存到文件，用RDD解析
    val tables:RDD[String] = spark.sparkContext.textFile("data/tableNames/tableNames.txt")
    //不加这一条会报错，好像是什么在Executor端转到driver端运行
    val tableNames = tables.collect()

    //循环遍历表名，写入hive，注意ods层命名规范
    tableNames.foreach(tableName => {
      val df = spark.read.jdbc(URL, tableName, properties)
      df.write.mode("Overwrite").saveAsTable(s"fincredit.ods_${tableName}")
      println(s"写入ods_${tableName}完成...")//添加标记
    })

    println("所有数据写入完毕，正在加载数据....")


    //测试写入数据是否完整
    tableNames.foreach(tableName => {
      spark.sql(s"select * from fincredit.ods_${tableName}").show(5)
      println(s"加载ods_${tableName}完成...")
    })

    println("所有数据加载完毕...")
    //至此ods层数据写入完毕，加载完毕，程序结束（写入格式默认parquet）

  }

  //单表全量导入
  def readTable(spark: SparkSession, URL: String, properties: Properties, tableName: String): Unit = {
    //手动获取mysql中的表名储存到文件，用RDD解析
    val df = spark.read.jdbc(URL, tableName, properties)
    df.write.mode("Overwrite").saveAsTable(s"fincredit.ods_${tableName}")
    println(s"写入ods_${tableName}完成...")//添加标记

  }

  //单表增量导入
  def readTableIncrement(spark: SparkSession, URL: String, properties: Properties ,tableName: String,query:String): Unit = {
    //手动获取mysql中的表名储存到文件，用RDD解析
    //获取临时表
    spark.read
      .jdbc(URL, tableName, properties)
      .createOrReplaceTempView("tmp")

    val update = spark.sql(
      s"""
         |select * from tmp
         |where $query
         |""".stripMargin)
    println("加载增量数据完毕...正在导入...")

//     将增量数据写入 Hive
    update.write.mode("append").saveAsTable(s"fincredit.ods_${tableName}")
    println(s"写入ods_${tableName}完成...")
  }

}
