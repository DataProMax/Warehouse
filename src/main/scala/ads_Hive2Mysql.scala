import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import java.util.Properties

object ads_Hive2Mysql {
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


    //初始化jdbc数据库连接信息
    val URL = "jdbc:mysql://node1:3306/fincredit_bi?useUnicode=true&characterEncoding=UTF-8"
    val properties = new Properties()
    properties.put("user", "****")
    properties.put("password", "******")

    writeTable(spark, URL, properties, "fincredit", "data/tableNames/hive_ads.txt")


    spark.stop()
  }

  def writeTable(spark: SparkSession, URL: String, properties: Properties, database: String,Path: String): Unit = {

    //批量读取表名
    val ads_tables = spark.sparkContext.textFile(Path)
    val tableNames = ads_tables.collect()
    //获取ads层数据
    tableNames.foreach(tableName => {
      val df = spark.read.table(database+"."+tableName)
      println(s"读取Hive中${tableName}完成...")//添加标记

      //写入mysql
      df.write.mode("Overwrite").jdbc(URL, tableName, properties)
      println(s"写入MySQL中${tableName}完成...")

    })

    println("所有数据写入完毕，正在加载数据....")

    tableNames.foreach(tableName => {
      spark.read.jdbc(URL, tableName, properties).show(10)
      println(s"加载MySQL中${tableName}完成...")
    })


  }

}
