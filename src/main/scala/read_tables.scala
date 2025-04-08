import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object read_tables {
  def main(args: Array[String]): Unit = {
    //临时设置日志打印级别
    Logger.getLogger("org").setLevel(Level.WARN)

    val spark = SparkSession.builder()
      .appName("用于替代Hive执行部分SQL")
      .config("spark.default.parallelism", 1)
      .master("local")
      .enableHiveSupport()
      .getOrCreate()

//    spark.sql(
//      """
//        |set hive.exec.dynamic.partition.mode=nonstrict;
//        |""".stripMargin)
//
//
//    spark.sql(
//      """
//        |insert overwrite table fincredit.dim_channel
//        |select id,
//        |       name,
//        |       channel_code,
//        |       channel_type,
//        |       channel_cus_fee,
//        |       channel_perf_fee,
//        |       updated_at,
//        |       created_at,
//        |       from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as etl_time,
//        |       from_unixtime(unix_timestamp(),'yyyy-MM-dd') as partition_date
//        |from fincredit.ods_channel_info;
//        |
//        |
//        |""".stripMargin)

    spark.sql(
      """
        |select * from fincredit.dim_channel
        |""".stripMargin).show


  }
}
