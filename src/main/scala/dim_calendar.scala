import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF
import org.apache.hadoop.hive.serde2.objectinspector.primitive.{ PrimitiveObjectInspectorFactory}
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorFactory, StructObjectInspector}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter

object date_table {
  def main(args: Array[String]): Unit = {

    //临时设置日志打印级别
    Logger.getLogger("org").setLevel(Level.WARN)

    //初始化SparkSession，整个程序的入口
    val spark = SparkSession.builder()
      .appName("生成日期维度表")
      .config("spark.default.parallelism", 1)
      .master("local")
      .enableHiveSupport()
      .getOrCreate()

    //注册自定义UDTF函数，用hive的函数注册方式
    spark.sql("create temporary function date_udtf as 'DateExplode'")//拷贝类名全路径

    spark.sql(
      """
        |select
        |
        |   date_udtf("2024-01-01", "2025-01-01")
        |
        |
        |""".stripMargin).show(367)
//      .write.mode("overwrite")
//      .format("hive")
//      .saveAsTable("fincredit.dim_calendar")
//    spark.sql(s"select * from fincredit.dim_calendar").show(367)


    spark.stop()

  }

}


//构造UDTF类继承hive中的父类实现日期炸开维度表函数
class DateExplode extends GenericUDTF{

  //对输入参数检查，对输出进行结构定义（几列，什么列名）
  override def initialize(argOIs: Array[ObjectInspector]): StructObjectInspector = {

    //检查是否输入两个参数以及是否字符串类型：这里没做日期检验因为有些细节没处理好;输入时务必确保两个日期为“yyyy-MM-dd”
    if(argOIs.length != 2)throw new IllegalArgumentException("输入参数个数不对，应该输入2个日期")
    if(!argOIs(0).getTypeName .equals("string")) throw new IllegalArgumentException("输入参数类型不对，应该输入string")

    //构造输出结果的列名list
    val fieldNames = new java.util.ArrayList[String]()
    fieldNames.add("dateId")                //日期id
    fieldNames.add("date_desc")             //日期描述
    fieldNames.add("day_of_month")          //日期是这个月的第几天
    fieldNames.add("day_of_month_desc")     //日期是这个月的第几天描述
    fieldNames.add("day_of_year")           //日期是这个年的第几天
    fieldNames.add("day_of_year_desc")      //日期是这个年的第几天描述
    fieldNames.add("week_of_year")          //第几周
    fieldNames.add("week_of_year_desc")     //第几周描述
    fieldNames.add("weekDayId")             //星期几id
    fieldNames.add("weekDay_desc")          //星期几描述
    fieldNames.add("month_of_year")         //第几月
    fieldNames.add("month_of_year_desc")    //第几月描述
    fieldNames.add("monthId")               //月份id
    fieldNames.add("month_desc")            //月份描述
    fieldNames.add("yearId")                //年份id
    fieldNames.add("year_desc")             //年份描述
    fieldNames.add("quarterId")             //季度id
    fieldNames.add("quarter_desc")          //季度描述
    fieldNames.add("quarter_of_year")       //第几季度
    fieldNames.add("quarter_of_year_desc")  //第几季度描述
    fieldNames.add("star_sign")             //星座
    fieldNames.add("create_time")           //创建时间
    fieldNames.add("update_time")           //更新时间
    fieldNames.add("etl_time")              //etl时间

    // 使用nCopies创建重复元素的类型列表（总共24字段，都是字符串类型）
    //构造输出结果的数据类型list
    val fieldType = java.util.Collections.nCopies(24,
      PrimitiveObjectInspectorFactory.javaStringObjectInspector.asInstanceOf[ObjectInspector]
    )

    //根据字段名和字段类型构建表结构
    ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldType)

  }

  override def process(objects: Array[AnyRef]): Unit = {

    //输入参数为Scala中AnyRef类型，需要转为Java中String类型
    //将输入参数转为时间对象
    val startDate = LocalDate.parse(objects(0).toString)
    val endDate = LocalDate.parse(objects(1).toString)
    //获取当前时间
    val nowTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))


    //循环输出（两个日期中间每一天）
    for(i <- startDate.toEpochDay to endDate.toEpochDay){
      val i_Time = LocalDate.ofEpochDay(i) //取到的日期

      //日期id及其描述
      val dateId = i_Time.format(DateTimeFormatter.ofPattern("yyyyMMdd"))
      val date_desc = i_Time.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))

      //日期是这个月的第几天及其描述
      val day_of_month = i_Time.getDayOfMonth
      val day_of_month_desc = i_Time.format(DateTimeFormatter.ofPattern(s"yyyy年M月第${day_of_month}天"))

      //日期是这个年的第几天及其描述
      val day_of_year = i_Time.getDayOfYear
      val day_of_year_desc = i_Time.format(DateTimeFormatter.ofPattern(s"yyyy年第${day_of_year}天"))

      //这个周是这一年第几周及其描述
      val week_of_year = i_Time.format(DateTimeFormatter.ofPattern("w"))
      val week_of_year_desc = i_Time.format(DateTimeFormatter.ofPattern(s"yyyy年第${week_of_year}周"))
      val weekDayId = i_Time.getDayOfWeek.getValue
      val weekDay_desc = WeekToLower(i_Time.getDayOfWeek.toString)   /*该方法 WeekToLower为自定义返回一个首字母大写的星期描述*/

      //这个月是这个年的第几月及其描述
      val month_of_year = i_Time.getMonthValue
      val month_of_year_desc = i_Time.format(DateTimeFormatter.ofPattern(s"yyyy年第${month_of_year}月"))
      val monthId = i_Time.format(DateTimeFormatter.ofPattern("yyyyMM"))
      val month_desc = i_Time.format(DateTimeFormatter.ofPattern("yyyy-MM"))

      //年份id及其描述
      val yearId = i_Time.getYear
      val yearId_desc = i_Time.getYear + "年"

      //季度id及其描述
      val quarterId = i_Time.format(DateTimeFormatter.ofPattern("yyyyQQ"))
      val quarter_of_year = i_Time.format(DateTimeFormatter.ofPattern("Q"))
      val quarter_desc = i_Time.format(DateTimeFormatter.ofPattern("yyyy-")) + "Q" + quarter_of_year
      val quarter_of_year_desc = i_Time.format(DateTimeFormatter.ofPattern(s"yyyy年Q季度"))

      //星座描述
      val star_sign = getStartSigns(month_of_year, day_of_month)

      //创建时间、更新时间、etl时间为当前时间点
      val create_time = nowTime
      val update_time = nowTime
      val etl_time = nowTime

      forward(Array(
        dateId,                     //日期id
        date_desc,                  //日期描述
        day_of_month,               //日期是这个月的第几天
        day_of_month_desc,          //日期是这个月的第几天描述
        day_of_year,                //日期是这个年的第几天
        day_of_year_desc,           //日期是这个年的第几天描述
        week_of_year,               //第几周
        week_of_year_desc,          //第几周描述
        weekDayId,                  //星期几id
        weekDay_desc,               //星期几描述
        month_of_year,              //第几月
        month_of_year_desc,         //第几月描述
        monthId,                    //月份id
        month_desc,                 //月份描述
        yearId,                     //年份id
        yearId_desc,                //年份描述
        quarterId,                  //季度id
        quarter_desc,               //季度描述
        quarter_of_year,            //第几季度
        quarter_of_year_desc,       //第几季度描述
        star_sign,                  //星座
        create_time,                //创建时间
        update_time,                //更新时间
        etl_time                    //etl时间

        ) ) //输出一行数据
    }

  }
  //重写抽象方法关闭相关流，没有则不填
  override def close(): Unit = {

  }

  // 格式化字符串的方法
  def WeekToLower(s: String): String = { s.substring(0, 1) + s.substring(1).toLowerCase }

  //查询星座方法
  def getStartSigns(month: Int, day: Int): String = {
    month match {
      case 3 => if (day >= 21) "白羊座" else "双鱼座"
      case 4 => if (day <= 19) "白羊座" else "金牛座"
      case 5 => if (day <= 20) "金牛座" else "双子座"
      case 6 => if (day <= 21) "双子座" else "巨蟹座"
      case 7 => if (day <= 22) "巨蟹座" else "狮子座"
      case 8 => if (day <= 22) "狮子座" else "处女座"
      case 9 => if (day <= 22) "处女座" else "天秤座"
      case 10 => if (day <= 23) "天秤座" else "天蝎座"
      case 11 => if (day <= 22) "天蝎座" else "射手座"
      case 12 => if (day <= 21) "射手座" else "摩羯座"
      case 1 => if (day <= 19) "摩羯座" else "水瓶座"
      case 2 => if (day <= 18) "水瓶座" else "双鱼座"
      case _ => "未知星座"
    }
  }
}
































