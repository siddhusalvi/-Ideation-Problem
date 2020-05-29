import SqlApp.logDfSchema
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import org.joda.time.DateTimeFieldType
object SqlApp extends App{
  val spark = SparkSession.builder()
    .config("spark.master","local")
    .appName("SqlApp")
    .getOrCreate()

  val logDfSchema = StructType(
    Array(
      StructField("DateTime",StringType),
      StructField("Cpu_Count",IntegerType),
      StructField("Cpu_Working_Time",DoubleType),
      StructField("Cpu_Idle_Time",DoubleType),
      StructField("Cpu_Percent",DoubleType),
      StructField("Usage_Cpu_Count",IntegerType),
      StructField("Software_Interrupts",IntegerType),
      StructField("System_calls",IntegerType),
      StructField("Interrupts",IntegerType),
      StructField("Load_Time_1_min",DoubleType),
      StructField("Load_Time_5_min",DoubleType),
      StructField("Load_Time_15_min",DoubleType),
      StructField("Total_Memory",DoubleType),
      StructField("Used_Memory",DoubleType),
      StructField("Free_Memory",DoubleType),
      StructField("Active_Memory",DoubleType),
      StructField("Inactive_Memory",DoubleType),
      StructField("Bufferd_Memory",DoubleType),
      StructField("Cache_Memory",DoubleType),
      StructField("Shared_Memory",DoubleType),
      StructField("Available_Memory",DoubleType),
      StructField("Total_Disk_Memory",DoubleType),
      StructField("Used_Disk_Memory",DoubleType),
      StructField("Free_Disk_Memory",DoubleType),
      StructField("Read_Disk_Count",IntegerType),
      StructField("Write_Disk_Count",IntegerType),
      StructField("Read_Disk_Bytes",DoubleType),
      StructField("Write_Disk_Bytes",DoubleType),
      StructField("Read_Time",IntegerType),
      StructField("Write_Time",IntegerType),
      StructField("I/O_Time",IntegerType),
      StructField("Bytes_Sent",DoubleType),
      StructField("Bytes_Received",DoubleType),
      StructField("Packets_Sent",IntegerType),
      StructField("Packets_Received",IntegerType),
      StructField("Errors_While_Sending",IntegerType),
      StructField("Errors_While_Receiving",IntegerType),
      StructField("Incoming_Packets_Dropped",IntegerType),
      StructField("Outgoing_Packets_Dropped",IntegerType),
      StructField("Boot_Time",StringType),
      StructField("User_Name",StringType),
      StructField("Keyboard",DoubleType),
      StructField("Mouse",DoubleType),
      StructField("Technologies",StringType),
      StructField("Files_Changed",IntegerType)
    )
  )

  val prePath = "src/resources/day ("
  val postPath=").csv"

  val logDF1 = spark.read
    .format("csv")
    .option("header", true)
    .schema(logDfSchema)
    .load("src/resources/day (1).csv")

  val logDF1 = getDfFromCsv(prePath+"1"+postPath,spark,logDfSchema)
  val logDF2 = getDfFromCsv(prePath+"2"+postPath,spark,logDfSchema)

  val logDF3 = spark.read
    .format("csv")
    .option("header", true)
    .schema(logDfSchema)
    .load("src/resources/day (3).csv")

  val logDF4 = spark.read
    .format("csv")
    .option("header", true)
    .schema(logDfSchema)
    .load("src/resources/day (4).csv")

  val logDF5 = spark.read
    .format("csv")
    .option("header", true)
    .schema(logDfSchema)
    .load("src/resources/day (5).csv")

  val logDF6 = spark.read
    .format("csv")
    .option("header", true)
    .schema(logDfSchema)
    .load("src/resources/day (6).csv")

  val logDF7 = spark.read
    .format("csv")
    .option("header", true)
    .schema(logDfSchema)
    .load("src/resources/day (7).csv")

  val logDF8 = spark.read
    .format("csv")
    .option("header", true)
    .schema(logDfSchema)
    .load("src/resources/day (8).csv")


  val logDF9 = spark.read
    .format("csv")
    .option("header", true)
    .schema(logDfSchema)
    .load("src/resources/day (9).csv")

  val logDF = logDF1.union(logDF2).union(logDF3).union(logDF4).union(logDF5).union(logDF6).union(logDF7).union(logDF8).union(logDF9)
  import spark.implicits._

    val idleUserDetails = logDF.select(
      logDF.col("User_Name"),
      logDF.col("Cpu_Idle_Time")
    )


  val highestIdleUser = idleUserDetails.groupBy(col("User_Name"))
  .sum("Cpu_Idle_Time").withColumnRenamed("sum(Cpu_Idle_Time)","Cpu_Idle_Time")
    .orderBy(col("Cpu_Idle_Time").desc)
    .show(1)

  def getDfFromCsv(path:String,spark:SparkSession,strct:StructType): DataFrame ={
    val DF = spark.read
      .format("csv")
      .option("header", true)
      .schema(strct)
      .load(path)
    DF
  }
}

