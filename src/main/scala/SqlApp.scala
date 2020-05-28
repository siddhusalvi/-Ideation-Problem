import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{sum,max}
object SqlApp extends App{
  val spark = SparkSession.builder()
    .config("spark.master","local")
    .appName("SqlApp")
    .getOrCreate()

  //DateTime	Cpu Count	Cpu Working Time	Cpu idle Time	cpu_percent	Usage Cpu Count 	number of software
  // interrupts since boot	number of system calls since boot	number of interrupts since boot	cpu avg load over 1 min	cpu avg load over 5 min
  // cpu avg load over 15 min	system_total_memory	system_used_memory	system_free_memory	system_active_memory	system_inactive_memory
  // system_buffers_memory	system_cached_memory	system_shared_memory	system_avalible_memory	disk_total_memory	disk_used_memory
  // disk_free_memory	disk_read_count	disk_write_count	disk_read_bytes	disk_write_bytes	time spent reading from disk
  // time spent writing to disk	time spent doing actual I/Os	number of bytes sent	number of bytes received	number of packets sent
  // number of packets recived	total number of errors while receiving	total number of errors while sending
  // total number of incoming packets which were dropped	total number of outgoing packets which were dropped	boot_time
  // user_name	keyboard	mouse	technology	files_changed

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
  val logDF = spark.read
    .format("csv")
    .option("header", true)
    .schema(logDfSchema)
    .load("src/resources/day (1).csv")

  import spark.implicits._
//logDF.show()
  logDF.createOrReplaceTempView("df")
  val idle_user_details = spark.sql("select User_Name, Cpu_Idle_Time from Df")
//   user_details.show()

  idle_user_details.createOrReplaceTempView("Df")
  val sum_idle_time = spark.sql("select User_Name, SUM(Cpu_Idle_Time) as Cpu_Idle_Time from Df group by User_Name")
//  sum_idle_time.show()

  sum_idle_time.createOrReplaceTempView("Df")
  val max_idle_user = spark.sql("select User_Name, Cpu_Idle_Time from Df WHERE Cpu_Idle_Time = (SELECT MAX(Cpu_Idle_Time)from Df)")
  max_idle_user.show()

  logDF.createOrReplaceTempView("df")
  val lazy_user_details = spark.sql("SELECT User_Name, SUM(Cpu_working_Time) as Working_Time from Df group by User_Name")

  lazy_user_details.createOrReplaceTempView("df")
  val lazy_user = spark.sql("SELECT User_Name, Working_Time from Df WHERE Working_Time = (SELECT MIN(Working_Time) from DF)")
  lazy_user.show()



}
