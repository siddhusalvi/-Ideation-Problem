import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

object SqlApp extends App {
  try {
    val spark = SparkSession.builder()
      .config("spark.master", "local")
      .appName("SqlApp")
      .getOrCreate()


    val logDfSchema = getDfSchema()
    val prePath = "src/resources/day ("
    val postPath = ").csv"

    val logDF = getDfFromCsv(prePath + "1" + postPath, spark, logDfSchema)
//    logDF.show()

    val Df = logDF
      .withColumn("Keyboard", col("keyboard").cast(IntegerType))
      .withColumn("Mouse", col("mouse").cast(IntegerType))
      .withColumn("User",col("User_Name"))
      .orderBy(col("User"))
      .select(col("User"),col("keyboard"),col("mouse"),col("DateTime"))
      .orderBy(col("User"),col("DateTime"))

  Df.show(300)
    var flag = true
    var name:String = ""
    var counter = 0
    var output:String = ""
    Df.foreach { row =>
      var data = row.toSeq
      if((data(1).toString.toInt + data(2).toString.toInt)==0){
        println("Idle : "+data(0)+" "+counter+" "+data(3))
        if(flag){
          name = data(0).toString
          flag = false
          counter = 1
          println(name+" is added 1st time "+counter+" "+data(3))
        }else if(data(0).toString.equals(name)){
          counter +=1
          name = data(0).toString
          println(name+" is added "+counter+" "+data(3))
        }else{
          name = data(0).toString
          flag = false
          counter = 1
          println(name+" is added 1st time at "+counter+" "+data(3))
        }
      }else{
        if(counter > 5){
          println(name+" was idle for "+(counter*5).toString+" "+data(3).toString+"\n")
        }
        counter = 0
        flag = true

      }
    }


  } catch {
    case exception => println(exception)
  }

  def getAllData(spark: SparkSession): DataFrame = {
    val logDfSchema = getDfSchema()
    val prePath = "src/resources/day ("
    val postPath = ").csv"

    val logDF1 = getDfFromCsv(prePath + "1" + postPath, spark, logDfSchema)
    val logDF2 = getDfFromCsv(prePath + "2" + postPath, spark, logDfSchema)
    val logDF3 = getDfFromCsv(prePath + "3" + postPath, spark, logDfSchema)
    val logDF4 = getDfFromCsv(prePath + "4" + postPath, spark, logDfSchema)
    val logDF5 = getDfFromCsv(prePath + "5" + postPath, spark, logDfSchema)
    val logDF6 = getDfFromCsv(prePath + "6" + postPath, spark, logDfSchema)
    val logDF7 = getDfFromCsv(prePath + "7" + postPath, spark, logDfSchema)
    val logDF8 = getDfFromCsv(prePath + "8" + postPath, spark, logDfSchema)
    val logDF9 = getDfFromCsv(prePath + "9" + postPath, spark, logDfSchema)

    val logDF = logDF1.union(logDF2).union(logDF3).union(logDF4).union(logDF5).union(logDF6).union(logDF7).union(logDF8).union(logDF9)
    logDF
  }

  //Function to get csv dataframe
  def getDfFromCsv(path: String, spark: SparkSession, strct: StructType): DataFrame = {
    val DF = spark.read
      .format("csv")
      .option("header", true)
      .option("timestampFormat", "MM/dd/yyyy HH:mm:ss.SSSSSS")
      .schema(strct)
      .load(path)
    DF
  }

  //Function to get dataframe schema
  def getDfSchema(): StructType = {
    val schema = StructType(
      Array(
        StructField("DateTime", TimestampType),
        StructField("Cpu_Count", IntegerType),
        StructField("Cpu_Working_Time", DoubleType),
        StructField("Cpu_Idle_Time", DoubleType),
        StructField("Cpu_Percent", DoubleType),
        StructField("Usage_Cpu_Count", IntegerType),
        StructField("Software_Interrupts", IntegerType),
        StructField("System_calls", IntegerType),
        StructField("Interrupts", IntegerType),
        StructField("Load_Time_1_min", DoubleType),
        StructField("Load_Time_5_min", DoubleType),
        StructField("Load_Time_15_min", DoubleType),
        StructField("Total_Memory", DoubleType),
        StructField("Used_Memory", DoubleType),
        StructField("Free_Memory", DoubleType),
        StructField("Active_Memory", DoubleType),
        StructField("Inactive_Memory", DoubleType),
        StructField("Bufferd_Memory", DoubleType),
        StructField("Cache_Memory", DoubleType),
        StructField("Shared_Memory", DoubleType),
        StructField("Available_Memory", DoubleType),
        StructField("Total_Disk_Memory", DoubleType),
        StructField("Used_Disk_Memory", DoubleType),
        StructField("Free_Disk_Memory", DoubleType),
        StructField("Read_Disk_Count", IntegerType),
        StructField("Write_Disk_Count", IntegerType),
        StructField("Read_Disk_Bytes", DoubleType),
        StructField("Write_Disk_Bytes", DoubleType),
        StructField("Read_Time", IntegerType),
        StructField("Write_Time", IntegerType),
        StructField("I/O_Time", IntegerType),
        StructField("Bytes_Sent", DoubleType),
        StructField("Bytes_Received", DoubleType),
        StructField("Packets_Sent", IntegerType),
        StructField("Packets_Received", IntegerType),
        StructField("Errors_While_Sending", IntegerType),
        StructField("Errors_While_Receiving", IntegerType),
        StructField("Incoming_Packets_Dropped", IntegerType),
        StructField("Outgoing_Packets_Dropped", IntegerType),
        StructField("Boot_Time", StringType),
        StructField("User_Name", StringType),
        StructField("Keyboard", DoubleType),
        StructField("Mouse", DoubleType),
        StructField("Technologies", StringType),
        StructField("Files_Changed", IntegerType)
      )
    )
    schema
  }

  //Function to get
  def getHighestIdleUser(logDF: DataFrame): DataFrame = {
    //Finding highest idle user
    val idleUserDetails = logDF.select(
      logDF.col("User_Name"),
      logDF.col("Cpu_Idle_Time")
    )

    val highestIdleUser = idleUserDetails.groupBy(col("User_Name"))
      .sum("Cpu_Idle_Time").withColumnRenamed("sum(Cpu_Idle_Time)", "Cpu_Idle_Time")
      .orderBy(col("Cpu_Idle_Time").desc)
      .withColumnRenamed("Cpu_Idle_Time", "highest_Idle_hours")
    highestIdleUser
  }

  def getLowestWorkingUser(logDF: DataFrame): DataFrame = {
    val workingUserDetails = logDF.select(
      logDF.col("User_Name"),
      logDF.col("Keyboard") + logDF.col("Mouse") as ("Working_Action")
    )


    val lowestWorkingUser = workingUserDetails.groupBy(col("User_Name"))
      .sum("Working_Action").withColumnRenamed("sum(Working_Action)", "Working_Action")
      .orderBy(col("Working_Action"))
      .withColumnRenamed("Working_Action", "lowest_Working_User")
    lowestWorkingUser
  }

  def getLateComingUser(spark: SparkSession): DataFrame = {
    val logDF = getArrivalData(spark)
    val timestamp = logDF.select(
      logDF.col("DateTime"),
      logDF.col("User_Name")
    )
      .orderBy("DateTime")
      .withColumn("Hour", date_format(col("DateTime"), "HH"))
      .withColumn("min", date_format(col("DateTime"), "mm"))
      .withColumn("Arrival", date_format(col("DateTime"), "HH:mm"))
      .withColumn("hr", col("Hour").cast(IntegerType))
      .withColumn("mn", col("min").cast(IntegerType))
      .withColumn("mns", col("hr") * 60 + col("mn"))
      .drop(col("Hour"))
      .drop(col("min"))
      .drop(col("hr"))
      .drop(col("mn"))
      .withColumn("lateMIN", col("mns") - 510)
      .drop(col("mns"))
      .orderBy(col("lateMIN").desc)

    val highestLateUser = timestamp.groupBy(col("User_Name"))
      .avg("LateMIN").withColumnRenamed("avg(LateMIN)", "LateMIN")
      .orderBy(col("LateMIN").desc)
      .withColumnRenamed("LateMIN", "highest_LateMIN")
    highestLateUser
  }

  def getArrivalData(spark: SparkSession): DataFrame = {
    val logDfSchema = getDfSchema()

    val prePath = "src/resources/day ("
    val postPath = ").csv"


    val logDF1 = getDfFromCsv(prePath + "1" + postPath, spark, logDfSchema)
    val logDF2 = getDfFromCsv(prePath + "2" + postPath, spark, logDfSchema)
    val logDF3 = getDfFromCsv(prePath + "3" + postPath, spark, logDfSchema)
    val logDF4 = getDfFromCsv(prePath + "4" + postPath, spark, logDfSchema)
    val logDF5 = getDfFromCsv(prePath + "5" + postPath, spark, logDfSchema)
    val logDF6 = getDfFromCsv(prePath + "6" + postPath, spark, logDfSchema)
    val logDF7 = getDfFromCsv(prePath + "7" + postPath, spark, logDfSchema)
    val logDF8 = getDfFromCsv(prePath + "8" + postPath, spark, logDfSchema)
    val logDF9 = getDfFromCsv(prePath + "9" + postPath, spark, logDfSchema)


    val log1Arrival = getArrivalTime(logDF1)
    val log2Arrival = getArrivalTime(logDF2)
    val log3Arrival = getArrivalTime(logDF3)
    val log4Arrival = getArrivalTime(logDF4)
    val log5Arrival = getArrivalTime(logDF5)
    val log6Arrival = getArrivalTime(logDF6)
    val log7Arrival = getArrivalTime(logDF7)
    val log8Arrival = getArrivalTime(logDF8)
    val log9Arrival = getArrivalTime(logDF9)

    val logArrival = log1Arrival.union(log2Arrival).union(log3Arrival).union(log4Arrival).union(log5Arrival).union(log6Arrival).union(log7Arrival).union(log8Arrival).union(log9Arrival)
    logArrival
  }

  def getArrivalTime(df: DataFrame): DataFrame = {
    val newDf = df.select(
      df.col("User_Name"),
      df.col("DateTime")
    ).dropDuplicates(Array("User_Name"))
    newDf
  }

  def getLeaves(spark: SparkSession): DataFrame = {
    val logArrival = getArrivalData(spark)
    val leaves = logArrival.groupBy(col("User_Name"))
      .count()
    val highestLeavesUser = leaves.withColumn("Leaves", expr("9 - count")).drop("count").orderBy(col("Leaves").desc)
    highestLeavesUser
  }

}
