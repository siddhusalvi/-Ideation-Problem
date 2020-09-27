import java.io.{File, FileWriter}
import java.util.Properties
import org.apache.spark.sql
import org.apache.spark.sql.functions.{col, from_json, _}
import org.apache.spark.sql.types.{DataTypes, StructType, _}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

//class to import csv files
case class Data(DateTime: String,
                Cpu_Count: Int,
                Cpu_Working_Time: Double,
                Cpu_Idle_Time: Double,
                Cpu_Percent: Double,
                Usage_Cpu_Count: Int,
                Software_interrupts: Int,
                System_calls: Int,
                Interrupts: Int,
                Load_Time_1_min: Double,
                Load_Time_5_min: Double,
                Load_Time_15_min: Double,
                Total_Memory: Double,
                Used_Memory: Double,
                Free_Memory: Double,
                Active_Memory: Double,
                Inactive_Memory: Double,
                Bufferd_Memory: Double,
                Cache_Memory: Double,
                Shared_Memory: Double,
                Available_Memory: Double,
                Total_Disk_Memory: Double,
                Used_Disk_Memory: Double,
                Free_Disk_Memory: Double,
                Read_Disk_Count: Int,
                Write_Disk_Count: Int,
                Read_Disk_Bytes: Double,
                Write_Disk_Bytes: Double,
                Read_Time: Int,
                Write_Time: Int,
                IO_Time: Int,
                Bytes_Sent: Double,
                Bytes_Received: Double,
                Packets_Sent: Int,
                Packets_Received: Int,
                Errors_While_Sending: Int,
                Errors_While_Receiving: Int,
                Incoming_Packets_Dropped: Int,
                Outgoing_Packets_Dropped: Int,
                Boot_Time: String,
                User_Name: String,
                Keyboard: Double,
                Mouse: Double,
                Technologies: String,
                Files_Changed: Int
               )

object SqlApp {
  def main(args: Array[String]): Unit = {
    try {
      val spark = SparkSession.builder()
        .config("spark.master", "local[2]")
        .appName("SqlApp")
        .getOrCreate()

      val tablename = "work"
      val sc = spark.sparkContext
      val logDfSchema = getDfSchema()
      val dir = "src\\resources\\"
      val path = dir + "userWorkData.csv"
      val prePath = "src/resources/day ("
      val postPath = ").csv"

      //Getting Single csv data in dataframe
      val userData = getDfFromCsv(prePath + "1" + postPath, spark, logDfSchema)
      userData.show()

      val anotherDF = userData.select(col("DateTime"), col("User_Name"), col("Keyboard"), col("Mouse"))

      val leavesOfUser = getLeaves(spark)
      leavesOfUser.show()

      val logDF = getAllData(spark)

      val highestIdleUser = getHighestIdleUser(userData)
      highestIdleUser.show()

      val lowestWorikingUser = getLowestWorkingUser(logDF)
      lowestWorikingUser.show()

      val lateUsers = getLateComingUser(spark)
      lateUsers.show()

      val getWorkOfUser = getUserWorkData(userData, path, spark)
      getWorkOfUser.show()

    } catch {
      case exception1: ClassNotFoundException => println(exception1)
      case exception2: sql.AnalysisException => println(exception2)
      case exception => println(exception)
    }
  }

  //Function to print Stream
  def printStreamDF(dataFrame: DataFrame): Unit = {
    dataFrame.writeStream
      .outputMode("append")
      .format("console")
      .start()
  }

  //Function to get stream config
  def getStreamDF(topic: String, port: Int, spark: SparkSession): DataFrame = {
    import spark.implicits._
    val schema = getStructuredSchema()
    val stream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:" + port.toString)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest") // going to replay from the beginning each time
      .load()
      .selectExpr("CAST(value AS STRING)")
      .select(from_json($"value", schema).as("Df"))

    stream
  }

  //Function to get schema for Structured streaming
  def getStructuredSchema(): StructType = {
    val schema = new StructType()
      .add("DateTime", DataTypes.TimestampType) //===================
      .add("Cpu_Count", DataTypes.IntegerType)
      .add("Cpu_Working_Time", DataTypes.DoubleType)
      .add("Cpu_Idle_Time", DataTypes.DoubleType)
      .add("Cpu_Percent", DataTypes.DoubleType)
      .add("Usage_Cpu_Count", DataTypes.IntegerType)
      .add("Software_Interrupts", DataTypes.IntegerType)
      .add("System_calls", DataTypes.IntegerType)
      .add("Interrupts", DataTypes.IntegerType)
      .add("Load_Time_1_min", DataTypes.DoubleType)
      .add("Load_Time_5_min", DataTypes.DoubleType)
      .add("Load_Time_15_min", DataTypes.DoubleType)
      .add("Total_Memory", DataTypes.DoubleType)
      .add("Used_Memory", DataTypes.DoubleType)
      .add("Free_Memory", DataTypes.DoubleType)
      .add("Active_Memory", DataTypes.DoubleType)
      .add("Inactive_Memory", DataTypes.DoubleType)
      .add("Bufferd_Memory", DataTypes.DoubleType)
      .add("Cache_Memory", DataTypes.DoubleType)
      .add("Shared_Memory", DataTypes.DoubleType)
      .add("Available_Memory", DataTypes.DoubleType)
      .add("Total_Disk_Memory", DataTypes.DoubleType)
      .add("Used_Disk_Memory", DataTypes.DoubleType)
      .add("Free_Disk_Memory", DataTypes.DoubleType)
      .add("Read_Disk_Count", DataTypes.IntegerType)
      .add("Write_Disk_Count", DataTypes.IntegerType)
      .add("Read_Disk_Bytes", DataTypes.DoubleType)
      .add("Write_Disk_Bytes", DataTypes.DoubleType)
      .add("Read_Time", DataTypes.IntegerType)
      .add("Write_Time", DataTypes.IntegerType)
      .add("I/O_Time", DataTypes.IntegerType)
      .add("Bytes_Sent", DataTypes.DoubleType)
      .add("Bytes_Received", DataTypes.DoubleType)
      .add("Packets_Sent", DataTypes.IntegerType)
      .add("Packets_Received", DataTypes.IntegerType)
      .add("Errors_While_Sending", DataTypes.IntegerType)
      .add("Errors_While_Receiving", DataTypes.IntegerType)
      .add("Incoming_Packets_Dropped", DataTypes.IntegerType)
      .add("Outgoing_Packets_Dropped", DataTypes.IntegerType)
      .add("Boot_Time", DataTypes.StringType)
      .add("User_Name", DataTypes.StringType)
      .add("Keyboard", DataTypes.DoubleType)
      .add("Mouse", DataTypes.DoubleType)
      .add("Technologies", DataTypes.StringType)
      .add("Files_Changed", DataTypes.IntegerType)
    schema
  }

  //Function to get idle time, working time
  def getUserWorkData(Df: DataFrame, path: String, spark: SparkSession): DataFrame = {

    val userIdleData = getIdleTime(Df, path, spark)
    val userEntries = getUserEntries(Df)
    val df = userIdleData.join(userEntries, userIdleData.col("ID") === userEntries.col("User_Name"))

    val userData = df.drop(col("User_Name"))
      .withColumn("WorkTime",
        floor(col("OfficeTime").cast(IntegerType) / 60) + col("OfficeTime") % 60 * .01
      )
      .withColumn("WorkingHours",
        floor((col("OfficeTime") - col("IdealTime")) / 60) + (col("OfficeTime") - col("IdealTime")) % 60 * .01
      )
      .withColumn("IdleTime",
        (floor(col("IdealTime") / 60) + col("IdealTime") % 60 * .01).cast(DataTypes.createDecimalType(32, 2))
      )
      .withColumnRenamed("ID", "UserName")
      .drop("Count", "OfficeTime")
    userData
  }

  //Function to get idle time of users
  def getIdleTime(givenData: DataFrame, path: String, spark: SparkSession): DataFrame = {
    val writer = new FileWriter(path, false)
    val newDF = givenData
      .withColumn("Keyboard", col("keyboard").cast(IntegerType))
      .withColumn("Mouse", col("mouse").cast(IntegerType))
      .withColumn("User", col("User_Name"))
      .orderBy(col("User"))
      .select(col("User"), col("keyboard"), col("mouse"), col("DateTime"))
      .orderBy(col("User"), col("DateTime")).collect()

    val sc = spark.sparkContext
    val counter = sc.longAccumulator
    var name: String = ""
    var flag = true
    for (row <- newDF) {
      var data = row.toSeq
      if ((data(1).toString.toInt + data(2).toString.toInt) == 0) {
        if (flag) {
          name = data(0).toString
          flag = false
          counter.reset()
          counter.add(1)
        } else if (data(0).toString.equals(name)) {
          counter.add(1)
          name = data(0).toString
        } else {
          //          if (counter.value > 5) {
          //            var str = name + "," + counter.value + "\n"
          //            writer.write(str)
          //          }
          name = data(0).toString
          flag = false
          counter.reset()
          counter.add(1)
        }
      } else {
        if (counter.value > 5) {
          var str = name + "," + counter.value + "\n"
          writer.write(str)
        }
        counter.reset()
        flag = true
      }
    }
    writer.close()

    val schema = StructType(
      Array(
        StructField("ID", StringType),
        StructField("Time", IntegerType)
      )
    )

    val userData = spark.read
      .format("csv")
      .schema(schema)
      .load(path)


    val idleUsers = userData.groupBy(col("ID")).sum("Time")

    val idleTime = idleUsers.withColumnRenamed("sum(Time)", "Count")
      .withColumn("IdealTime", col("Count") * 5)
      .orderBy(col("IdealTime"))

    idleTime
  }

  //function to calculate working duration
  def getUserEntries(DF: DataFrame): DataFrame = {
    val arrivalDF = getArrivalTime(DF).withColumnRenamed("DateTime", "Arrival")
    val leavingDF = getDepartureTime(DF).withColumnRenamed("DateTime", "Departure")

    val userEntries = arrivalDF.join(leavingDF, arrivalDF.col("User_Name") === leavingDF.col("User_Name"))
      .drop(leavingDF.col("User_Name"))

    val workCalculation = userEntries
      .withColumn("ArrivalTime", date_format(col("Arrival"), "KK:HH:mm a"))
      .withColumn("LeavingTime", date_format(col("Departure"), "KK:HH:mm a"))
      .withColumn("OfficeTime",
        date_format(col("Departure"), "HH").cast(IntegerType) * 60 + date_format(col("Departure"), "mm").cast(IntegerType) -
          date_format(col("Arrival"), "HH").cast(IntegerType) * 60 + date_format(col("Arrival"), "mm").cast(IntegerType)
      )
      .drop("Arrival", "Departure")
    workCalculation
  }

  //Function to get leaving time of user
  def getDepartureTime(df: DataFrame): DataFrame = {
    val newDf = df.orderBy(col("DateTime") desc).select(
      df.col("User_Name"),
      df.col("DateTime")
    ).dropDuplicates(Array("User_Name")).orderBy("DateTime")
    newDf
  }

  //Function to store data in database
  def saveToDB(DF: DataFrame, table: String): Unit = {
    val url = "jdbc:mysql://127.0.0.1:3306"
    val properties = new Properties()
    properties.put("user", "root")
    properties.put("password", System.getenv("dbpass"))
    Class.forName("com.mysql.jdbc.Driver")
    val dbTable = "ideation." + table
    DF.write.mode(SaveMode.Overwrite).jdbc(url, dbTable, properties)
  }

  //Function to get all data
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

  //Function to get highest idle user using CPU
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

  //Function to get lowest working user using CPU
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

  //Function to get late coming user
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

  //Function to get leaves of user
  def getLeaves(spark: SparkSession): DataFrame = {
    val logArrival = getArrivalData(spark)
    val leaves = logArrival.groupBy(col("User_Name"))
      .count()
    val highestLeavesUser = leaves.withColumn("Leaves", expr("9 - count")).drop("count").orderBy(col("Leaves").desc)
    highestLeavesUser
  }

  //Function to get arriving time of users
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

  //Function to get single user arrival
  def getArrivalTime(df: DataFrame): DataFrame = {
    val newDf = df.select(
      df.col("User_Name"),
      df.col("DateTime")
    ).dropDuplicates(Array("User_Name"))
    newDf
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
        StructField("DateTime", TimestampType), //===================
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

  //Function to get all files in directory
  def getListOfFiles(dir: File, extensions: List[String]): List[File] = {
    dir.listFiles.filter(_.isFile).toList.filter { file =>
      extensions.exists(file.getName.endsWith(_))
    }
  }

  //Function to save csv file
  def saveAsCsv(df: DataFrame, path: String): Unit = {
    df.coalesce(1)
      .write.option("header", "true").csv(path)
  }

}
