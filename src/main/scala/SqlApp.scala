import java.io.{File, FileWriter}
import java.util.Properties
import org.apache.spark.sql.functions.from_json
import org.apache.spark.network.protocol.Encoders
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

import scala.concurrent.duration._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession



import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructType}





object SqlApp {
  def main(args: Array[String]): Unit = {
//    try {
      val spark = SparkSession.builder()
        .config("spark.master", "local[2]")
        .appName("SqlApp")
        .getOrCreate()





//      val tablename = "work"
//      val sc = spark.sparkContext
//      val logDfSchema = getDfSchema()
//      val dir = "src\\resources\\"
//      val path = dir + "userWorkData.csv"
//      val prePath = "src/resources/day ("
//      val postPath = ").csv"


//
//    val schema = StructType(
//      Array(
//        StructField("DateTime", StringType),//===================
//        StructField("Cpu_Count", IntegerType),
//        StructField("Cpu_Working_Time", DoubleType),
//        StructField("Cpu_Idle_Time", DoubleType),
//        StructField("Cpu_Percent", DoubleType),
//        StructField("Usage_Cpu_Count", IntegerType),
//        StructField("Software_Interrupts", IntegerType),
//        StructField("System_calls", IntegerType),
//        StructField("Interrupts", IntegerType),
//        StructField("Load_Time_1_min", DoubleType),
//        StructField("Load_Time_5_min", DoubleType),
//        StructField("Load_Time_15_min", DoubleType),
//        StructField("Total_Memory", DoubleType),
//        StructField("Used_Memory", DoubleType),
//        StructField("Free_Memory", DoubleType),
//        StructField("Active_Memory", DoubleType),
//        StructField("Inactive_Memory", DoubleType),
//        StructField("Bufferd_Memory", DoubleType),
//        StructField("Cache_Memory", DoubleType),
//        StructField("Shared_Memory", DoubleType),
//        StructField("Available_Memory", DoubleType),
//        StructField("Total_Disk_Memory", DoubleType),
//        StructField("Used_Disk_Memory", DoubleType),
//        StructField("Free_Disk_Memory", DoubleType),
//        StructField("Read_Disk_Count", IntegerType),
//        StructField("Write_Disk_Count", IntegerType),
//        StructField("Read_Disk_Bytes", DoubleType),
//        StructField("Write_Disk_Bytes", DoubleType),
//        StructField("Read_Time", IntegerType),
//        StructField("Write_Time", IntegerType),
//        StructField("I/O_Time", IntegerType),
//        StructField("Bytes_Sent", DoubleType),
//        StructField("Bytes_Received", DoubleType),
//        StructField("Packets_Sent", IntegerType),
//        StructField("Packets_Received", IntegerType),
//        StructField("Errors_While_Sending", IntegerType),
//        StructField("Errors_While_Receiving", IntegerType),
//        StructField("Incoming_Packets_Dropped", IntegerType),
//        StructField("Outgoing_Packets_Dropped", IntegerType),
//        StructField("Boot_Time", StringType),
//        StructField("User_Name", StringType),
//        StructField("Keyboard", DoubleType),
//        StructField("Mouse", DoubleType),
//        StructField("Technologies", StringType),
//        StructField("Files_Changed", IntegerType)
//      )
//    )
//
//    val schema = StructType(
//      List(
//        StructField("name", DataTypes.StringType),//===================
//        StructField("Count",  DataTypes.IntegerType)
//      ))
//
//
//









    import spark.implicits._



//    val inputDf = spark.readStream
//      .format("kafka")
//      .option("kafka.bootstrap.servers", "localhost:9092")
//      .option("subscribe", "csv")
//      .option("startingOffsets", "earliest") // going to replay from the beginning each time
//      .load()
//
//    println("CSV")
//    val csvDF = inputDf.selectExpr( "CAST(value AS STRING)")
//
//    csvDF.writeStream
//      .outputMode("append")
//      .format("console")
//      .start()
//
//
//
//    val inputDf = spark.readStream
//      .option("multiline", "true")
//      .format("kafka")
//      .option("kafka.bootstrap.servers", "localhost:9092")
//      .option("subscribe", "json")
//      .load()
//      .selectExpr("CAST(value AS STRING)")
//      .select(from_json($"value", schema).as("Df"))
//
//
//    val selectDf = inputDf.select("*")
//    selectDf.writeStream
//      .outputMode("append")
//      .format("console")
//      .start()
//
//    spark.streams.awaitAnyTermination()



    val schema = new StructType()
      .add("id", DataTypes.StringType)
      .add("count", DataTypes.IntegerType)






    val inputDf = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "user5")
      .option("startingOffsets", "earliest") // going to replay from the beginning each time
      .load()
      .selectExpr("CAST(value AS STRING)")
      .select(from_json($"value", schema).as("Df"))


        val selectDf = inputDf.selectExpr("Df.*")
        selectDf.writeStream
          .outputMode("append")
          .format("console")
          .start()





//    val selectDf = inputDf.selectExpr("Df.Mouse","Df.User_Name")
//    selectDf.writeStream
//      .outputMode("append")
//      .format("console")
//      .start()

//    spark.streams.awaitAnyTermination()










    //    val path = "src/resources/data.json"
//    val peopleDF = spark.read.option("multiline", "true")
    //      .json(path)
//
//    // The inferred schema can be visualized using the printSchema() method
//    peopleDF.printSchema()
//    peopleDF.show()
//









//
//        val stream = spark.readStream.format("kafka")
//      .option("kafka.bootstrap.servers", "localhost:9092")
//      .option("subscribe", "Producer")
//      .load()



//     val df =spark.readStream
//      .format("kafka")
//      .option("kafka.bootstrap.servers", "localhost:9092")
//      .option("subscribe", "json")
//      .load()
//
//
//
//    df.foreach(println(_)
//    )


    //    import spark.implicits._
//      val ad = df.selectExpr( "CAST(value AS STRING)")
//        .as[(String)]
//          .writeStream
////          .outputMode("complete")
//          .format("console")
//          .start()
//          .awaitTermination()

//    df.select(from_json($"value".cast("string")))
//      df.select(from_json($"value".cast("string"), schema).alias("value"))
//      .filter(...)  // Add the condition
//    .select(to_json($"value").alias("value")
//      // Write back
//      .writeStream
//      .format("kafka")
//      .option("kafka.bootstrap.servers", outservers)
//      .option("subscribe", outtopic)
//      .start()



    //    val stream = spark.readStream.format("kafka")
    //      .option("kafka.bootstrap.servers", "localhost:9092")
    //      .option("subscribe", "Producer")
    //      .load()




    //    import spark.implicits._
//    val words = stream.selectExpr("CAST value AS STRING")
//      .as(Encoders.STRING())
//    // Generate running word count
//    val wordCounts = words.groupBy("value").count()
//
//    val query = wordCounts.writeStream
//      .outputMode("complete")
//      .format("console")
//      .start()
//
//    query.awaitTermination()
//

//
//    val df = stream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
//      .writeStream
//      .outputMode("complete")
//      .format("console")
//      .start()
//      .awaitTermination()

//    val df = spark
//      .readStream
//      .format("kafka")
//      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
//      .option("subscribe", "topic1")
//      .load()
//
//    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
//      .as[(String, String)]
//


//============================================================
//    val ds = stream
//      .writeStream
//      .format("console")
//      .option("checkpointLocation", "src/resources/checkpoints")
//      .start()
//    ds.awaitTermination()




    //      import spark.implicits._
//      val kafka = spark.readStream
//        .format("kafka")
//        .schema(logDfSchema)
//        .option("bootstrap.servers", "localhost:9092")
//        .option("subscribe","Producer")
//        .option("startingOffsets", "latest")
//        .load()
//
//      kafka.show()

//      val logDF = getDfFromCsv(prePath + "5" + postPath, spark, logDfSchema)
//      val workOfUser = getUserWorkData(logDF, path, spark).orderBy(col("ArrivalTime"))






//    } catch {
//      case exception => println(exception)
//    }

    spark.streams.awaitAnyTermination()

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
      .orderBy(col("IdealTime") desc)

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
        StructField("DateTime", TimestampType),//===================
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

  //Function to get single user arrival
  def getArrivalTime(df: DataFrame): DataFrame = {
    val newDf = df.select(
      df.col("User_Name"),
      df.col("DateTime")
    ).dropDuplicates(Array("User_Name"))
    newDf
  }

  //Function to get leaves of user
  def getLeaves(spark: SparkSession): DataFrame = {
    val logArrival = getArrivalData(spark)
    val leaves = logArrival.groupBy(col("User_Name"))
      .count()
    val highestLeavesUser = leaves.withColumn("Leaves", expr("9 - count")).drop("count").orderBy(col("Leaves").desc)
    highestLeavesUser
  }

  def getListOfFiles(dir: File, extensions: List[String]): List[File] = {
    dir.listFiles.filter(_.isFile).toList.filter { file =>
      extensions.exists(file.getName.endsWith(_))
    }
  }

}
