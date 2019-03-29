import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType, TimestampType}
import org.apache.spark.sql.functions._


object StrategyTracking {
  final val BOOTSTRAPSERVERS = "127.0.0.1:9092"

  def main(args: Array[String]): Unit = {

    val mySchema = StructType(Array(
      StructField("ts", TimestampType),
      StructField("REFERENCE_PRICE", DoubleType),
      StructField("STRATEGY1", DoubleType),
      StructField("STRATEGY2", DoubleType),
      StructField("STRATEGY3", DoubleType)
    ))

    val spark = SparkSession
      .builder
      .appName("Strategy Tracking")
      .master("local")
      .getOrCreate()

    val fileStreamDf = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", BOOTSTRAPSERVERS)
      .option("startingOffsets", "latest")
      .option("subscribe", "security")
      .load()

    val df=fileStreamDf.select(col("value").cast("string")) .alias("csv").select("csv.*")
    val df_structured=df.selectExpr(
      "split(value,',')[0] as ts"
      ,"split(value,',')[1] as REFERENCE_PRICE"
      ,"split(value,',')[2] as STRATEGY1"
      ,"split(value,',')[3] as STRATEGY2"
      ,"split(value,',')[3] as STRATEGY3")
      .select(col("ts").cast(TimestampType),
        col("REFERENCE_PRICE").cast(DoubleType),
        col("STRATEGY1").cast(DoubleType),
        col("STRATEGY2").cast(DoubleType),
        col("STRATEGY3").cast(DoubleType)
      ).withColumn("SPREAD1", col("STRATEGY1")-col("REFERENCE_PRICE"))
      .withColumn("SPREAD2", col("STRATEGY2")-col("REFERENCE_PRICE"))
      .withColumn("SPREAD3", col("STRATEGY3")-col("REFERENCE_PRICE"))

    val rollingDF = df_structured.withWatermark("ts","5 seconds")
      .groupBy(window(col("ts"), "5 seconds"))
      .agg(avg("SPREAD1") as "mean").
      select("window.start", "window.end", "mean")



    val consoleOutput = rollingDF.writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", "false")
        .start()
    consoleOutput.awaitTermination()


  }
}
