import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StructField, StructType, TimestampType}


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
      .option("startingOffsets", "earliest")
      .option("subscribe", "security")
      .schema(mySchema)
      .load()


    val query = fileStreamDf.writeStream
      .format("csv")
      .option("checkpointLocation", "checkpoint/")
      .start("output/")
      .awaitTermination()


  }
}
