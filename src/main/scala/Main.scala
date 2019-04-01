import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, TimestampType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

case class InputRow(SECURITYID:String,
                    ts:java.sql.Timestamp,
                    STRATEGY1:java.lang.Double,
                    SPREAD1: java.lang.Double,
                    STRATEGY2:java.lang.Double,
                    SPREAD2: java.lang.Double,
                    STRATEGY3:java.lang.Double,
                    SPREAD3: java.lang.Double)

case class StrategyState(SECURITYID:String,
                         var windowStart: java.sql.Timestamp,
                         var windowEnd: java.sql.Timestamp,
                         var windowMean1:java.lang.Double,
                         var windowStd1:java.lang.Double,
                         var windowMedian1:java.lang.Double,
                         var windowMad1:java.lang.Double,
                         var windowList1:List[InputRow],
                         var s1Count:java.lang.Integer,
                         var totalCount:java.lang.Integer
                         )

case class StrategyOutput(SECURITYID:String,
                          var ts: java.sql.Timestamp,
                          var rank: List[String],
                          var S1_uptimePct: Double,
                          var S1_alert1:Option[Boolean],
                          var S1_alert2:Option[Boolean]
                         )

object StrategyTracking {
  final val BOOTSTRAPSERVERS = "127.0.0.1:9092"

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("Strategy Tracking")
      .master("local")
      .getOrCreate()
    import spark.implicits._

    //Read raw data from Kafka topic
    val fileStreamDf = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", BOOTSTRAPSERVERS)
      .option("startingOffsets", "latest")
      .option("subscribe", "security")
      .load()

    //Cast value column to string and reconstruct the dataframe
    val df=fileStreamDf.select(col("value").cast("string"))

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
      .withColumn("SECURITYID", lit("Demo"))


    def updateStateWithEvent(state:StrategyState, input:InputRow):StrategyState = {
      // no timestamp, just ignore it
      if (Option(input.ts).isEmpty) {
        return state
      }

      state.totalCount+=1

      if (!Option(input.STRATEGY1).isEmpty) {
        state.s1Count+=1
        state.windowList1 = state.windowList1 :+ input
        state.windowList1 = state.windowList1.filter(_.ts.after(new Timestamp(input.ts.getTime - 5 * 60 * 1000)))
        state.windowStart = state.windowList1.head.ts
        state.windowEnd = state.windowList1.last.ts
        state.windowMean1 = state.windowList1.map(_.SPREAD1.toDouble).sum/state.windowList1.length
        state.windowStd1 = state.windowList1.map(_.SPREAD1.toDouble - state.windowMean1).map(x=>x*2).sum/state.windowList1.length
        if (state.windowList1.length%2==1) {
          state.windowMedian1 = state.windowList1.map(_.SPREAD1.toDouble).sortWith(_ < _)(state.windowList1.length/2)
        } else {
          state.windowMedian1 = (state.windowList1.map(_.SPREAD1.toDouble).sortWith(_ < _)(state.windowList1.length/2-1) +
            state.windowList1.map(_.SPREAD1.toDouble).sortWith(_ < _)(state.windowList1.length/2))/2
        }
        state.windowMad1 = state.windowList1.map(_.SPREAD1.toDouble - state.windowMean1).map(x=>Math.abs(x)).sum/state.windowList1.length
      }

      //return the updated state
      state
    }

    import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode, GroupState}

    def updateAcrossEvents(SECURITYID:String,
                           inputs: Iterator[InputRow],
                           oldState: GroupState[StrategyState]):StrategyOutput = {
      var state:StrategyState = if (oldState.exists) oldState.get else StrategyState(SECURITYID,
        null, null, 0.0, 0.0, 0.0, 0.0, List[InputRow](), 0, 0
      )

      var output = StrategyOutput(null, null, List[String](), 0.0, None, None)

      for (input <- inputs) {
        state = updateStateWithEvent(state, input)
        oldState.update(state)

        val rank = List(("s1",input.STRATEGY1),("s2",input.STRATEGY2),("s3",input.STRATEGY3))
          .sortWith(_._2!= null && _._2 == null).map(m => m._1)

        val uptimePct1 = state.windowList1.length.toDouble/state.totalCount

        if (Option(input.STRATEGY1).isEmpty) {
          output = StrategyOutput(state.SECURITYID, input.ts, rank, uptimePct1, None, None)
        } else {
          val alert1 = if (Math.abs(input.SPREAD1 - state.windowMean1) / state.windowStd1>3.5) Some(true) else Some(false)
          val alert2 = if (Math.abs(input.SPREAD1 - state.windowMedian1) / state.windowMad1>3.5) Some(true) else Some(false)
          output = StrategyOutput(state.SECURITYID, input.ts, rank, uptimePct1, alert1, alert2)
        }
      }

      return output

    }

    val df_strategy1 = df_structured.select(
      col("ts"),
      col("SECURITYID"),
      col("STRATEGY1"),
      col("SPREAD1"),
      col("STRATEGY2"),
      col("SPREAD2"),
      col("STRATEGY3"),
      col("SPREAD3"),
    )


    val consoleOutput = df_strategy1.as[InputRow]
      .groupByKey(_.SECURITYID)
      .mapGroupsWithState(GroupStateTimeout.NoTimeout())(updateAcrossEvents)
      .writeStream
//      .format("parquet")
      .trigger(Trigger.ProcessingTime("5 seconds"))
//      .format("csv")
      .outputMode("update")
      .option("checkpointLocation", "checkpoint/")
      .option("truncate", "false")
      //      .start("output/")
      .queryName("events_per_window")
      .format("console")
      .start("output/")
      .awaitTermination()



  }
}
