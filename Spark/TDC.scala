import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql._
import com.datastax.spark.connector._

object TDC {
  def main(args: Array[String]): Unit = {

    val spark=SparkSession.builder()
      .master("local[4]")
      .appName("Distance System")
      .config("spark.cassandra.connection.host","localhost")
      .config("spark.cassandra.connection.port","9042")
      .getOrCreate()

    val sc=spark.sparkContext
    sc.setLogLevel("ERROR")


    val start_df=spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers","localhost:9092")
      .option("subscribe","start-point")
      .load()

    val schema=StructType(Array(
      StructField("car_id",StringType),
      StructField("time",TimestampType)
    ))

    val start_value=start_df.selectExpr("CAST(value AS STRING)")
    val start_data=start_value.select(from_json(col("value"),schema).as("data")).select("data.*")
    val start_data_new=start_data.withColumnRenamed("car_id","start_id")
      .withColumnRenamed("time","start_time")

    val stop_df=spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers","localhost:9092")
      .option("subscribe","stop-point")
      .load()

    val stop_value=stop_df.selectExpr("CAST(value AS STRING)")
    val stop_data=stop_value.select(from_json(col("value"),schema).as("data")).select("data.*")
    val stop_data_new=stop_data.withColumnRenamed("car_id","stop_id")
      .withColumnRenamed("time","stop_time")

    //Join this data frame
    val total_data=start_data_new.join(stop_data_new,start_data_new("start_id")===stop_data_new("stop_id"),"inner")

    //Step1:Calculate time difference
    val total_data1=total_data.withColumn("time_diference",unix_timestamp(col("stop_time"))-unix_timestamp(col("start_time")))

    //Step2:Filter data
    val total_data2=total_data1.where(col("time_diference")>0 and col("time_diference")<180)

    //Step3:Clear data
    val total_data3=total_data2.select(col("time_diference"),col("start_id"),col("start_time"),col("stop_time"))
      .withColumnRenamed("start_id","car_id")
      .withColumn("time_diference",lit(col("time_diference")))


    val query=total_data3.writeStream
      .foreachBatch { (batchDF: DataFrame, _: Long) =>
        batchDF.write
          .cassandraFormat("road1", "tdc")
          .mode("append")
          .save()
      }
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .outputMode("append")
      .start()



    val output=total_data3.writeStream
      .format("console")
      .outputMode("append")
      .option("truncate","false")
      .start()
      .awaitTermination()



  }

}
