import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import  org.apache.spark.sql.functions._

//Burada kendi atlas hesabımı gizlemek için url'i başka bir dosyadan çektim.
//Sizde atlas_url değişkeni yerine kendi atlas hesabınızın url'inizi veya başka bir mongodb url'sini kullana bilirsiniz.
import Atlas.mongo_url

/*
*insertmongo
*updatemongo
*deletemongo
* */

object mongodb_stream2 {
  def main(args: Array[String]): Unit = {

    //Spark session'u kuruyoruz.
    val spark=SparkSession.builder()
      .master("local[4]")
      .appName("MongoDb2")
      .getOrCreate()

    //Log kaydı olarak sadece hataları göstermesini istediğimiz için
    //Spark context'i oluşturup loglevel parametresini ERROR olarak atıyoruz.

    val sc=spark.sparkContext
    sc.setLogLevel("ERROR")

    // Verimiz için en uygun şemayı oluşturuyoruz.

    val mongo_schema = StructType(Array(
      StructField("documentKey", MapType(StringType,StringType)),
      StructField("clusterTime",TimestampType),
      StructField("fullDocument",
        StructType(Array(
          StructField("_id", StringType),
          StructField("level", IntegerType),
          StructField("score", IntegerType),
          StructField("nickname", StringType),
          StructField("item", MapType(StringType, IntegerType))
        ))),
      StructField("updateDescription", StructType(Array(
        StructField("updatedFields", StructType(Array(
          StructField("_id", StringType),
          StructField("level", IntegerType),
          StructField("score", IntegerType),
          StructField("nickname", StringType),
          StructField("item", MapType(StringType, IntegerType))
        ))),
          StructField("removedFields",ArrayType(StringType)),
            StructField("truncatedArrays",ArrayType(StringType))

      ))),
      StructField("operationType", StringType)
    ))


    //Mongodb'den verileri okuyoruz.
    val MongoDf=spark.readStream
      .format("mongodb")
      .option("spark.mongodb.connection.uri", mongo_url)
      .option("spark.mongodb.database", "41universe")
      .option("spark.mongodb.collection", "gamers")
      .schema(mongo_schema)
      .load()

    // Verileri topiclere göre ayıklıyoruz.
    val InsertDf=MongoDf.select(col("fullDocument"),col("clusterTime"))
      .where(col("operationType")==="insert")

    val UpdatetDf = MongoDf.select(col("updateDescription"), col("clusterTime"))
      .where(col("operationType") === "update")

    val DeletetDf = MongoDf.select(col("documentKey"), col("clusterTime"))
      .where(col("operationType") === "delete")

    //Kafkaya göndereceğimiz için Json formatına dönüştürüyoruz.
    val InsertJson=InsertDf.toJSON
    val UpdatetJson=UpdatetDf.toJSON
    val DeleteJson=DeletetDf.toJSON




    val OutputInsert=InsertJson.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "127.0.0.1:9092")
      .option("topic", "insertmongo")
      .option("checkpointLocation","/home/ozan/StreamLog2_ınsert")
      .trigger(Trigger.Continuous("1 second"))
      .outputMode("append")
      .start()



    val OutputUpdate = UpdatetJson.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "127.0.0.1:9092")
      .option("topic", "updatemongo")
      .option("checkpointLocation", "/home/ozan/StreamLog2_update")
      .trigger(Trigger.Continuous("1 second"))
      .outputMode("append")
      .start()



    val OutputDelete = DeleteJson.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "127.0.0.1:9092")
      .option("topic", "deletemongo")
      .option("checkpointLocation", "/home/ozan/StreamLog2_delete")
      .trigger(Trigger.Continuous("1 second"))
      .outputMode("append")
      .start()
      .awaitTermination()





  }

}
