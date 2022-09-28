import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

//Burada kendi atlas hesabımı gizlemek için url'i başka bir dosyadan çektim.
//Sizde atlas_url değişkeni yerine kendi atlas hesabınızın url'inizi veya başka bir mongodb url'sini kullana bilirsiniz.
import Atlas.mongo_url

object mongodb_stream1 {
  def main(args: Array[String]): Unit = {

    //Spark session'u kuruyoruz.
    val spark=SparkSession.builder()
      .master("local[4]")
      .appName("MongoDb1")
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

    //Kafkaya göndereceğimiz için Json formatına dönüştürüyoruz.
    val MongoJson=MongoDf.toJSON

    //Verinin son halini kafkaya yolluyoruz.
    val OutputKafka=MongoJson.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "127.0.0.1:9092")
      .option("topic", "mongodb")
      .option("checkpointLocation","/home/ozan/StreamLog1")
      .trigger(Trigger.Continuous("1 second"))
      .outputMode("append")
      .start()
      .awaitTermination()





  }

}
