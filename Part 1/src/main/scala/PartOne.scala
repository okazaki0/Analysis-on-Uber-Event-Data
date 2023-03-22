import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.sql.types.TimestampType

object PartOne {

  case class Uber(dt: java.sql.Timestamp, lat: Double, lon: Double, base: String) extends Serializable

  val schema = StructType(Array(
    StructField("dt", StringType, true),
    StructField("lat", DoubleType, true),
    StructField("lon", DoubleType, true),
    StructField("base", StringType, true)
  ))


  def main(args: Array[String]) {
//I delete the data from the file because it large x
    var file1: String = "/Users/AvyaTiK/Desktop/M2/Hadoop/Project/data/uber-raw-data-sep14.csv"
    var savedirectory: String = "/Users/AvyaTiK/Desktop/M2/Hadoop/Project/ubermodel"



    val spark: SparkSession = SparkSession.builder().config("spark.master", "local").appName("uber").getOrCreate()

    import spark.implicits._
    val df: Dataset[Uber] = spark.read.option("inferSchema", "false").schema(schema).option("header", "true").csv(file1).as[Uber]

    df.show
    df.schema


    val featureCols = Array("lat", "lon")
    val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")
    val df2 = assembler.transform(df)
    df2.show
    val Array(trainingData, testData) = df2.randomSplit(Array(0.7, 0.3), 5043)
    val kmeans: KMeans = new KMeans()
      .setK(10)
      .setFeaturesCol("features")
      .setPredictionCol("cid")
      .setSeed(1L)
    val model: KMeansModel = kmeans.fit(trainingData)

    println("Final Centers: ")
    model.clusterCenters.foreach(println)

    //Convert timestamp string in mm/dd/yyyy hh:mm:ss to timestamp format

    val df3 = df2.withColumnRenamed("dt","dt_string")
    val df4 = df3.withColumn("dt",unix_timestamp(df3("dt_string"),"MM/dd/yyyy HH:mm:ss").cast(TimestampType))

    val clusters = model.transform(df4).drop("dt_string")
    clusters.show
    clusters.createOrReplaceTempView("uber")

    println("Which clusters had the highest number of pickups?")
    clusters.groupBy("cid").count().orderBy(desc("count")).show
    println("select cid, count(cid) as count from uber group by cid")
    spark.sql("select cid, count(cid) as count from uber group by cid").show

    println("Which cluster/base combination had the highest number of pickups?")
    clusters.groupBy("cid", "base").count().orderBy(desc("count")).show

    println("which hours of the day and which cluster had the highest number of pickups?")
    clusters.select(hour($"dt").alias("hour"), $"cid")
      .groupBy("hour", "cid").agg(count("cid")
      .alias("count")).orderBy(desc("count")).show
    println("SELECT hour(uber.dt) as hr,count(cid) as ct FROM uber group By hour(uber.dt)")
    spark.sql("SELECT hour(uber.dt) as hr,count(cid) as ct FROM uber group By hour(uber.dt)").show

    // to save the model
    println("save the model")
    model.write.overwrite().save(savedirectory)



  }

}