import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Demo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("ScalaMnMCount")
      .config("spark.master", "local")
      .getOrCreate()
  /*
    if(args.length < 1){
      print("Usage: MnMCount src/data/mnm_dataset.csv")

      sys.exit(1)
    }
    val mnmFile = args(0)
    println(mnmFile)

   */

    val mnmFilePath = "src/data/mnm_dataset.csv"

    val mnmDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(mnmFilePath)

    mnmDF.printSchema()

    mnmDF.show(10, false)
    // Columns State Color Total
    val query1 = mnmDF
      .select("State", "Color", "Count")
      .where(col("Count") > 60)
      .orderBy("Count")
    query1.show()

    val countMnMDf = mnmDF
      .select("State", "Color", "Count")
      .groupBy("State", "Color")
      .agg(count("Count").alias("Total"))
      .orderBy(desc("Total"))

    countMnMDf.show()
    println(s"Total rows: ${countMnMDf.count()}")

    val CACountMnMDF = mnmDF
      .select("State", "Color", "Count")
      .where(col("State") === "CA")
      .groupBy("State", "Color")
      .agg(count("Count").alias("Total"))
      .orderBy(desc("Total"))

    CACountMnMDF.show(10)

    spark.close()
  }
}
