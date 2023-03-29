import com.databricks.spark.xml._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object XMLtoJSON2 {

  def main(main: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("XMLToJSON")
      .config("spark.master", "local")
      .getOrCreate()

    val xmlPath = "/home/jorge/IdeaProjects/sparkGettingStarted/src/data/nestedxml.xml"
    val jsonsPath = "/home/jorge/IdeaProjects/sparkGettingStarted/src/data/jsons/"
    /*
        /*
        * <item id="0001" type="donut">
                <name>Cake</name>
                <ppu>0.55</ppu>
                <batters>
                    <batter id="1001">Regular</batter>
                    <batter id="1001">Chocolate</batter>
                    <batter id="1001">Blueberry</batter>
                </batters>
                <topping id="5001">None</topping>
                <topping id="5002">Glazed</topping>
                <topping id="5003">Sugar</topping>
                <topping id="5004">Sprinkless</topping>
                <topping id="5005">Chocolate</topping>
                <topping id="5006">Maple</topping>
            </item>
        * */

        val itemSchema = StructType(Seq(
          StructField("_id", StringType, nullable = true),
          StructField("_type", StringType, nullable = false),
          StructField("name", StringType, nullable = false),
          StructField("ppu", DoubleType, nullable = false),
          StructField("batters", StructType(Seq(
            StructField("batter", ArrayType(StructType(Seq(
              StructField("_id", StringType, nullable = false),
              StructField("_VALUE", StringType, nullable = false)
            ))), nullable = false)
          )), nullable = false),
          StructField("topping", ArrayType(StructType(Seq(
            StructField("_id", StringType, nullable = false),
            StructField("_VALUE", StringType, nullable = false)
          ))), nullable = false),
        ))

        val df = spark.read
          .option("rootTag", "items")
          .option("rowTag", "item")
          .option("ignoreSurroundingSpaces", "true")
          .schema(itemSchema)
          .xml(xmlPath)

        //df.show(false)

        val json = df.toJSON.toDF()
        json.show()

        json.write.format("json").mode("overwrite").save()

         */

    // json.write.option("multiLine", "true").json("/home/jorge/IdeaProjects/sparkGettingStarted/src/data/books.json")

    val schema = StructType(
      Array(
        StructField("_id", StringType),
        StructField("_type", StringType),
        StructField("name", StringType),
        StructField("ppu", DoubleType),
        StructField("batters", StructType(
          Array(
            StructField("batter", ArrayType(
              StructType(
                Array(
                  StructField("_id", StringType),
                  StructField("_VALUE", StringType)
                )
              )
            ))
          )
        )),
        StructField("topping", ArrayType(
          StructType(
            Array(
              StructField("_id", StringType),
              StructField("_VALUE", StringType)
            )
          )
        ))
      )
    )

    val df = spark.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "item")
      .schema(schema)
      .load(xmlPath)

    df.show()

    spark.close()
  }
}
