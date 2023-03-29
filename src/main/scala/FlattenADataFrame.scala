import com.databricks.spark.xml._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode

object FlattenADataFrame {
  def main(main: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("XMLToJSON")
      .config("spark.master", "local")
      .getOrCreate()

    val xmlPath = "src/data/nestedxml.xml"

    val df = spark.read
      .option("rootTag", "items")
      .option("rowTag", "item")
      .xml(xmlPath)

    df.show()
    df.printSchema()

    // Cambiando los nombres _id y _type por Id y Type
    val df2 = df.select("*", "batters.*")
      .drop("batters")
      .withColumnRenamed("_id", "Id")
      .withColumnRenamed("_type", "Type")

    df2.show(false)

    val df3 = df2.withColumn("batter", explode(df2("batter")))
      .withColumn("topping", explode(df2("topping")))

    df3.show(false)

    //extraccion de las columnas value e id de topping
    val df4 = df3.select("*", "topping.*")

    df4.show(false)

    val df5 = df4
      .withColumnRenamed("_VALUE", "Topping_value")
      .withColumnRenamed("_id", "Topping_id")
      .drop("topping")
      .select("*", "batter.*")

    df5.show(false)

    val df6 = df5
      .withColumnRenamed("_VALUE", "Batter_value")
      .withColumnRenamed("_id", "Batter_id")
      .drop("batter")

    df6.show(false)

    // Creando una vista temporal para poder hacer queries de SQL
    df6.createTempView("donuts")

    spark.sql(
      """
        |SELECT Type, name, ppu
        |FROM donuts
        |WHERE Topping_value = "Chocolate"
        |""".stripMargin).show()

    spark.close()
  }
}
