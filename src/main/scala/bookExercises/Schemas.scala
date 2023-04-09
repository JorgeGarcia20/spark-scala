package bookExercises

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


object Schemas extends App{
  val spark = SparkSession
    .builder()
    .appName("Schemas and DataFrames")
    .config("spark.master", "local")
    .getOrCreate()

  val jsonFilePath = "./src/data/blogs.json"
  // Definiendo un schema (esquema)

  val schema = StructType(Array(
    StructField("Id", IntegerType, false),
    StructField("First", StringType, false),
    StructField("Last", StringType, false),
    StructField("Url", StringType, false),
    StructField("Published", StringType, false),
    StructField("Hits", IntegerType, false),
    StructField("Campaigns", ArrayType(StringType), false),
  ))

  // Creando un DataFrame a partir de un archivo JSON con el esquema predefinido.
  val blogsDF = spark.read
    .schema(schema)
    .json(jsonFilePath)

  // Mostrando el DataFrame
  blogsDF.show(false)

  // Mostrando el schema
  println(blogsDF.printSchema())
  println(blogsDF.schema)

  // TODO: Aplanar la columna Campaigns.

  /*
  * COLUMNS AND EXPRESSIONS
  * */

  // Mostrando las columnas del DataFrame
  println(blogsDF.columns)

  // la funcion col retorna un tipo columna
  // Retornando la columna URl
  println(blogsDF.col("Url"))

  // Usando una expresion para computar un valor
  blogsDF.select(expr("Hits * 2").alias("Hits times two")).show(false)
  // También se puede usar col para computar un valor
  blogsDF.select((col("Hits") * 2).alias("Hits times two")).show(false)

  // Usando una expresion para computar big hitters para los blogs
  // Agregara una nueva columna llamada Big Hitters basada en una expresion condicional.
  blogsDF.withColumn("Big Hitters", (expr("Hits > 10000"))).show()

  /*
  * Concatenando tres columnas
  * Crear una columna con fast last y el id concatedados para formar una nueva columna
  * llamada AuthorsId
  * */

  blogsDF.withColumn("AuthorsId",
    (concat(expr("First"), expr("Last"), expr("Id"))))
    .select(col("AuthorsId"))
    .show(4)

  // Alternativas para seleccionar una columna
  blogsDF.select(expr("Hits")).show(2)
  blogsDF.select(col("Hits")).show(2)
  blogsDF.select("Hits").show(2)

  // Ordenando por hits
  blogsDF.sort(col("Hits").desc).show()

  spark.close()
}