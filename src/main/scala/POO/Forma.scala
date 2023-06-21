package POO
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.databricks.spark.xml._
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types.{ArrayType, StructType}

class Forma(df: DataFrame, spark: SparkSession){
  private def expandNestedColumn(df_temp: DataFrame): DataFrame = {
    var dfTemp: DataFrame = df_temp
    var selectClauseList = List.empty[String]

    for (columnName <- dfTemp.schema.names) {
      dfTemp.schema(columnName).dataType match {
        case _: ArrayType =>
          dfTemp = dfTemp.withColumn(columnName, explode(dfTemp(columnName)).alias(columnName))
          selectClauseList :+= columnName
        case structType: StructType =>
          for (field <- structType.fields) {
            selectClauseList :+= columnName + "." + field.name
          }
        case _ =>
          selectClauseList :+= columnName
      }
    }
    val columnNames = selectClauseList.map(name => col(name).alias(name.replace('.', '_')))
    dfTemp.select(columnNames: _*)
  }

  def aplanarDataFrame(): DataFrame = {
    var dfTemp = df
    var nestedColumnCount = 1
    while (nestedColumnCount != 0) {
      var nestedColumnCountTemp = 0

      for (columnName <- dfTemp.schema.names) {
        if (dfTemp.schema(columnName).dataType.isInstanceOf[ArrayType]
          ||
          dfTemp.schema(columnName).dataType.isInstanceOf[StructType]) {
          nestedColumnCountTemp += 1
        }
      }

      if (nestedColumnCountTemp != 0) {
        dfTemp = expandNestedColumn(dfTemp)
      }
      nestedColumnCount = nestedColumnCountTemp
    }
    dfTemp
  }
}

object main extends App{
  val spark = SparkSession
    .builder()
    .master("local")
    .getOrCreate()

  val ruta = "src/data/nestedxml.xml"

  val df =  spark.read
    .option("rootTag", "items")
    .option("rowTag", "item")
    .xml(ruta)

  val forma1 = new Forma(df, spark)
  forma1.aplanarDataFrame().show()

  //val newDataFrame = new Forma(ruta, spark)
  //val dfAplanado = newDataFrame.orquestador
  //dfAplanado.show()
}
