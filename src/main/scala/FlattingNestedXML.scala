import org.apache.spark.sql.{DataFrame, SparkSession}
import com.databricks.spark.xml._
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types.{ArrayType, StructField, StructType}

object FlattingNestedXML{

  def expandNestedColumn(df_temp: DataFrame): DataFrame ={
    var df: DataFrame = df_temp
    var selectClauseList = List.empty[String]

    for (columnName <- df.schema.names) {
      if (df.schema(columnName).dataType.isInstanceOf[ArrayType]){
        df = df.withColumn(columnName, explode(df(columnName)).alias(columnName))
        selectClauseList :+= columnName
      }else if (df.schema(columnName).dataType.isInstanceOf[StructType]){
        for (field <- df.schema(columnName).dataType.asInstanceOf[StructType].fields){
          selectClauseList :+= columnName + "." + field.name
        }
      }else {
        selectClauseList :+= columnName
      }
    }

    val columnNames = selectClauseList.map(name => col(name).alias(name.replace('.', '_')))
    df.select(columnNames:_*)
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("ScalaMnMCount")
      .config("spark.master", "local")
      .getOrCreate()

    val xmlPath = "src/data/nestedxml.xml"
    var df = spark.read.option("rowTag", "items").xml(xmlPath)

    var nestedColumnCount = 1
    while (nestedColumnCount != 0){
      var nestedColumnCountTemp = 0

      for (columnName <- df.schema.names) {
        if (df.schema(columnName).dataType.isInstanceOf[ArrayType] || df.schema(columnName).dataType.isInstanceOf[StructType]) {
          nestedColumnCountTemp += 1
        }
      }

      if (nestedColumnCountTemp != 0){
        df = expandNestedColumn(df)
        df.show(10, false)
      }
      nestedColumnCount = nestedColumnCountTemp
    }
    spark.close()
  }
}
