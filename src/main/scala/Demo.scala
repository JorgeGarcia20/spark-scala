import org.apache.spark.{SparkConf, SparkContext}

class Demo {
  def main(main: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Demo").setMaster("local[1]")
    val sc = SparkContext.getOrCreate(conf)
    val path = "src/data/penguins.csv"
  }
}
