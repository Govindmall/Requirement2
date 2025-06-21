import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object Requirement2{
  def main(args: Array[String]): Unit = {
    // created Spark Session
    val spark = SparkSession.builder().appName("DistinctCountries").master("local[*]").getOrCreate()
    //read the csv file into DataFrame
    val salesDF = spark.read.option("header", "true").csv("C:/Users/gomall/Downloads/sparsales.csv")
    //compute total units sold per region
    val regionSalesDF=salesDF.groupBy("Region").agg(sum("Units Sold").as("Total_unit_sold"))
    //show the regionSalesDF in console
    regionSalesDF.show()
    regionSalesDF.write.parquet("C:/Users/gomall/Desktop/2.parquet")
    spark.stop()


  }

}