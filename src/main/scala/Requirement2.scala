import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Requirement2{
  def main(args: Array[String]): Unit = {
    // created Spark Session
    val spark = SparkSession.builder().appName("DistinctCountries").master("local[*]").getOrCreate()
    //read the csv file into DataFrame
    val salesDF = spark.read.option("header", "true").csv("C:/Users/gomall/Downloads/sparsales.csv")
    //get distinct countries

    //saving as a parquet file
    //countriesDF.write.parquet("C:/Users/gomall/Desktop/result1.parquet")
    spark.stop()


  }

}