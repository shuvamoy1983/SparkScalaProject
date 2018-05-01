/**
  * Created by shuvamoymondal on 4/30/18.
  */


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.json4s.jackson.JsonMethods.parse

import scala.io.Source._
import org.apache.spark.sql.functions.explode

case class Address1(street: String, city: String) extends Serializable
case class PersonWithAddresses1(name: String,addresses: Array[Address1]) extends  Serializable


object test_json {
  def main(args: Array[String]): Unit = {

    implicit val formats = org.json4s.DefaultFormats

    val spark: SparkSession =
      SparkSession
        .builder()
        .appName("AppName")
        .config("spark.master", "local")
        .getOrCreate()


    import spark.implicits._

   /* val json_data =fromFile("/Users/shuvamoymondal/test.json").mkString

    val parsedData = parse(json_data).extract[Array[PersonWithAddresses1]]
    val mySourceDataset = spark.createDataset(parsedData)
    mySourceDataset.printSchema() */

    println("Dataframe manipulation started")

    val p =spark.read.option("multiline", true).option("mode","PERMISSIVE").json("/Users/shuvamoymondal/test.json")
    val df_1=p.select($"name",explode($"addresses").as("adr"))
    val df1 = df_1.select($"name",$"adr.address1.city",$"adr.address1.street",$"adr.address2.street")
    df1.show()

    val df =spark.read.option("multiline", true).option("mode","PERMISSIVE").json("/Users/shuvamoymondal/keywordData.json")
    val df_2 =spark.read.option("multiline", true).option("mode","PERMISSIVE").json("/Users/shuvamoymondal/keywordData.json")
    var xp_df_1 = df.withColumn("term_flat", explode(df("keyword_data")))
    var xp_df_2 = xp_df_1.drop(xp_df_1.col("keyword_data"))
    var xp_df_data_points = xp_df_2.withColumn("data_points", xp_df_2("term_flat.data_points"))
    var xp_df_name = xp_df_data_points.withColumn("m_guid", xp_df_data_points("data_points.name"))
    var xp_df_name_val = xp_df_name.withColumn("m_name", xp_df_name("data_points.value"))
    var xp_df_final = xp_df_name_val.drop(xp_df_name_val.col("term_flat"))
    var final_df = xp_df_final.drop(xp_df_final.col("data_points"))

     final_df.show(10)
  }
}

