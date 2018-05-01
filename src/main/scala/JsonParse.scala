/**
  * Created by shuvamoymondal on 4/29/18.
  */



import ScalaJsonParse.JsonValidation
import org.apache.spark.sql.SparkSession
import org.json4s.jackson.JsonMethods.parse


case class Location(types:String, coordinates: Array[String])


case class SensorData(
                       bulletin_date:String
                       , bulletin_date_formatted:String
                       , easting:String
                       , last_uploaded:String
                       , latest_capture:String
                       , latest_week:String
                       , latitude:String
                       , location:Location
                       , longitude:String
                       , no2_air_quality_band:String
                       , no2_air_quality_index:String
                       , northing:String
                       , site_code:String
                       , site_name:String
                       , site_type:String
                       , spatial_accuracy:String
                       , ward_code:String
                       , ward_name:String
                     )

object JsonParse {
  def main(args: Array[String]): Unit = {

    implicit val formats = org.json4s.DefaultFormats

    val spark: SparkSession =
      SparkSession
        .builder()
        .appName("AppName")
        .config("spark.master", "local")
        .getOrCreate()



    import spark.implicits._

    val myJson = "[{\"bulletin_date\":\"2016-07-02T21:00:00.000\",\"bulletin_date_formatted\":\"Day 184 21:00\",\"easting\":\"530529\",\"last_uploaded\":\"2016-07-02T23:05:04.000\",\"latest_capture\":\"No\",\"latest_week\":\"No\",\"latitude\":\"51.517368\",\"location\":{\"types\":\"Point\",\"coordinates\":[-0.120194,51.517368]},\"longitude\":\"-0.120194\",\"no2_air_quality_band\":\"Low\",\"no2_air_quality_index\":\"1\",\"northing\":\"181501\",\"site_code\":\"IM1\",\"site_name\":\"Camden - Holborn (inmidtown)\",\"site_type\":\"Kerbside\",\"spatial_accuracy\":\"Unknown\",\"ward_code\":\"E05000138\",\"ward_name\":\"Holborn and Covent Garden\"},{\"bulletin_date\":\"2017-01-26T03:00:00.000\",\"bulletin_date_formatted\":\"Day 026 03:00\",\"easting\":\"529885\",\"last_uploaded\":\"2017-01-26T04:05:05.000\",\"latest_capture\":\"No\",\"latest_week\":\"No\",\"latitude\":\"51.527707\",\"location\":{\"types\":\"Point\",\"coordinates\":[-0.129053,51.527707]},\"longitude\":\"-0.129053\",\"no2_air_quality_band\":\"Low\",\"no2_air_quality_index\":\"1\",\"northing\":\"182635\",\"site_code\":\"CD9\",\"site_name\":\"Camden - Euston Road\",\"site_type\":\"Roadside\",\"spatial_accuracy\":\"Unknown\",\"ward_code\":\"E05000141\",\"ward_name\":\"King's Cross\"}]"

    val parsedData = parse(myJson).extract[Array[SensorData]]

    val mySourceDataset = spark.createDataset(parsedData)
    mySourceDataset.printSchema()


}
  }

