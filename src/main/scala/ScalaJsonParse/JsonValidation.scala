package ScalaJsonParse

import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

import org.apache.spark.SparkConf
import org.apache.spark
/**
  * Created by shuvamoymondal on 4/30/18.
  */

// ./spark-submit --class JsonParse --jars /Users/shuvamoymondal/Downloads/json4s-native_2.10-3.2.4.jar /Users/shuvamoymondal/goworkspace/pkg/github.com/shuva/main/ScalaJsonParse/target/ScalaJsonParse-1.0-SNAPSHOT.jar
case class Address(street: String, city: String)
case class PersonWithAddresses(name: String, addresses: Map[String, Address])

class JsonValidation {


  implicit val formats = DefaultFormats



  def parseJson(str: String ) : Unit = {

    var data = parse(json_data)
    println(data.extract[PersonWithAddresses])


  }


  val json_data =
    """
      |          {
      |            "name": "joe",
      |            "addresses": {
      |              "address1": {
      |                "street": "Bulevard",
      |                "city": "Helsinki"
      |              },
      |              "address2": {
      |                "street": "Soho",
      |                "city": "London"
      |              }
      |            }
      |          }""".stripMargin


  parseJson(json_data)
  //data.extract[PersonWithAddresses]
}
