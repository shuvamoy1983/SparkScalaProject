/**
  * Created by shuvamoymondal on 4/29/18.
  */


import org.apache.spark.SparkConf
import org.apache.spark
import org.json4s._
import org.json4s.jackson.JsonMethods._
import java.util.Date
import ScalaJsonParse.JsonValidation

case class Address(street: String, city: String)
case class PersonWithAddresses(name: String, addresses: Map[String, Address])

object JsonParse {
  def main(args: Array[String]): Unit = {

    implicit val formats = DefaultFormats


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


    var data = parse(json_data)
   // println(data.extract[PersonWithAddresses])

    val p = new JsonValidation

}
  }

