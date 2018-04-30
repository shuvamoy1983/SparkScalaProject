package ScalaJsonParse

import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

/**
  * Created by shuvamoymondal on 4/30/18.
  */

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
