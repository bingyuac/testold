import java.io.File
import java.net.InetAddress

import com.maxmind.db.CHMCache
import com.maxmind.geoip2.DatabaseReader
import com.maxmind.geoip2.model.CityResponse
import org.json4s.DefaultFormats

/**
  * Created by andy on 16-10-17.
  */

object IPnew {

  implicit val formats = DefaultFormats

  //def main(args: Array[String]): Unit = {
  //val url = "F:\\Code\\OpenSource\\Data\\spark-sbt\\src\\main\\resources\\GeoLite2-City.mmdb"
  val url = "/home/andy/下载/ip库/GeoLite2-City.mmdb"
  //    val url2 = "/opt/db/geo/GeoLite2-City.mmdb"
  val geoDB = new File(url);
  geoDB.exists()
  val geoIPResolver = new DatabaseReader.Builder(geoDB).withCache(new CHMCache()).build();
  val ip1 = "218.21.128.31"  //中国 内蒙古自治区 包头市
  val ip2 = "222.173.17.203" //中国 山东省 济南
  val ip3 = "124.117.66.0"    //中国 新疆 乌鲁木齐
  val ip4 = "14.134.141.71"   //中国 宁夏回族自治区 银川
  val ip5 = "128.059.255.255"   //美国 纽约州 纽约

  def resolve_ip(resp: CityResponse): (String, String, String) = {
    (resp.getCountry.getNames.get("zh-CN"), resp.getSubdivisions.get(0).getNames().get("zh-CN"), resp.getCity.getNames.get("zh-CN"))
  }

  val inetAddress = InetAddress.getByName(ip1)
  val geoResponse = geoIPResolver.city(inetAddress)
  val (country, province, city) = (geoResponse.getCountry.getNames.get("zh-CN"), geoResponse.getSubdivisions.get(0).getNames().get("zh-CN"), geoResponse.getCity.getNames.get("zh-CN"))
  println(s"$country")
  println(resolve_ip(geoResponse))
  //println(s"country:$country,province:$province,city:$city")
  println(s"$country $province $city")
}

// }





/*object IPnew {

  implicit val formats = DefaultFormats

  def main(args: Array[String]): Unit = {
    //val url = "F:\\Code\\OpenSource\\Data\\spark-sbt\\src\\main\\resources\\GeoLite2-City.mmdb"
    val url = "/home/andy/下载/ip库/GeoLite2-City.mmdb"
    //    val url2 = "/opt/db/geo/GeoLite2-City.mmdb"
    val geoDB = new File(url);
    geoDB.exists()
    val geoIPResolver = new DatabaseReader.Builder(geoDB).withCache(new CHMCache()).build();
    val ip1 = "218.21.128.31"  //中国 内蒙古自治区 包头市
    val ip2 = "222.173.17.203" //中国 山东省 济南
    val ip3 = "124.117.66.0"    //中国 新疆 乌鲁木齐
    val ip4 = "14.134.141.71"   //中国 宁夏回族自治区 银川
    val ip5 = "128.059.255.255"   //美国 纽约州 纽约



    def resolve_ip(resp: CityResponse): (String, String, String) = {
      (resp.getCountry.getNames.get("zh-CN"), resp.getSubdivisions.get(0).getNames().get("zh-CN"), resp.getCity.getNames.get("zh-CN"))
    }



    val inetAddress = InetAddress.getByName(ip1)
    val geoResponse = geoIPResolver.city(inetAddress)
    val (country, province, city) = (geoResponse.getCountry.getNames.get("zh-CN"), geoResponse.getSubdivisions.get(0).getNames().get("zh-CN"), geoResponse.getCity.getNames.get("zh-CN"))
    println(s"$country")


    println(resolve_ip(geoResponse))


    //println(s"country:$country,province:$province,city:$city")
    println(s"$country $province $city")
  }

}*/
