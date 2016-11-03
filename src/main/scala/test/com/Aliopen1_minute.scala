package test.com

import java.io.File
import java.net.InetAddress
import java.text.SimpleDateFormat
import java.util.{Date, Locale, Properties}

import com.maxmind.db.CHMCache
import com.maxmind.geoip2.DatabaseReader
import com.maxmind.geoip2.model.CityResponse

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.DefaultFormats

/**
  * Created by andy on 16-10-17.
  */
object Aliopen1_minute {
  def main(args: Array[String]) {
    //val sparkConf = new SparkConf().setMaster("yarn-client").setAppName("LogStreaming")spark://open003:7077
    //val sparkConf = new SparkConf().setMaster("local[4]").set("spark.sql.shuffle.partitions","20").setAppName("Aliopen1Streaming")
    val sparkConf = new SparkConf().setMaster("local[8]").setAppName("Aliopen1_nimute_Streaming")
    //每60秒一个批次
    val ssc = new StreamingContext(sparkConf, Seconds(60))
    //从Kafka中读取数据，topic为ali，该topic包含两个分区
    val kafkaStream = KafkaUtils.createStream(
      ssc,
      "60.205.152.16:2181,60.205.152.20:2181,60.205.152.57:2181", //Kafka集群使用的zookeeper
      "aliopen1-consumer-minute", //该消费者使用的group.id
      Map[String, Int]("ali" -> 0,"ali" -> 1,"ali" -> 2,"ali" -> 3,"ali" -> 4,"ali" -> 5,"ali" -> 6,"ali" -> 7,"ali" -> 8,"ali" -> 9), //日志在Kafka中的topic及其分区
      StorageLevel.MEMORY_AND_DISK_SER)
      .map(x => x._2.split(" ", -1))
    kafkaStream.foreachRDD(foreachFunc = (rdd: RDD[Array[String]], time: Time) => {
      val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
      import sqlContext.implicits._

      val logDataFrame = rdd.map(w => new Aliopen1Log(
        parseDateFormat.parseTime(
          parseDateFormat.parseFormat(w(11).substring(1))), w(8),
        IPnew.resolve_ip(IPnew.geoIPResolver.city(InetAddress.getByName(w(8)))), w(13).substring(1, 4), w(14), w(15).replace("\"", ""), w(16), w(17))
      ).toDF()

      //注册为tempTable
      logDataFrame.registerTempTable("aliopen1log")
      //val stat_browser_sql = ""
      //val stat_browser_url_sql = ""
      val stat_data_minute_sql = "select \"1\" as client_id,\"1001\" as project_id,ip,substring(day,1,12) as time," +
        "sum(case when bytes=\"-\" then \"0\" else bytes end) as bytes from aliopen1log group by substring(day,1,12),ip"
      val stat_data_url_minute_sql = "select \"1\" as client_id,\"1001\" as project_id,request,substring(day,1,12) as time," +
        "sum(case when bytes=\"-\" then \"0\" else bytes end) as bytes from aliopen1log group by substring(day,1,12),request"
      val stat_ip_minute_sql = "select \"1\" as client_id,\"1001\" as project_id,substring(day,1,12) as time,ip,count(*) as counts" +
        " from aliopen1log group by substring(day,1,12),ip"
      val stat_ip_geography_sql = "select \"1\" as client_id,ip as c_ip,split(geo,\",\")[0] as country," +
        "case when instr(split(geo,\",\")[1],'省')>0 then split(split(geo,\",\")[1],'省')[0] " +
        "when instr(split(geo,\",\")[1],'市')>0 then split(split(geo,\",\")[1],'市')[0] " +
        "when instr(split(geo,\",\")[1],'新疆')>0 then substring(split(geo,\",\")[1],1,2)" +
        "when instr(split(geo,\",\")[1],'内蒙古')>0 then substring(split(geo,\",\")[1],1,3)" +
        "when instr(split(geo,\",\")[1],'宁夏')>0 then substring(split(geo,\",\")[1],1,2) " +
        "when instr(split(geo,\",\")[1],'广西')>0 then substring(split(geo,\",\")[1],1,2) " +
        "when instr(split(geo,\",\")[1],'西藏')>0 then substring(split(geo,\",\")[1],1,2) " +
        "when instr(split(geo,\",\")[1],'香港')>0 then substring(split(geo,\",\")[1],1,2) " +
        "when instr(split(geo,\",\")[1],'澳门')>0 then substring(split(geo,\",\")[1],1,2) " +
        "when instr(split(geo,\",\")[1],'台湾')>0 then substring(split(geo,\",\")[1],1,2) else split(geo,\",\")[1] end as province," +
        "case when instr(split(geo,\",\")[2],'市')>0 then split(split(geo,\",\")[2],'市')[0] else split(geo,\",\")[2] end as city,\"0\" as longitude,\"0\" as latitude " +
        "from aliopen1log"
      //val stat_new_ip_sql = ""
      //val stat_os_minute_sql = ""
      //val stat_os_url_minute_sql = ""
      //val stat_page_time_sql = ""
      val stat_pv_minute_sql = "select \"1\" as client_id,\"1001\" as project_id,substring(day,1,12) as time,request,count(*) as counts,\"1\" as type" +
        " from aliopen1log group by substring(day,1,12),request"
      val stat_pv_url_minute_sql = "select \"1\" as client_id,\"1001\" as project_id,ip,request,substring(day,1,12) as time,count(*) as counts,\"1\" as type" +
        " from aliopen1log group by substring(day,1,12),request,ip"
      //val stat_referrer_minute_sql = ""
      //val stat_referrer_url_minute_sql = ""
      val stat_request_minute_sql = "select \"1\" as client_id,substring(day,1,12) as time,ip,method,count(*) as counts,\"1001\" as project_id " +
        "from aliopen1log group by substring(day,1,12),ip,method"
      val stat_status_minute_sql = "select \"1\" as client_id,\"1001\" as project_id,ip,substring(day,1,12) as time,status,count(*) as counts" +
        " from aliopen1log group by substring(day,1,12),ip,status"
      val stat_status_url_minute_sql = "select \"1\" as client_id,\"1001\" as project_id,request,substring(day,1,12) as time,status,count(*) as counts " +
        "from aliopen1log group by substring(day,1,12),request,status"
      //val stat_top_page_sql = ""
      //val stat_tp_minute_sql = ""

      val stat_data_minute = sqlContext.sql(stat_data_minute_sql)
      val stat_data_url_minute = sqlContext.sql(stat_data_url_minute_sql)
      val stat_ip_minute = sqlContext.sql(stat_ip_minute_sql)
      val stat_pv_minute = sqlContext.sql(stat_pv_minute_sql)
      val stat_pv_url_minute = sqlContext.sql(stat_pv_url_minute_sql)
      val stat_request_minute = sqlContext.sql(stat_request_minute_sql)
      val stat_status_minute = sqlContext.sql(stat_status_minute_sql)
      val stat_status_url_minute = sqlContext.sql(stat_status_url_minute_sql)
      val stat_ip_geography = sqlContext.sql(stat_ip_geography_sql)

      val dfWriter_data = stat_data_minute.write.mode("append")
      val dfWriter_data_url = stat_data_url_minute.write.mode("append")
      val dfWriter_ip = stat_ip_minute.write.mode("append")
      val dfWriter_pv = stat_pv_minute.write.mode("append")
      val dfWriter_pv_url = stat_pv_url_minute.write.mode("append")
      val dfWriter_stat_request = stat_request_minute.write.mode("append")
      val dfWriter_stat_status = stat_status_minute.write.mode("append")
      val dfWriter_stat_status_url = stat_status_url_minute.write.mode("append")
      val dfWriter_stat_ip_geography = stat_ip_geography.write.mode("append")
      //
      val prop = new Properties()
      prop.put("user", "root")
      prop.put("password", "mysql")

      dfWriter_data.jdbc("jdbc:mysql://101.200.218.23:3306/open03", "stat_data_minute", prop)
      dfWriter_data_url.jdbc("jdbc:mysql://101.200.218.23:3306/open03", "stat_data_url_minute", prop)
      dfWriter_ip.jdbc("jdbc:mysql://101.200.218.23:3306/open03","stat_ip_minute", prop)
      dfWriter_pv.jdbc("jdbc:mysql://101.200.218.23:3306/open03","stat_pv_minute", prop)
      dfWriter_pv_url.jdbc("jdbc:mysql://101.200.218.23:3306/open03","stat_pv_url_minute", prop)
      dfWriter_stat_request.jdbc("jdbc:mysql://101.200.218.23:3306/open03","stat_request_minute", prop)
      dfWriter_stat_status.jdbc("jdbc:mysql://101.200.218.23:3306/open03","stat_status_minute", prop)
      dfWriter_stat_status_url.jdbc("jdbc:mysql://101.200.218.23:3306/open03","stat_status_url_minute", prop)
      dfWriter_stat_ip_geography.jdbc("jdbc:mysql://101.200.218.23:3306/open03","stat_ip_geography", prop)

    })
    ssc.start()
    ssc.awaitTermination()
  }

  case class Aliopen1Log(day: String, ip: String,geo: String, method: String, request: String, protocol: String, status: String, bytes: String)
  object SQLContextSingleton {
    @transient private var instance: SQLContext = _

    def getInstance(sparkContext: SparkContext): SQLContext = {
      if (instance == null) {
        instance = new SQLContext(sparkContext)
      }
      instance
    }
  }


  //TimeParse
  object parseDateFormat {

    def parseFormat(string: String): Date = {

      val FORMAT = new SimpleDateFormat("d/MMM/yyyy:HH:mm:ss", Locale.ENGLISH)
      var date: Date = new Date()
      date = FORMAT.parse(string)
      date
    }
    def parseTime(date: Date): String = {
      val format1 = new SimpleDateFormat("yyyyMMddHHmmss")
      format1.format(date)
    }

  }

  object IPnew {

    implicit val formats = DefaultFormats

    val url = "/home/hadoop/test/project/ip/GeoLite2-City.mmdb"

    val geoDB = new File(url);
    geoDB.exists()
    val geoIPResolver = new DatabaseReader.Builder(geoDB).withCache(new CHMCache()).build();
    /*
        val ip1 = "218.21.128.31"  //中国 内蒙古自治区 包头市
        val ip2 = "222.173.17.203" //中国 山东省 济南
        val ip3 = "124.117.66.0"    //中国 新疆 乌鲁木齐
        val ip4 = "14.134.141.71"   //中国 宁夏回族自治区 银川
        val ip5 = "128.059.255.255"   //美国 纽约州 纽约
    */

    def resolve_ip(resp: CityResponse): String = {
      resp.getCountry.getNames.get("zh-CN") + "," + resp.getSubdivisions.get(0).getNames().get("zh-CN") + "," + resp.getCity.getNames.get("zh-CN")
    }

  }

}
