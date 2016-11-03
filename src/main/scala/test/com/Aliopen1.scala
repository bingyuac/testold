package test.com

import java.text.SimpleDateFormat
import java.util.{Date, Locale, Properties}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by andy on 16-10-17.
  */
object Aliopen1 {
  def main(args: Array[String]) {
    //val sparkConf = new SparkConf().setMaster("yarn-client").setAppName("LogStreaming")spark://open003:7077
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("Aliopen1Streaming")
    //每60秒一个批次
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    //从Kafka中读取数据，topic为ali，该topic包含两个分区
    val kafkaStream = KafkaUtils.createStream(
      ssc,
      "60.205.152.16:2181", //Kafka集群使用的zookeeper
      "test-consumer-group", //该消费者使用的group.id
      Map[String, Int]("ali" -> 0,"ali" -> 1,"ali" -> 2,"ali" -> 3,"ali" -> 4,"ali" -> 5,"ali" -> 6,"ali" -> 7,"ali" -> 8,"ali" -> 9), //日志在Kafka中的topic及其分区
      StorageLevel.MEMORY_AND_DISK_SER)
      .map(x => x._2.split(" ", -1))
    kafkaStream.foreachRDD((rdd: RDD[Array[String]], time: Time) => {
      val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
      import sqlContext.implicits._
      //构造case class: Aliopen1Log,提取日志中相应的字段
      //                 date          ip method     request  protocol  status bytes referrer     OS  browser
      ////192.168.1.141 - - [27/Sep/2016:16:13:07 +0800] "GET / HTTP/1.1" 403 4961 "-" "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36"
      //2016-10-13T13: 54:59.692469+08:00 open1 Apache  - [aliopen1@001 tag="Aliopen1Test"] 207.46.13.108 - - [13/Oct/2016:13:53:33 +0800] "GET / HTTP/1.1" 200 19490
      //val logDataFrame = rdd.map(w => new Aliopen1Log(w(10).substring(1, 20),w(7),w(12).substring(1,3),w(13),w(14).replace("\"",""),w(15),w(16),w(17).replace("\"",""),w(20),w(23))).toDF()
      //val logDataFrame = rdd.map(w => new Aliopen1Log(w(0).substring(0, 10),w(2),w(6))).toDF()
      //val logDataFrame = rdd.map(w => new Aliopen1Log(w(11).substring(1),w(8),w(13).substring(1,3),w(14),w(15).replace("\"",""),w(16),w(17))).toDF()
      val logDataFrame = rdd.map(w => new Aliopen1Log(parseDateFormat.parseTime(parseDateFormat.parseFormat(w(11).substring(1))), w(8), w(13).substring(1, 3), w(14), w(15).replace("\"", ""), w(16), w(17))).toDF()
      //val data = parseDateFormat
      //Date date = parseDateFormat(time);
      // dateformat1.format(date);

      //注册为tempTable
      logDataFrame.registerTempTable("aliopen1log")
      //查询该批次的pv,ip数,uv
      val stat_data_minute = sqlContext.sql("select \"1\" as client_id,\"1\" as project_id,ip,substring(day,1,12) as time," +
        "sum(case when bytes=\"-\" then \"0\" else bytes end) as bytes from aliopen1log group by substring(day,1,12),ip")
      //打印查询结果
      val stat_data_url_minute = sqlContext.sql("select \"1\" as client_id,\"1\" as project_id,request,substring(day,1,12) as time," +
        "sum(case when bytes=\"-\" then \"0\" else bytes end) as bytes from aliopen1log group by substring(day,1,12),request")
      val stat_ip_minute = sqlContext.sql("select \"1\" as client_id,\"1\" as project_id,substring(day,1,12) as time,ip,count(*) as counts" +
        " from aliopen1log group by substring(day,1,12),ip")
      val stat_pv_minute = sqlContext.sql("select \"1\" as client_id,\"1\" as project_id,substring(day,1,12) as time,request,count(*) as counts" +
        " from aliopen1log group by substring(day,1,12),request")
      val stat_pv_url_minute = sqlContext.sql("select \"1\" as client_id,\"1\" as project_id,ip,request,substring(day,1,12) as time,count(*) as counts" +
        " from aliopen1log group by substring(day,1,12),request,ip")

      stat_data_minute.show()
      stat_data_url_minute.show()
      stat_ip_minute.show()
      stat_pv_minute.show()
      stat_pv_url_minute.show()
      //val stat_data_daily = sqlContext.sql("select \"1\" as client_id,\"1\" as project_id,ip,substring(day,1,12) as time," +
      // "case when bytes=\"-\" then \"0\" else bytes end as bytes from aliopen1log group by substring(day,1,12),ip,case when bytes=\"-\" then \"0\" else bytes end")

      val dfWriter_data = stat_data_minute.write.mode("append")
      //val dfWriter = stat_data_daily.write.mode("append")
      val dfWriter_data_url = stat_data_url_minute.write.mode("append")
      val dfWriter_ip = stat_ip_minute.write.mode("append")
      val dfWriter_pv = stat_pv_minute.write.mode("append")
      val dfWriter_pv_url = stat_pv_url_minute.write.mode("append")
      val prop = new Properties()
      prop.put("user", "root")
      prop.put("password", "mysql")
      //101.200.218.23 open2database
      //dfWriter.jdbc("jdbc:mysql://60.205.152.16:3306/test","test2",prop)
      dfWriter_data.jdbc("jdbc:mysql://101.200.218.23:3306/aliopen01", "stat_data_minute", prop)
      dfWriter_data_url.jdbc("jdbc:mysql://101.200.218.23:3306/aliopen01", "stat_data_url_minute", prop)
      //dfWriter_ip.jdbc("jdbc:mysql://101.200.218.23:3306/aliopen01","stat_ip_minute", prop)
      //dfWriter_pv.jdbc("jdbc:mysql://101.200.218.23:3306/aliopen01","stat_pv_minute", prop)
      //dfWriter_pv_url.jdbc("jdbc:mysql://101.200.218.23:3306/aliopen01","stat_pv_url_minute", prop)
    })
    ssc.start()
    ssc.awaitTermination()
  }

  //case class Aliopen1Log(day:String, ip:String, method:String, request:String, protocol:String, status:String, bytes:String, referrer:String, OS:String, browser:String)
  //case class Aliopen1Log(day:String, ip:String, cookieid:String)
  case class Aliopen1Log(day: String, ip: String, method: String, request: String, protocol: String, status: String, bytes: String)

  object SQLContextSingleton {
    @transient private var instance: SQLContext = _

    def getInstance(sparkContext: SparkContext): SQLContext = {
      if (instance == null) {
        instance = new SQLContext(sparkContext)
      }
      instance
    }
  }
  //case class
  object parseDateFormat {
    //val FORMAT = new SimpleDateFormat("d/MMM/yyyy:HH:mm:ss", Locale.ENGLISH)
    //val format1 = new SimpleDateFormat("yyyyMMddHHmmss")
    //@transient private var date: Date = _
    def parseFormat(string: String): Date = {
      //@transient private val date: Date = _
      //val FORMAT = new SimpleDateFormat("d/MMM/yyyy:HH:mm:ss", Locale.ENGLISH)
      val FORMAT = new SimpleDateFormat("d/MMM/yyyy:HH:mm:ss", Locale.ENGLISH)

      var date: Date = new Date()
      date = FORMAT.parse(string)
      //format1.format(date)
      date
    }
    def parseTime(date: Date): String = {
      val format1 = new SimpleDateFormat("yyyyMMddHHmmss")
      format1.format(date)
    }
    //parse
  }

}
