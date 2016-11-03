package test.com
import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * Created by andy on 16-9-28.
  */
object DapLogStreaming {

  def main (args : Array[String]) {
    //val sparkConf = new SparkConf().setMaster("yarn-client").setAppName("LogStreaming")spark://open003:7077
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("LogStreaming")
    //每60秒一个批次
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    //从Kafka中读取数据，topic为daplog，该topic包含两个分区
    val kafkaStream = KafkaUtils.createStream(
      ssc,
      "60.205.152.16:2181", //Kafka集群使用的zookeeper
      "test-consumer-group", //该消费者使用的group.id
      Map[String, Int]("test" -> 1), //日志在Kafka中的topic及其分区
      StorageLevel.MEMORY_AND_DISK_SER)
      //.map(x => x._2.split(" ", -1))  //日志以|~|为分隔符
      .map(x => x._2.split("\\|~\\|", -1))
      kafkaStream.foreachRDD((rdd: RDD[Array[String]], time: Time) => {
      val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
      import sqlContext.implicits._
      //构造case class: DapLog,提取日志中相应的字段
        //2015-11-11T14:59:59|~|xxx|~|202.109.201.181|~|xxx|~|xxx|~|xxx|~|B5C96DCA0003DB546E7
      //val logDataFrame = rdd.map(w => DapLog(w(0).substring(0, 10),w(2),w(6))).toDF()
      //								date     					ip	method				 request		protocol		status bytes referrer				OS		browser
      //Sep 27 16:13:02 0 TestFromOpen2 2016-09-27T16:13:16.270705+08:00 open2
      ////192.168.1.141 - - [27/Sep/2016:16:13:07 +0800] "GET / HTTP/1.1" 403 4961 "-" "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36"
      //2016-10-13T13: 54:59.692469+08:00 open1 Apache  - [aliopen1@001 tag="Aliopen1Test"] 207.46.13.108 - - [13/Oct/2016:13:53:33 +0800] "GET / HTTP/1.1" 200 19490
      //val logDataFrame = rdd.map(w => new DapLog(w(10).substring(1, 20),w(7),w(12).substring(1,3),w(13),w(14).replace("\"",""),w(15),w(16),w(17).replace("\"",""),w(20),w(23))).toDF()
      val logDataFrame = rdd.map(w => new DapLog(w(0).substring(0, 10),w(2),w(6))).toDF()
        //注册为tempTable
      logDataFrame.registerTempTable("daplog")
      //查询该批次的pv,ip数,uv
      val logCountsDataFrame =
      //sqlContext.sql("select date_format(current_timestamp(),'yyyy-MM-dd HH:mm:ss') as time,count(1) as pv,count(distinct ip) as ips,count(distinct cookieid) as uv from daplog")
      sqlContext.sql("select date_format(current_timestamp(),'yyyy-MM-dd HH:mm:ss') as time,count(1) as pv,count(distinct ip) as ips from daplog")
      //打印查询结果

      logCountsDataFrame.show()

      val dfWriter = logCountsDataFrame.write.mode("append")
      val prop = new Properties()
      prop.put("user","root")
      prop.put("password", "mysql")
      dfWriter.jdbc("jdbc:mysql://60.205.152.16:3306/test","test",prop)
    })

    ssc.start()
    ssc.awaitTermination()

  }
  //case class DapLog(day:String, ip:String, method:String, request:String, protocol:String, status:String, bytes:String, referrer:String, OS:String, browser:String)
  case class DapLog(day:String, ip:String, cookieid:String)
  object SQLContextSingleton {
    @transient  private var instance: SQLContext = _
    def getInstance(sparkContext: SparkContext): SQLContext = {
      if (instance == null) {
        instance = new SQLContext(sparkContext)
      }
      instance
    }
}
  //val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)
  //val module = runtimeMirror.staticModule("scala.collection.mutable.HashMap")
  //val obj = runtimeMirror.reflectModule(module)
  //println(obj.instance)

}
