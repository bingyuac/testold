import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * Created by andy on 16-10-19.
  */
object HTest {
  def main(args: Array[String]) {
    val sparkContext = new SparkContext("local[4]", "HiveTable")
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sparkContext)
    val sqlContext = new SQLContext(sparkContext)
    //println("xxxxxxxxxxxxx")
    //hiveContext.setConf("hive.metastore.warehouse.dir", "hdfs://192.168.1.111:9111/user/hive/warehouse")
    //hiveContext.sql("CREATE TABLE IF NOT EXISTS test1 (key INT, value STRING)")
    //hiveContext.sql("select * from a1").show()

    val jdbcDF = sqlContext.load( "jdbc",Map("url" -> "jdbc:mysql://open003:3306/hive?user=hive&password=hive", "dbtable" -> "hive.TBLS","driver" -> "com.mysql.jdbc.Driver"))
    jdbcDF.show()

    }
  }
