import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by andy on 16-10-17.
  */
object MysqlTest {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]")

    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._

    val df = sc.makeRDD(1 to 5).map(i => ("open01", i, i * 2)).toDF("aasingle","aadouble","xxxx")

    val dfWriter = df.write.mode("append")

    val prop = new Properties()

    prop.put("user","root")
    prop.put("password", "mysql")
    dfWriter.jdbc("jdbc:mysql://60.205.152.16:3306/test","test2",prop)

  }

}

