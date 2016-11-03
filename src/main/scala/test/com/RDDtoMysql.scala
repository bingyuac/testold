package test.com
import java.sql.{DriverManager, PreparedStatement, Connection}
import org.apache.spark.{SparkContext, SparkConf}
/**
  * Created by andy on 16-10-17.
  */
object RDDtoMysql {

  case class Blog(name: String, count: Int)

  def myFun(iterator: Iterator[(String, Int)]): Unit = {
    var conn: Connection = null
    var ps: PreparedStatement = null
    val sql = "insert into blog(name, count) values (?, ?)"
    try {
      conn = DriverManager.getConnection("jdbc:mysql://60.205.152.16:3306/test", "root", "mysql")
      iterator.foreach(data => {
        ps = conn.prepareStatement(sql)
        ps.setString(1, data._1)
        ps.setInt(2, data._2)
        ps.executeUpdate()
      }
      )
    } catch {
      case e: Exception => println("Mysql Exception")
    } finally {
      if (ps != null) {
        ps.close()
      }
      if (conn != null) {
        conn.close()
      }
    }
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("RDDToMysql").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val data = sc.parallelize(List(("hadoop", 10), ("hello", 20), ("你好", 30)))
    data.foreachPartition(myFun)
  }
}
