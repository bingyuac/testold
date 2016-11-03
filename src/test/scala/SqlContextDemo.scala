import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
/**
  * Created by andy on 16-10-20.
  */
object SqlContextDemo {

    def main(args: Array[String]): Unit = {
      println("SqlContextDemo")
      val conf=new SparkConf().setAppName("NetworkWordCount").setMaster("local[4]")
      val sc=new SparkContext(conf)
      val context=new SQLContext(sc)
      case class Person(name:String,age:Int)
      val rddPerson=sc.textFile("/home/andy/testSqlContext.txt").map(_.split(",")).map(p=>new Person(p(0),p(1).toInt))

      context.createDataFrame(rddPerson,Person.getClass).registerTempTable("rddtable");
      context.sql("select name from  rddtable").map(t=>"name:"+t(0)).collect().foreach(println)

    }

}
