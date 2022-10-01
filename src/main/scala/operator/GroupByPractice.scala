package operator

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Name GroupByPractice
 * @Description TODO
 * @Author joker
 * @Date 2021/10/1 10:32
 * @Version 1.0
 * */
object GroupByPractice {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("GroupByPractice")
    val sparkContext: SparkContext = new SparkContext(sparkConf)

//    val value: RDD[Int] = sparkContext.makeRDD(List(3, 4, 5, 6, 7, 8, 9), 3)
//    val value2: RDD[String] = sparkContext.makeRDD(List("Hello", "world", "scala", "start", "Helloworld", "word", "stop"), 3)
//    val groupByRDD: RDD[(Int, Iterable[Int])] = value.groupBy(_ % 2)
//    val groupByRDD1: RDD[(Char, Iterable[String])] = value2.groupBy(_.charAt(0))
//
//
//    groupByRDD.collect().foreach(println)
//    groupByRDD1.collect().foreach(println)

    val logRDD: RDD[String] = sparkContext.textFile("data/apache.log")
    val hourRDD: RDD[(Int, Int)] = logRDD.map(line => {
      val time: String = line.split(" ")(3)
      val sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
      val date: Date = sdf.parse(time)
      (date.getHours, 1)
    })
    val resRDD: RDD[(Int, Int)] = hourRDD.groupBy(_._1).map({
      case (hour, iter) => (hour, iter.size)
    })

    resRDD.collect().foreach(println)






    sparkContext.stop()

  }

}
