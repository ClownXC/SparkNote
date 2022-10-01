import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName RDDPractice
 * @Description TODO
 * @Author joker
 * @Date 2022/9/30 22:24
 * @Version 1.0
 * */
object RDDPractice {

  def main(args: Array[String]): Unit = {
    val sparkConf  = new SparkConf().setMaster("local[*]").setAppName("RDDPractice")
//    sparkConf.set("spark.default.parallelism", "6")

    val sc = new SparkContext(sparkConf)


    val seq = Seq(1, 32, 2, 5, 6, 7)

//    val rdd: RDD[Int] = sc.parallelize(seq)
    val rdd: RDD[Int] = sc.makeRDD(seq, 3)

//    rdd.collect().foreach(println)
    rdd.saveAsTextFile("output")


    sc.stop()


  }

}
