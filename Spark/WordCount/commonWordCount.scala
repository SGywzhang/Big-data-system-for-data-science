import org.apache.spark.{SparkConf, SparkContext}

object wordCount {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("wordcount")
    val sc = new SparkContext(conf)

    val input1 = sc.textFile("/Users/yaowenzhang/Desktop/assignment1/Task1_data/task1-input1.txt")
    val input2 = sc.textFile("/Users/yaowenzhang/Desktop/assignment1/Task1_data/task1-input2.txt")
    val stopWords = sc.textFile("/Users/yaowenzhang/Desktop/assignment1/Task1_data/stopwords.txt").collect.seq

    val flatMapInput1 = input1.flatMap(line => line.split(" ")).filter(x => !stopWords.contains(x) && x!="")
    val flatMapInput2 = input2.flatMap(line => line.split(" ")).filter(x => !stopWords.contains(x) && x!="")

    val mapReduce1 = flatMapInput1.map(word => (word, 1)).reduceByKey(_+_)
    val mapReduce2 = flatMapInput2.map(word => (word, 1)).reduceByKey(_+_)

    val joinedRDD = mapReduce1.join(mapReduce2)

    val commonWords = joinedRDD.mapValues(x=>List(x._1,x._2).min)

    val top15SortedCommonWords = commonWords.map(commonWords=>commonWords.swap).sortByKey(false,1).take(15)

    val top15RDD = sc.parallelize(top15SortedCommonWords)

    top15RDD.saveAsTextFile("/Users/yaowenzhang/Desktop/spark")
  }
}
