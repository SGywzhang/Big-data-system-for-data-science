import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import annotation.tailrec
import scala.reflect.ClassTag

/** A raw stackoverflow posting, either a question or an answer */
case class Posting(postingType: Int, id: Int, parentId: Option[Int], score: Int, tags: Option[String]) extends Serializable


/** The main class */
object Assignment2 extends Assignment2 {

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Assignment2")
  @transient lazy val sc: SparkContext = new SparkContext(conf)
  //sc.setLogLevel("WARN")
  //System.setProperty("hadoop.home.dir", "c:\\winutils")

  /** Main function */
  def main(args: Array[String]): Unit = {

    val lines   = sc.textFile("/Users/yaowenzhang/Desktop/CS5425/assignment2/Task2_data/QA_data.csv")
    val raw     = rawPostings(lines)
    val grouped = groupedPostings(raw)
    val scored  = scoredPostings(grouped)
    val vectors = vectorPostings(scored)


    val means   = kmeans(sampleVectors(vectors), vectors, debug = true)
    val results = clusterResults(means, vectors)
    printResults(results)
  }
}


/** The parsing and kmeans methods */
class Assignment2 extends Serializable {

  /** Domains */
  val Domains =
    List("Machine-Learning", "Compute-Science", "Algorithm", "Big-Data", "Data-Analysis", "Security", "Silicon Valley", "Computer-Systems",
      "Deep-learning", "Internet-Service-Providers", "Programming-Language", "Cloud-services", "Software-Engineering", "Embedded-System", "Architecture")


  //You should change these parameters to see the difference.

  /** K-means parameter: How "far apart" domains should be for the kmeans algorithm? */
  def DomainSpread = 50000

  /** K-means parameter: Number of clusters */
  def kmeansKernels = 45

  /** K-means parameter: Convergence criteria */
  def kmeansEta: Double = 20.0D

  /** K-means parameter: Maximum iterations */
  def kmeansMaxIterations = 120


  //
  //
  // Parsing utilities:
  //
  //

  /** Load postings from the given file */
  def rawPostings(lines: RDD[String]): RDD[Posting] =
    lines.map(line => {
      val arr = line.split(",")
      Posting(postingType =    arr(0).toInt,
        id =             arr(1).toInt,
        parentId =       if (arr(2) == "") None else Some(arr(2).toInt),
        score =          arr(3).toInt,
        tags =           if (arr.length >= 5) Some(arr(4).intern()) else None)
    })


  /** Group the questions and answers together */
  def groupedPostings(postings: RDD[Posting]): RDD[(Int, Iterable[(Posting, Posting)])] = {
    // Filter the questions and answers separately
    // Prepare them for a join operation by extracting the QID value in the first element of a tuple.
    val questions = postings
      .filter(_.postingType == 1)
      .map(posting => (posting.id, posting))

    val answers = postings
      .filter(_.postingType == 2)
      .filter(_.parentId.isDefined)
      .map(posting => (posting.parentId, posting))

    val answers_flattened = for ((Some(k), v) <- answers ) yield (k, v)

    // Use one of the join operations to obtain an RDD[(QID, (Question, Answer))].
    val joined = questions.join(answers_flattened)

    // Obtain an RDD[(QID, Iterable[(Question, Answer)])].
    joined.groupByKey()
  }


  /** Compute the maximum score for each posting */
  def scoredPostings(grouped: RDD[(Int, Iterable[(Posting, Posting)])]): RDD[(Posting, Int)] = {

    //toDo
    def answerHighScore(as: Iterable[Posting]): Int = as.map(_.score).max

    grouped.flatMap(_._2).groupByKey().mapValues(answerHighScore)
  }


  /** Compute the vectors for the kmeans */
  def vectorPostings(scored: RDD[(Posting, Int)]): RDD[(Int, Int)] = {

    //todo
    def firstDomainInTag(tag: String): Option[Int] = {
      val idx = Domains.indexOf(tag)
      if (idx >= 0) Some(idx) else None
    }

    val vectors = for {
      (posting, score) <- scored
      tag <- posting.tags
      idx <- firstDomainInTag(tag)
    } yield (idx * DomainSpread, score)


    vectors.persist()

  }


  /** Sample the vectors for kmeans*/
  def sampleVectors(vectors: RDD[(Int, Int)]): Array[(Int, Int)] = {

    assert(kmeansKernels % Domains.length == 0, "kmeansKernels should be a multiple of the number of Domains.")

    val perDomain = kmeansKernels / Domains.length

    //todo
    def reservoirSampling(domain: Int, iter: Iterator[Int], size: Int): Array[Int] = {
      val res = new Array[Int](size)
      val rnd = new util.Random(domain)

      for (i <- 0 until size) {
        assert(iter.hasNext, s"iterator must have at least $size elements")
        res(i) = iter.next
      }

      var i = size.toLong
      while (iter.hasNext) {
        val elt = iter.next
        val j = math.abs(rnd.nextLong) % i
        if (j < size)
          res(j.toInt) = elt
        i += 1
      }

      res
    }

    val res =
      if (DomainSpread < 500)
        vectors.takeSample(false, kmeansKernels, 42)
      else
        vectors.groupByKey.flatMap({
          case (domain, vectors) => reservoirSampling(domain, vectors.toIterator, perDomain).map((domain, _))
        }).collect()

    assert(res.length == kmeansKernels, res.length)
    res
  }


  //
  //
  //  Kmeans method:
  //
  //

  /** Main kmeans computation */
  @tailrec final def kmeans(means: Array[(Int, Int)], vectors: RDD[(Int, Int)], iter: Int = 1, debug: Boolean = false): Array[(Int, Int)] = {
    //toDo
    val newMeans = means.clone() // you need to compute newMeans


    // Side effects!
    vectors
      .map(
        vector => (findClosest(vector, means), vector)
      )
      .groupByKey()
      .mapValues(averageVectors)
      .collect()
      .foreach(pair => {
        newMeans.update(pair._1, pair._2)
      })

    val distance = euclideanDistance(means, newMeans)

    if (debug) {
      println(
        s"""Iteration: $iter
           |  * current distance: $distance
           |  * desired distance: $kmeansEta
           |  * means:""".stripMargin)
      for (idx <- 0 until kmeansKernels)
        println(f"   ${means(idx).toString}%20s ==> ${newMeans(idx).toString}%20s  " +
          f"  distance: ${euclideanDistance(means(idx), newMeans(idx))}%8.0f")
    }

    if (converged(distance))
      newMeans
    else if (iter < kmeansMaxIterations)
      kmeans(newMeans, vectors, iter + 1, debug)
    else {
      println("Reached max iterations!")
      newMeans
    }

  }




  //
  //
  //  Kmeans utilities:
  //
  //

  /** Decide whether the kmeans clustering converged */
  def converged(distance: Double) =
    distance < kmeansEta


  /** Return the euclidean distance between two points */
  def euclideanDistance(v1: (Int, Int), v2: (Int, Int)): Double = {
    val part1 = (v1._1 - v2._1).toDouble * (v1._1 - v2._1)
    val part2 = (v1._2 - v2._2).toDouble * (v1._2 - v2._2)
    part1 + part2
  }

  /** Return the euclidean distance between two points */
  def euclideanDistance(a1: Array[(Int, Int)], a2: Array[(Int, Int)]): Double = {
    assert(a1.length == a2.length)
    var sum = 0d
    var idx = 0
    while(idx < a1.length) {
      sum += euclideanDistance(a1(idx), a2(idx))
      idx += 1
    }
    sum
  }

  /** Return the closest point */
  def findClosest(p: (Int, Int), centers: Array[(Int, Int)]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity
    for (i <- 0 until centers.length) {
      val tempDist = euclideanDistance(p, centers(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }
    bestIndex
  }


  /** Average the vectors */
  def averageVectors(ps: Iterable[(Int, Int)]): (Int, Int) = {
    val iter = ps.iterator
    var count = 0
    var comp1: Long = 0
    var comp2: Long = 0
    while (iter.hasNext) {
      val item = iter.next
      comp1 += item._1
      comp2 += item._2
      count += 1
    }
    ((comp1 / count).toInt, (comp2 / count).toInt)
  }


  def computeMedian(a: Iterable[(Int, Int)]) = {
    val s = a.map(x => x._2).toArray
    val length = s.length
    val (lower, upper) = s.sortWith(_<_).splitAt(length / 2)
    if (length % 2 == 0) (lower.last + upper.head) / 2 else upper.head
  }


    def computeAverage(a: Iterable[(Int, Int)]):Double={
    val s = a.map(x => x._2).toArray
    val s_sorted = s.sortWith(_<_)
    val length = s_sorted.length
    s.sum / length
  }

  //
  //
  //  Displaying results:
  //
  //
  def clusterResults(means: Array[(Int, Int)], vectors: RDD[(Int, Int)]): Array[(String, Double, Int, Int,Double)] = {
    val closest = vectors.map(p => (findClosest(p, means), p))
    val closestGrouped = closest.groupByKey()

    val median = closestGrouped.mapValues { vs =>val DomainId: Int = vs.map(_._1).groupBy(identity).maxBy(_._2.size)._1 // most common domain in the cluster
    val DomainLabel: String   = Domains.apply(DomainId / DomainSpread) // most common domain in the cluster
    val clusterSize: Int    = vs.size
    val DomainPercent: Double = vs.count(v => v._1 == DomainId) * 100d / clusterSize // percent of the questions in the most common domain
    val medianScore: Int    = computeMedian(vs)
    val averageScore: Double = computeAverage(vs)

      (DomainLabel, DomainPercent, clusterSize, medianScore,averageScore)
    }

    median.collect().map(_._2).sortBy(_._4)
  }

  def printResults(results: Array[(String, Double, Int, Int,Double)]): Unit = {
    println("Resulting clusters:")
    println("  medianScore\taverageScore\tDominant Domain (%percent)\tQuestions")
    println("================================================")
    for ((domain, percent, size, medianScore,averageScore) <- results)
      println(f"${medianScore}%7d\t${averageScore}%.2f\t${domain}%-17s\t(${percent}%-5.1f%%)\t${size}%7d")
  }}
