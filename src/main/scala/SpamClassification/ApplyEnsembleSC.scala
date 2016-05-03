


package ca.uwaterloo.cs.evisoup.assignment6

//import io.bespin.scala.util.Tokenizer


import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import scala.math._


class Conf6c(args: Seq[String]) extends ScallopConf(args)  {
  mainOptions = Seq(input, model,output,method)
  val input = opt[String](descr = "input path", required = true)

  val model = opt[String](descr = "model", required = true)
  val output = opt[String](descr = "output", required = true)
  val method = opt[String](descr = "method", required = true)
}

object ApplyEnsembleSC {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf6c(argv)

    log.info("Input: " + args.input())
    log.info("Model: " + args.model())
    log.info("output: " + args.output())
    log.info("method: " + args.method())

    val conf = new SparkConf().setAppName("A6c")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val t1 = sc.textFile(args.model() + "/part-00000")
    val t2 = sc.textFile(args.model() + "/part-00001")
    val t3 = sc.textFile(args.model() + "/part-00002")

    val set1 = t1
      .map(line => {
        //(547993,2.019484093190069E-4)
        val token1 = line.split("\\(")
        val token2 = token1(1).split("\\)")
        val token3 = token2(0).split(",")
        ( token3(0).toInt ,token3(1).toDouble )
      })
      .collectAsMap
    val map1 = sc.broadcast( set1)
    //check :  cusMap.value.contains( ??? )

    val set2 = t2
      .map(line => {
        //(547993,2.019484093190069E-4)
        val token1 = line.split("\\(")
        val token2 = token1(1).split("\\)")
        val token3 = token2(0).split(",")
        ( token3(0).toInt ,token3(1).toDouble )
      })
      .collectAsMap
    val map2 = sc.broadcast( set2)


    val set3 = t3
      .map(line => {
        //(547993,2.019484093190069E-4)
        val token1 = line.split("\\(")
        val token2 = token1(1).split("\\)")
        val token3 = token2(0).split(",")
        ( token3(0).toInt ,token3(1).toDouble )
      })
      .collectAsMap
    val map3 = sc.broadcast( set3)

    val method = args.method()

    ///////////////////////////////////////////////////
    // Scores a document based on its list of features.
    def spamminess(features: Array[Int]) : Array[Double] = {
      var score1 = 0d
      var score2 = 0d
      var score3 = 0d
      features.foreach(f => {
          if (map1.value.contains(f)) score1 += map1.value(f)
          if (map2.value.contains(f)) score2 += map2.value(f)
          if (map3.value.contains(f)) score3 += map3.value(f)

      })
      val result = Array(score1,score2,score3)
      result
    }

    ///////////////////////////////////////////////////

    val inputFile = sc.textFile(args.input())

    val pred = inputFile.map(line =>{

      val eachLine = line.split(" ")
      val docid = eachLine(0).toString
      val features = eachLine.drop(2)

      val scoreArray = spamminess( features.map(_.toInt) )
      val s1 = scoreArray(0)
      val s2 = scoreArray(1)
      val s3 = scoreArray(2)
      var score = 0d
      var predict = "ham"

      if(method.equals("average") ){
        score = (s1 + s2 +s3) / 3
        if(score > 0) predict = "spam"
      }else{
        //vote
        if( ( (s1 > 0) && (s2 > 0) ) ||
            ( (s1 > 0) && (s3 > 0) ) ||
            ( (s2 > 0) && (s3 > 0) ) ){
          // 2 over 1, spam
          predict = "spam"

        }
        var spam = 0
        var ham = 0
        if(s1 > 0) spam = spam +1
        if(s2 > 0) spam = spam +1
        if(s3 > 0) spam = spam +1
        if(s1 <= 0) ham = ham +1
        if(s2 <= 0) ham = ham +1
        if(s3 <= 0) ham = ham +1
        score = spam - ham

      }


      (docid, eachLine(1), score, predict)

      })

    pred.saveAsTextFile(args.output())

  }
}