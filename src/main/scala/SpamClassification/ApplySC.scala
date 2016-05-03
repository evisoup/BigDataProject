


package ca.uwaterloo.cs.evisoup.assignment6

//import io.bespin.scala.util.Tokenizer


import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import scala.math._


class Conf6b(args: Seq[String]) extends ScallopConf(args)  {
  mainOptions = Seq(input, model,output)
  val input = opt[String](descr = "input path", required = true)

  val model = opt[String](descr = "model", required = true)
  val output = opt[String](descr = "output", required = true)
}

object ApplySC {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf6b(argv)

    log.info("Input: " + args.input())
    log.info("Model: " + args.model())
    log.info("output: " + args.output())

    val conf = new SparkConf().setAppName("A6b")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.model() + "/part-00000")
    val cusSet = textFile
      .map(line => {
        //(547993,2.019484093190069E-4)
        val token1 = line.split("\\(")
        val token2 = token1(1).split("\\)")
        val token3 = token2(0).split(",")

        ( token3(0).toInt ,token3(1).toDouble )
      })
      .collectAsMap

    val cusMap = sc.broadcast( cusSet)
    //check :  cusMap.value.contains( ??? )


    // Scores a document based on its list of features.
    def spamminess(features: Array[Int]) : Double = {
      var score = 0d
      features.foreach(f => if (cusMap.value.contains(f)) score += cusMap.value(f))
      score
    }

    // This is the main learner:
    val delta = 0.002

    ///////////////////////////////////////////////////

    val inputFile = sc.textFile(args.input())

    val pred = inputFile.map(line =>{

      val eachLine = line.split(" ")
      val docid = eachLine(0).toString
      val features = eachLine.drop(2)
      var score = spamminess( features.map(_.toInt) )
      var predict = "ham"
      if(score > 0) predict = "spam"

      (docid, eachLine(1), score, predict)

      })

    pred.saveAsTextFile(args.output())

  }
}


