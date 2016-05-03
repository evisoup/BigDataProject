
package ca.uwaterloo.cs.evisoup.assignment6

//import io.bespin.scala.util.Tokenizer


import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import scala.math._
import util.Random

class Conf6a(args: Seq[String]) extends ScallopConf(args)  {
  mainOptions = Seq(input, model, shuffle)
  val input = opt[String](descr = "input path", required = true)

  val model = opt[String](descr = "model", required = true)
  //val shuffle = opt[String](descr = "shuffle", required = false )
  val shuffle = toggle("shuffle")
}

object TrainSC {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf6a(argv)

    log.info("Input: " + args.input())
    log.info("Model: " + args.model())

    val conf = new SparkConf().setAppName("A6a")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.model())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

     var textFile = sc.textFile(args.input())

    // w is the weight vector (make sure the variable is within scope)
    //Map[Int, Double]()
    val w = scala.collection.mutable.HashMap[Int, Double]()

    // Scores a document based on its list of features.
    def spamminess(features: Array[Int]) : Double = {
      var score = 0d
      features.foreach(f => if (w.contains(f)) score += w(f))
      score
    }

    // This is the main learner:
    val delta = 0.002


    if( args.shuffle.isSupplied ){
       textFile = sc.textFile(args.input()).map( line=>{

        ( Random.nextInt, line)

      })
      .sortByKey(true)
      .map( x => {
          x._2
        })
    }

    val trained = textFile.map(line =>{
      //clueweb09-en0094-20-13546 spam 387908 697162
      // Parse input

      val eachLine = line.split(" ")
      var isSpam = 1
      if( eachLine(1).toString.equals("ham") ) isSpam = 0
      val docid = eachLine(0).toString
      val features = eachLine.drop(2)
      val featuresInt = features.map( x=> { x.toInt })

      (0, (docid, isSpam, featuresInt))
    })
      .groupByKey(1)
      // Then run the trainer....

      .flatMap( entry=>{

      //*begin
      entry._2.foreach( cell => {
        val isSpam = cell._2
        val features = cell._3


        var score = spamminess( features )
        val prob = 1.0 / (1 + exp(-score))

        features.foreach(f => {
          if (w.contains(f)) {
            w(f) += (isSpam - prob) * delta
          } else {
            w(f) = (isSpam - prob) * delta
          }
        })

      })
      //*end
      w
    })

    trained.saveAsTextFile(args.model())

    ///////////


  }
}



