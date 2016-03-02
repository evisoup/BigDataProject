package ca.uwaterloo.cs.bigdata2016w.evisoup.assignment5

//import io.bespin.scala.util.Tokenizer


import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

// --input TPC-H-0.1-TXT --date '1996-01-01'
// select count(*) from lineitem where l_shipdate = 'YYYY-MM-DD';

class Conf(args: Seq[String]) extends ScallopConf(args)  {
  mainOptions = Seq(input, date)
  val input = opt[String](descr = "input path", required = true)
  // val output = opt[String](descr = "output path", required = true)
  //val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val date = opt[String](descr = "given date", required = true)
}

object Q1 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info(">>>>>>Date: " + args.date())

    val conf = new SparkConf().setAppName("Q1")
    val sc = new SparkContext(conf)
    val date = args.date()

    // val outputDir = new Path(args.output())
    // FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input()+"/lineitem.tbl")
    val counts = textFile
      .flatMap(line => {
        val tokens = line.split('|')
        if (tokens.length > 11) {
            //1999
            //1999-04
            //1999-04-05
            if( tokens(10) == date.toString() ){
              List("match")
            }else
            if( tokens(10).substring(0,7) == date.toString() ){
              List("match")
            }else
            if( tokens(10).substring(0,4) == date.toString() ){
              List("match")
            }
            else List()

        }else List()
      })
      // .map(bigram => (bigram, 1))
      // .reduceByKey(_ + _)
      .count()
    println("ANSWER=" + counts )  

    //println(counts.toString())

  }
}
