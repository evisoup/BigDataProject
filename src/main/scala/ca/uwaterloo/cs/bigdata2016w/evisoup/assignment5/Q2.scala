
package ca.uwaterloo.cs.bigdata2016w.evisoup.assignment5

//import io.bespin.scala.util.Tokenizer


import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

// --input TPC-H-0.1-TXT --date '1996-01-01'
// select count(*) from lineitem where l_shipdate = 'YYYY-MM-DD';

class Conf2(args: Seq[String]) extends ScallopConf(args)  {
  mainOptions = Seq(input, date)
  val input = opt[String](descr = "input path", required = true)
  // val output = opt[String](descr = "output path", required = true)
  //val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val date = opt[String](descr = "given date", required = true)
}

object Q2 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf2(argv)

    log.info("Input: " + args.input())
    log.info(">>>>>>Date: " + args.date())

    val conf = new SparkConf().setAppName("Q2")
    val sc = new SparkContext(conf)
    val date = args.date()

    val textFile = sc.textFile(args.input()+"/orders.tbl")
    val firstSet = textFile
      .map(line => {
        val tokens = line.split('|')
            ( tokens(0).toInt, tokens(6) )
            //(orderkey, clerk)
      })

    val textFileTwo = sc.textFile(args.input()+"/lineitem.tbl")
    val secSet = textFileTwo
      .map(line => {
        val tokens = line.split('|')
            // if( tokens(10) == date.toString() ||
            //     tokens(10).substring(0,7) == date.toString() ||
            //     tokens(10).substring(0,4) == date.toString() ){
                 ( tokens(0).toInt, tokens(10) )
            //}

      })
      .filter( p => {

         if( p._2 == date.toString() ||
             p._2.substring(0,7) == date.toString() ||
             p._2.substring(0,4) == date.toString() ){
              true
          }else{
            false
          }

        })
      // .count()
      // println("ANSWER>>>>>>>>>>" + secSet )  

      .cogroup(firstSet)

      .filter( p => {
         if( p._2._1.toList != Nil ){
              true
          }else{
            false
          }
      })

      .flatMap( x => {

          var one = x._2._1.toList
          var two = x._2._2.toList

          for( i <- 0 to one.length-1 ) yield (x._1, (one(i).toString(), two(0) ))

      })

      .sortByKey()
      .take(20)
      .map( line => {
        ( line._2._2 , line._1 )
      })
      .foreach(println)

  }
}
