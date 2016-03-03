

package ca.uwaterloo.cs.bigdata2016w.evisoup.assignment5

//import io.bespin.scala.util.Tokenizer


import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import scala.collection.mutable.Map
// --input TPC-H-0.1-TXT --date '1996-01-01'
// select count(*) from lineitem where l_shipdate = 'YYYY-MM-DD';

class Conf3(args: Seq[String]) extends ScallopConf(args)  {
  mainOptions = Seq(input, date)
  val input = opt[String](descr = "input path", required = true)

  val date = opt[String](descr = "given date", required = true)
}

object Q3 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf3(argv)

    log.info("Input: " + args.input())
    log.info(">>>>>>Date: " + args.date())

    val conf = new SparkConf().setAppName("Q3")
    val sc = new SparkContext(conf)
    val date = args.date()

    var sMap: Map[String,String] =  Map()
    val sFile = sc.textFile(args.input()+"/supplier.tbl")
   
    val sSet = sFile
      .map(line => {
        val tokens = line.split('|')
            //( tokens(0) ,  tokens(1) )
            //SUPPLY -> (supp key, name)
            ( tokens(0).toString ,tokens(1).toString )
            
           
      })
      .collectAsMap

     val broadcastSmap = sc.broadcast( sSet)
 

    var pMap: Map[String,String] =  Map()
    
    val pFile = sc.textFile(args.input()+"/part.tbl")
    val pSet = pFile
      .map(line => {
        val tokens = line.split('|')
            //( tokens(0) ,  tokens(1) )
            //PART -> (part key, name)
            ( tokens(0).toString ,  tokens(1).toString   )
            
      })
      .collectAsMap

    val broadcastPmap = sc.broadcast( pSet )

    

    val textFileTwo = sc.textFile(args.input()+"/lineitem.tbl")
    val secSet = textFileTwo
      .map(line => {
        val tokens = line.split('|')

//if( pMap.contains( tokens(1).toString ) && sMap.contains( tokens(2).toString) ){
        		
          (tokens(0).toInt,  tokens(10), tokens(1).toString, tokens(2).toString   )

           
           //order 0, part key 1, supp key 2, date10
           //(l_orderkey, date, p_name, s_name)

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

      

      .filter( p => {

          // println("   size:>>>>>>>>>>>>>")
          // println( broadcastSmap.value.size ) 
         if( broadcastPmap.value.contains(p._3) 
          && broadcastSmap.value.contains(p._4)  ){
        // if( pMap.contains(p._3) 
        //   && sMap.contains(p._4)  ){
              true
          }else{
            false
          }

       })

      


      .map( x => {

          //order 0, part key 1, supp key 2, date10
           //(l_orderkey, date, p_name, s_name)
      //     var one = x._2._1.toList
      //     var two = x._2._2.toList

      // for( i <- 0 to one.length-1 ) yield (x._1, (one(i).toString(), two(0) ))
        (x._1, (   broadcastPmap.value(x._3)  , broadcastSmap.value(x._4)))

      })

      .sortByKey()
      .take(20)
      .map( line => {
        (  line._1 , line._2._1, line._2._2)
      })
      .foreach(println)

  }
}
