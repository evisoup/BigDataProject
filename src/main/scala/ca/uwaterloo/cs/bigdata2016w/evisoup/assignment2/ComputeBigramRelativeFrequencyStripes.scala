package ca.uwaterloo.cs.bigdata2016w.evisoup.assignment2

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

import org.apache.spark.Partitioner
import scala.collection.Map

// class Conf(args: Seq[String]) extends ScallopConf(args) with Tokenizer {
//   mainOptions = Seq(input, output, reducers)
//   val input = opt[String](descr = "input path", required = true)
//   val output = opt[String](descr = "output path", required = true)
//   val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
// }


class mySecondPartitioner(partitions: Int) extends Partitioner {
  override def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[String]

    return ((k.split(" ").head.hashCode & Int.MaxValue) % numPartitions).toInt
  }
}

object ComputeBigramRelativeFrequencyStripes extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf().setAppName("ComputeBigramRelativeFrequencyStripes")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())
    val counts = textFile

      .flatMap( line => {
        val tokens = tokenize(line)


        if (tokens.length > 1) {

          tokens.sliding(2).map( p=> {

            var aa = scala.collection.mutable.Map[String, Int]()

            aa.put(p(1), 1)

            (p(0), aa)//.toList

            }).toList

        }
        else List()

      })


      //.reduceByKey(_ + _)
       .reduceByKey( (x,y) => {

          val combined = (x /: y) { case (map, (k,v)) =>
                                          map + ( k -> (v + map.getOrElse(k, 0)) )
                                }

          combined

        } )
       .sortByKey()
       .partitionBy(new mySecondPartitioner(args.reducers())  )


      .mapPartitions(x => { 

        x.map( y => {
            var sum = 0.0f
            y._2 foreach {case (key, value) =>  (sum = sum + value)}


            val incM = y._2 map {  case (key, value) =>  { 
                      (key, value/sum )
                      
            }  }

            (y._1, incM)

          }
        )
      })

    counts.saveAsTextFile(args.output())
  }
}