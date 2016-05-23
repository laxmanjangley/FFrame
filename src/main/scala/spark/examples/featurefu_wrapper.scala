package spark.examples

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql
import org.apache.spark.sql._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types
import org.apache.spark.sql.types._


/**
  * Created by laxman.jangley on 24/5/16.
  */
class featurefu_wrapper {
  def main(args : Array[String]) {
    println("im ini\n");
    val conf = new SparkConf().setAppName("SparkGrep").setMaster(args(0))
    val sc = new SparkContext(conf)
    val inputFile = sc.textFile(args(1), 2).cache()
    val matchTerm : String = args(2)
    val numMatches = inputFile.filter(line => line.contains(matchTerm)).count()
    println("%s lines in %s contain %s".format(numMatches, args(1), matchTerm))
  }
}
