package spark.examples

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
//import spark.examples.featurefu_wrapper

object SparkGrep {
	def main(args: Array[String]) {
		if (args.length < 3) {
			System.err.println("Usage: SparkGrep <host> <input_file> <match_term>")
			System.exit(1)
		}
		val ff = new featurefu_wrapper()
		ff.main(args)
		System.exit(0)
	}
}