package spark.examples

import org.apache.spark.{SparkConf, SparkContext}
//import spark.examples.featurefu_wrapper

object SparkGrep {
	def main(args: Array[String]) {
//		if (args.length < 3) {
//			System.err.println("Usage: SparkGrep <host> <input_file> <match_term>")
//			System.exit(1)
//		}
		val ff = new df_test()
		ff.main(args)
//		val wraptest = new wrapTest()
//    wraptest.main()
		System.exit(0)
	}
}