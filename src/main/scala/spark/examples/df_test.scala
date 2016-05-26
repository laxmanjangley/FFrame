package spark.examples

import com.linkedin.featurefu.expr.{Expression, VariableRegistry}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.{Row, SQLContext}
import spark.examples.feature.FeatureFuTransformer

/**
  * Created by laxman.jangley on 25/5/16.
  */
class df_test {
  def main(args : Array[String]) {
    println("im ini\n")
    val conf = new SparkConf().setAppName("dataframe_test").setMaster(args(0))
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load("test.csv")
    df.show(10)
    val calc : String => Double = (exp  : String) => {
      val vr = new VariableRegistry()
      val expression = Expression.parse(exp, vr)
      expression.evaluate()
    }
    val ff = new FeatureFuTransformer()
      .setInputCol("a")
      .setExpr("(+ a (+ b c))")
      .setOutputCol("f")
      .setInputCols(Seq("a","b","c"))
      .setNumFeatures(5)
      .setFunction(calc)
    val x = ff.transform(df)
    x.show(10)

    //    val inputFile = sc.textFile(args(1), 2).cache()
//    val matchTerm : String = args(2)
//    val numMatches = inputFile.filter(line => line.contains(matchTerm)).count()
//    println("%s lines in %s contain %s".format(numMatches, args(1), matchTerm))
  }
}
