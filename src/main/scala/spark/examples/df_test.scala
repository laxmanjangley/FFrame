package spark.examples



import com.linkedin.featurefu.expr.{Expression, VariableRegistry}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import spark.feature.ExprTransformer

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
//    df.show(10)
    val calc : String => Double = (exp  : String) => {
      val vr = new VariableRegistry()
      val expression = Expression.parse(exp, vr)
      expression.evaluate()
    }
    def cal (exp :  String, vars : Seq[String] , vals : Seq[Double]) = {
      val vr = new VariableRegistry()
      vals.zipWithIndex foreach {case (v,i) => exp.replace(vars(i), v.toString)}
      val expr = Expression.parse(exp, vr)
      expr.evaluate()
    }
    val ff = new FeatureFuTransformer()
      .setInputCol("a")
      .setExpr("(+ a b)")
      .setOutputCol("f")
      .setInputCols(Seq("a","b"))
      .setNumFeatures(5)
      .setFunction(calc)
    var t0 = System.nanoTime()
    ff.transform(df)
    var t1 = System.nanoTime()
    println("Elapsed time 1: " + (t1 - t0) + "ns")
//    val x = ff.transform(df)
//    x.show(10)
    val gg = new ExprTransformer()
      .setInputCols(Seq("a", "b"))
      .setExpr("(+ a b)")
      .setOutputCol("myResult")
      .setNumFeatures(5)
      .setFunction(calc)
    t0 = System.nanoTime()
    gg.transform(df).show(20)
    t1 = System.nanoTime()
    println("Elapsed time 2: " + (t1 - t0) + "ns")

    //    val inputFile = sc.textFile(args(1), 2).cache()
//    val matchTerm : String = args(2)
//    val numMatches = inputFile.filter(line => line.contains(matchTerm)).count()
//    println("%s lines in %s contain %s".format(numMatches, args(1), matchTerm))
  }
}
