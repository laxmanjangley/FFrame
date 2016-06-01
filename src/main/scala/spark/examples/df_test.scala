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
      .option("header", "true")
      .option("inferSchema", "true")
      .load("test.csv")

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

    val ff = new Expr()
      .setExpr("(+ a (* b (+ c d)))")
      .setOutputCol("f")
      .setNumFeatures(5)
      .setFunction(calc)

    var t0 = System.nanoTime()
    ff.transform(df)
    var t1 = System.nanoTime()
    println("Elapsed time 1: " + (t1 - t0) + "ns")

    val gg = new ExprTransformer()
      .setExpr("(+ a (* b (+ c d)))")
      .setOutputCol("f")
      .setInputCols(Seq("a","b", "c", "d"))
      .setNumFeatures(5)
      .setFunction(calc)
    t0 = System.nanoTime()
    gg.transform(df)
    t1 = System.nanoTime()
    println("Elapsed time 2: " + (t1 - t0) + "ns")
  }
}
