package spark.examples

import com.linkedin.featurefu.expr.{Expression, VariableRegistry}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import spark.feature.{ExprTransformer, VecTransformer}
import spark.examples._

import scala.collection.mutable.Map

/**
  * Created by laxman.jangley on 25/5/16.
  */
class df_test {
  val calc : java.lang.String => Double = (exp  : java.lang.String) => {
    val vr = new VariableRegistry()
    val expression = Expression.parse(exp, vr)
    expression.evaluate()
  }

  def main(args : Array[String]) {
    println("im ini\n")
    val conf = new SparkConf().setAppName("dataframe_test").setMaster(args(0))
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("/home/laxman.jangley/project/FFrame/test.csv")


//    def cal (exp :  String, vars : Seq[String] , vals : Seq[Double]) = {
//      val vr = new VariableRegistry()
//      vals.zipWithIndex foreach {case (v,i) => exp.replace(vars(i), v.toString)}
//      val expr = Expression.parse(exp, vr)
//      expr.evaluate()
//    }
//
//    val ff = new ExpressionEval()
//      .setExpr("(+ a (* b (+ c d)))")
//      .setOutputCol("f")
//      .setNumFeatures(5)
//      .setFunction(calc)
//
//    var t0 = System.nanoTime()
//    ff.transform(df)
//    var t1 = System.nanoTime()
//    println("Elapsed time 1: " + (t1 - t0) + "ns")
//
    val gg = new ExprTransformer()
      .setExpr("(+ a (* b (+ c d)))")
      .setOutputCol("f")
      .setInputCols(Seq("a","b", "c", "d"))
      .setNumFeatures(5)
      .setFunction(calc)
//    t0 = System.nanoTime()
//    gg.transform(df)
//    t1 = System.nanoTime()
//    println("Elapsed time 2: " + (t1 - t0) + "ns")

    type Environment = Map[String, Object]
    class functions{
      val f : (Array[Object] => Object) = x => x match {
        case (Array(x)) => x
        case  x : Array[Object] => (x(0).asInstanceOf[Double] + f(x.drop(1)).asInstanceOf[Double]).asInstanceOf[Object]
      }
      val g : (Array[Any] => Any) = x => x match {
        case (Array(x)) => x.asInstanceOf[String]
        case x: Array[Any] => x(0).asInstanceOf[String] + g(x.drop(1)).asInstanceOf[String]
      }
    }
    def eval(tree:Object, env:  Map[String, Object]): Object = tree match {
      case Op(x, f) => f(x.map(xs => eval(xs, env)))
      case Var(v) => env(v)
      case Const(v) => v
    }
    val z = new functions
    val exp = Op(Array(Var("a"), Var("b")), z.f)
//    var env1: Map[String, Object] = Map()
//    env1 += ("x" -> Double.box(1))
//    env1 += ("y" -> Double.box(2))
//    env1 += ("xx" -> "laxman".asInstanceOf[Object])
//    env1 += ("yy" -> "jangley".asInstanceOf[Object])
    val ff = new VecTransformer()
      .setFunction(eval)
      .setInputCols(Seq("a", "b"))
      .setOutputCol("relax")
      .setNumFeatures(5)
      .setTree(exp)
    ff.transform(df).show(20)
  }
}
