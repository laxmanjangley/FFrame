package spark.examples
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import spark.feature.{ExprEval, VecTransformer}
import scala.io.Source
import scala.collection.mutable
import scala.collection.mutable.Map

/**
  * Created by root on 24/6/16.
  */

object SparkExp {

  implicit class Regex(sc: StringContext) {
    def r = new scala.util.matching.Regex(sc.parts.mkString, sc.parts.tail.map(_ => "x"): _*)
  }

  object tokenType extends Enumeration {
    type tokenType = Value
    val number, variable, string, op, rparen, lparen, notok, comma, ws = Value
  }

  import tokenType._

  val tokenize: String => Array[(Object, Object)] = (expression) => {
    val input = expression.toCharArray
    var pos: Int = 0
    val getNextToken = () => {
      var current: Array[Char] = Array()
      var flag = true
      var token: (tokenType, String) = null
      while (flag) {
        if (pos >= input.length) {
          flag = false
        } else {
          current = current :+ input(pos)
          pos += 1
          current.mkString match {
            case r"^[-+]?\d*\.?\d*" => token = (number, current.mkString)
            case r"[a-zA-Z_][a-zA-Z0-9_]{0,31}" => token = (variable, current.mkString)
            case r"[#a-zA-Z_][a-zA-Z0-9_]{0,31}" => token = (op, current.mkString)
            //TODO string case here, regex fucks up
            //            case  => token = (string, current.toString)
//            case r"[\"].*[\"]" => (string, current.mkString)
            case r"[(]" => token = (lparen, current.mkString)
            case r"[)]" => token = (rparen, current.mkString)
            case r"[,]" => token = (comma, ",")
            case " " => token = (ws, " ")
            case _ => flag = false; pos -= 1; current = Array();
          }
        }
      }
      token
    }
    var res: Array[(Object, Object)] = Array()
    while (pos < input.length) {
      val token = getNextToken().asInstanceOf[(Object, Object)]
      res :+= token
    }
    res
  }

  val parse = (fenv: mutable.Map[String, Object]) => (tok : Array[(Object, Object)]) =>  (env: Map[String, Object]) => {
    val vstack = new mutable.Stack[Object]
    val fstack = new mutable.Stack[String]
    //      val tok = tokenize(exp)
    var args: Array[Object] = Array()
    tok.asInstanceOf[Array[(tokenType,  String)]] foreach {
      case (`number`, n) => vstack.push(Double.box(n.toFloat))
      case (`variable`, v) => vstack.push(env(v))
      case (`op`, f) => fstack.push(f)
      case (`lparen`, _) => vstack.push(null)
      case (`string`, s) => vstack.push(s.asInstanceOf[Object])
      case _ => //chill
    }
    while (!vstack.isEmpty) {
      val i = vstack.pop()
      if (i != null) args :+= i
      else {
        if (!fstack.isEmpty) {
          val f = fenv(fstack.pop()).asInstanceOf[Array[Object] => Object]
          vstack.push(f(args.reverse))
          args = Array()
        }
      }
    }
    args(0)
  }

  def main (arg : Array[String]): Unit = {


    val conf = new SparkConf().setAppName("dataframe_test").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("/home/laxman.jangley/project/FFrame/test.csv")

    type Environment = Map[String, Object]
    class functions extends Serializable {
      val f: (Array[Object] => Object) = {
        case (Array(x)) => x
        case x: Array[Object] => (x(0).asInstanceOf[Int] * f(x.drop(1)).asInstanceOf[Int]).asInstanceOf[Object]
      }
      val g: (Array[Object] => Object) = {
        case (Array(x)) => x.asInstanceOf[String]
        case x: Array[Object] => x(0).asInstanceOf[String] + g(x.drop(1)).asInstanceOf[String]
      }
    }

    def time[R](block: => R): R = {
      val t0 = System.nanoTime()
      val result = block
      val t1 = System.nanoTime()
      println("Elapsed time: " + (t1 - t0) + "ns")
      result
    }

    var env: Environment = Map()
    val z = new functions()


  env("#f") = (z.f.asInstanceOf[Object])

    val lines = Source.fromFile("/home/laxman.jangley/project/FFrame/example").getLines.toSeq.map(l => (l.split('=')(0).trim, l.split('=')(1).trim))
//    lines.foreach(println)
    //    env.foreach {case (x,y) => println(x)}
    //    System.exit(0)
    val ff = new ExprEval()
      .setFunction(parse(env) )
      .setInputCols(Seq("a", "b", "c"))
      .setNumFeatures(5)
      .setTt(tokenize)
      .setoutputTuples(lines)
    time  {ff.transform(df).show(20)}

//    val gg = new VecTransformer()
//      .setTree(null)
//      .setNumFeatures((5))
//      .setOutputCol("relax")
//      .setInputCols(Seq())
//      .setFunction(null)
//    time {gg.transform(df).show(20)}
//    tokenize("\"laxman\"")
  }
}