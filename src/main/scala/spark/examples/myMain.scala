package spark.examples

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import spark.feature.{ExprTransformer, VecTransformer}

import scala.collection.mutable
import scala.collection.mutable.Map


abstract class Tree extends Serializable

case class Op(args:Array[Tree], f:Array[Object]=>Object) extends Tree
case class Var(v : String) extends Tree
case class Const(v : Object) extends Tree




object myMain {
	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("dataframe_test").setMaster(args(0))
		val sc = new SparkContext(conf)
		val sqlContext = new SQLContext(sc)
		val df = sqlContext.read
			.format("com.databricks.spark.csv")
			.option("header", "true")
			.option("inferSchema", "true")
			.load("/home/laxman.jangley/project/FFrame/test.csv")

		type Environment = Map[String, Object]
		class functions extends Serializable{
			val f : (Array[Object] => Object) = {
				case (Array(x)) => x
				case  x : Array[Object] => (x(0).asInstanceOf[Double] + f(x.drop(1)).asInstanceOf[Double]).asInstanceOf[Object]
			}
			val g : (Array[Object] => Object) = {
				case (Array(x)) => x.asInstanceOf[String]
				case x: Array[Object] => x(0).asInstanceOf[String] + g(x.drop(1)).asInstanceOf[String]
			}
		}
		object lol extends Serializable {
			def func (tree:Object) (env:  Map[String, Object]): Object = tree match {
				case Op(x, f) => f(x.map(xs => func(xs) (env)))
				case Var(v) => env(v)
				case Const(v) => v
			}
		}
		val eval : (Object => (Map[String,Object] => Object)) = tree => env => {
			val ostack = new mutable.Stack[Object]
			val fstack = new mutable.Stack[Array[Object]=>Object]
			ostack.push(tree)
			var args : Array[Object] = Array()
			while(!ostack.isEmpty) {
				val i = ostack.pop()
				if (i != null) {
					i match {
						case Var(x) => args = args :+ env(x)
						case Const(x) => args = args :+ x
						case Op(x, f) => {
							fstack.push(f)
							ostack.push(null)
							x.foreach(i => ostack.push(i))
						}
					}
				} else {
					if (!fstack.isEmpty) {
						val f = fstack.pop()
						ostack.push(Const(f(args)))
						args = Array()
					}
				}
			}
			args(0)
		}
		val z = new functions
		var env:Environment = Map()
		env += ("a" -> Double.box(100))
		val exp = Op(Array(Var("a"), Const(Double.box(1))), z.f)
		val e = Var("a");
		val ff = new VecTransformer()
			.setFunction(eval )
			.setInputCols(Seq("a", "b"))
			.setOutputCol("relax")
			.setNumFeatures(5)
			.setTree(exp)
		ff.transform(df).show(20)
		println(eval (exp) (env))
	}
}