package spark.examples

import spark.examples.{Const, Op, Tree, Var}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import spark.feature.{ExprTransformer, VecTransformer}
import spark.examples.Parser
import scala.collection.mutable
import scala.collection.mutable.Map







object myMain {
	def main(args: Array[String]) {
//		val conf = new SparkConf().setAppName("dataframe_test").setMaster(args(0))
//		val sc = new SparkContext(conf)
//		val sqlContext = new SQLContext(sc)
//		val df = sqlContext.read
//			.format("com.databricks.spark.csv")
//			.option("header", "true")
//			.option("inferSchema", "true")
//			.load("/home/laxman.jangley/project/FFrame/test.csv")

		type Environment = Map[String, Object]
		class functions extends Serializable{
			val f : (Array[Object] => Object) = {
				case (Array(x)) => x
				case  x : Array[Object] => (x(0).asInstanceOf[Int] + f(x.drop(1)).asInstanceOf[Int]).asInstanceOf[Object]
			}
			val g : (Array[Object] => Object) = {
				case (Array(x)) => x.asInstanceOf[String]
				case x: Array[Object] => x(0).asInstanceOf[String] + g(x.drop(1)).asInstanceOf[String]
			}
		}
		val eval : (Object => (Map[String,Object] => Object)) = tree => env => {
			val ostack = new mutable.Stack[Object]
			val fstack = new mutable.Stack[Array[Object]=>Object]
			ostack.push(tree)
//			println(tree.asInstanceOf[Const])
			var args : Array[Object] = Array()
			while(!ostack.isEmpty) {
				val i = ostack.pop()
//				println(i)
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
//		val z = new functions
		var env:Environment = Map()
		env += ("a" -> Double.box(100))
//		val exp = Op(Array(Var("a"), Var("b")), z.f)
//		val e = Var("a");
//		val ff = new VecTransformer()
//			.setFunction(eval )
//			.setInputCols(Seq("a", "b"))
//			.setOutputCol("relax")
//			.setNumFeatures(5)
//			.setTree(exp)
//		ff.transform(df).show(20)
//		println(eval (exp) (env))
		val t = new Parser{
//			var grammar = Map()
//			var mapping = Map()
			var expression = "#plus(x,y)"
		}
//		t.tokenize().foreach(x => println(x._2))
		println(t.parse())
//		println(eval (t.parse()) (env))
//		x match {
//			case Op(x, f) => x.foreach(i => println(i))
//		}

	}
}