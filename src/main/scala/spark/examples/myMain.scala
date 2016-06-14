package spark.examples

import org.apache.spark.{SparkConf, SparkContext}


import scala.collection.mutable.Map


abstract class Tree

case class Op(args:Array[Tree], f:Array[Object]=>Object) extends Tree
case class Var(v : String) extends Tree
case class Const(v : Object) extends Tree




object myMain {
	def main(args: Array[String]) {
//		val z = new functions
//		val exp = Op(Array(Var("x"), Var("y")), z.f)
//		var env1: Map[String, Object] = Map()
//		env1 += ("x" -> Double.box(1))
//		env1 += ("y" -> Double.box(2))
//		env1 += ("xx" -> "laxman".asInstanceOf[Object])
//		env1 += ("yy" -> "jangley".asInstanceOf[Object])
//		val exp1 = Op(Array(Var("xx"), Var("yy")), z.g)
//		println(eval(exp1, env1))
//		System.exit(0)
		val x = new df_test
		x.main(args)
	}
}