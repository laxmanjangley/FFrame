package spark.examples

import scala.collection.mutable

/**
  * Created by root on 17/6/16.
  */
abstract class Tree extends Serializable

case class Op(args:Array[Tree], f:Array[Object]=>Object) extends Tree {
  def getArgs() = args
}
case class Var(v : String) extends Tree
case class Const(v : Object) extends Tree



abstract class Parser extends Serializable {
  implicit class Regex(sc: StringContext) {
    def r = new scala.util.matching.Regex(sc.parts.mkString, sc.parts.tail.map(_ => "x"): _*)
  }
  type Grammar = Array[(String, String)]
  var grammar : Grammar = Array(("s", "f(s*)"), ("s", "var"), ("s", "const"))
  var expression : String
  val f : (Array[Object] => Object) = {
    case (Array(x)) => x
    case  x : Array[Object] => (x(0).asInstanceOf[Double] + f(x.drop(1)).asInstanceOf[Double]).asInstanceOf[Object]
  }
  val g : (Array[Object] => Object) = {
    case (Array(x)) => x.asInstanceOf[String]
    case x: Array[Object] => x(0).asInstanceOf[String] + g(x.drop(1)).asInstanceOf[String]
  }
  var env : Map[String, Object] = Map("x" -> "laxman".asInstanceOf[Object], "y" -> " stud".asInstanceOf[Object],  "#id" -> {x : Array[Object] => x.head}.asInstanceOf[Object], "#plus" -> g.asInstanceOf[Object])
  object tokenType extends Enumeration {
    type tokenType = Value
    val number , variable, string, op, rparen, lparen, notok, comma, ws = Value
  }
  import tokenType._
  val tokenize : Unit => Array[(tokenType, String)] = Unit => {
    val input = expression.toCharArray
    var pos : Int = 0
    val getNextToken = () => {
      var current : Array[Char] = Array()
      var flag = true
      var token :(tokenType, String) = null
      while(flag){
        if(pos >= input.length){
          flag  = false
        } else {
          current = current :+ input(pos)
          pos += 1
          current.mkString match {
            case r"^[-+]?\d*\.?\d*" => token = (number, current.mkString)
            case r"[a-zA-Z_][a-zA-Z0-9_]{0,31}" => token = (variable, current.mkString)
            case r"[#a-zA-Z_][a-zA-Z0-9_]{0,31}" => token = (op, current.mkString)
              //TODO string case here, regex fucks up
//            case  => token = (string, current.toString)
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
    var res : Array[(tokenType, String)] = Array()
    while(pos < input.length) {
      val token = getNextToken()
      res :+= token
    }
    res
  }

  val parse = () => {
    val tok = tokenize()
    val vstack = new mutable.Stack[Object]
    val fstack = new mutable.Stack[String]
    var args : Array[Object] = Array()
    tok foreach {
      case (`number`, n) => vstack.push(Double.box(n.toFloat))
      case (`variable`, v) => vstack.push(env(v))
      case (`op`, f) => fstack.push(f)
      case (`lparen`, _) => vstack.push(null)
      case _ => //chill
    }
    while(!vstack.isEmpty){
      val i = vstack.pop()
      if(i != null) args :+= i
      else {
        if(!fstack.isEmpty){
          val f = env(fstack.pop()).asInstanceOf[Array[Object] => Object]
          vstack.push(f(args.reverse))
          args = Array()
        }
      }
    }
    args(0)
  }
}
