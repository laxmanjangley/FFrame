package spark.parserUtil

import java.io.Serializable
import scala.collection.mutable
import scala.collection.mutable.Map

/**
  * Created by root on 7/7/16.
  */
class Parser extends Serializable {
  implicit class Regex(sc: StringContext) {
    def r = new scala.util.matching.Regex(sc.parts.mkString, sc.parts.tail.map(_ => "x"): _*)
  }

  object tokenType extends Enumeration{
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
            //TODO string case here
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

  //  val parse = (fenv: mutable.Map[String, Object]) => (tok : Array[(Object, Object)]) =>  (env: Map[String, Object]) => {
  def parse (fenv: mutable.Map[String, Object])  (tok : Array[(Object, Object)])  (env: Map[String, Object]) = {
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
}
