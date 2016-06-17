package spark.examples
import java.util

import spark.examples.{Const, Op, Tree, Var}

import scala.collection.mutable
import scala.util.matching.Regex

/**
  * Created by root on 17/6/16.
  */


abstract class Parser {
  implicit class Regex(sc: StringContext) {
    def r = new scala.util.matching.Regex(sc.parts.mkString, sc.parts.tail.map(_ => "x"): _*)
  }
  type Grammar = Map[String, Array[String]]
  var grammar : Grammar
  var expression : String
  var mapping : Map[String, Object]
  object tokenType extends Enumeration {
    type tokenType = Value
    val number , variable, string, op, rparen, lparen, notok = Value
  }
  import tokenType._
  val tokenize : Unit => Array[(tokenType, Object)] = Unit => {
    val input = expression.toCharArray
    var pos : Int = 0
    var current : Array[Char] = Array()
    val getNextToken = () => {
      var flag = true
      var token :(tokenType, Object) = new (tokenType, Object)
      while(flag){
        if(pos == input.length){
          flag  = false
        }
        current = current :+ input(pos)
        pos +=1
        current.toString match {
          case r"[+-]([0-9]*[.])?[0-9]+" => flag = false; token = (number, current.toString.toFloat)
          case r"[a-zA-Z_][a-zA-Z0-9_]{0,31}" => flag = false; token = (variable, current.toString)
          case r"#[a-zA-Z_][a-zA-Z0-9_]{0,31}" => flag = false; token =  (op, current.toString)
          case r"\".*\"" => flag = false; token = (string, current.toString)
          case r"[(]" => flag = false; token  = (lparen, current.toString)
          case r"[)]" =>flag = false; token = (rparen, current.toString)
          case _ => token = (notok, "")
        }
      }
      token
    }
    var res : Array[(tokenType, Object)] = Array()
    while(pos != input.length) {
      val token = getNextToken()
      res = res :+ (token._1, token._2.asInstanceOf[Object])
    }
    res
  }
}
