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
  type Grammar = Array[(String, String)]
//  var grammar : Grammar
  var grammar : Grammar = Array(("s", "f(s*)"), ("s", "var"), ("s", "const"))
  var expression : String
//  var mapping : Map[String, Object]
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
//          println(current.mkString)
          current.mkString("") match {
            case r"[+-]([0-9]*[.])?[0-9]+" => token = (number, current.mkString(""))
            case r"[a-zA-Z_][a-zA-Z0-9_]{0,31}" => token = (variable, current.mkString(""))
            case r"[#a-zA-Z_][a-zA-Z0-9_]{0,31}" => token = (op, current.mkString(""))
              //TODO string case here, regex fucks up
//            case  => token = (string, current.toString)
            case r"[(]" => token = (lparen, current.mkString(""))
            case r"[)]" => token = (rparen, current.mkString(""))
            case r"[,]" => token = (comma, ",")
            case " " => token = (ws, " ")
            case _ => flag = false; pos -= 1; current = Array()
          }
        }
      }
      token
    }
    var res : Array[(tokenType, String)] = Array()
    while(pos < input.length) {
      val token = getNextToken()
      res = res :+ (token._1, token._2)
    }
    res
  }
  val parse = () => {
    val tok = tokenize()
    val stack = new mutable.Stack[tokenType]

  }
}
