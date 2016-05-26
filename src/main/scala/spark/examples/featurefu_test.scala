package spark.examples

import com.linkedin.featurefu.expr._

/**
  * Created by laxman.jangley on 25/5/16.
  */
class featurefu_test {
  def main(args : Array[String]) {
        val registry: VariableRegistry = new VariableRegistry
        val expr: Expr = Expression.parse(args(1), registry)
        System.out.println("=" + expr.toString)
        if (registry.isEmpty) {
          System.out.println("=" + expr.evaluate)
        }

        System.out.println("tree")
        System.out.println(Expression.prettyTree(expr))
  }
}

