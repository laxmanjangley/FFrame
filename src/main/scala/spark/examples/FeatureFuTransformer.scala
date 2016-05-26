package spark.examples


import com.linkedin.featurefu.expr.{Expr, Expression, VariableRegistry}

import scala.collection.mutable.ArrayBuilder
import org.apache.spark.SparkException
import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.attribute.{Attribute, AttributeGroup, NumericAttribute, UnresolvedAttribute}
import org.apache.spark.mllib.linalg.{Vector, VectorUDT, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}
import org.apache.spark.ml.param._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
//import com.linkedin.featurefu.expr

/**
  * Created by laxman.jangley on 24/5/16.
  */

trait FFParams extends Params {
  val inputcols = new Param[Array[String]](this,  "inputcols", "input columns")
  val outputcol = new Param[String](this, "outputcol", "output column")
  val expr = new Param[String](this, "expr", "feature fu expression")
  def pvals(pm: ParamMap) =  {
    pm.get(inputcols).getOrElse("topicSet")
    pm.get(outputcol).getOrElse("feature")
    pm.get(expr).getOrElse("expression")
  }
}
class FeatureFuTransformer (override val uid: String)
  extends Transformer with FFParams {

  def this() = this(Identifiable.randomUID("FeatureFuTransformer"))

  /** @group setParam */
  def setInputCols(value: Array[String]): this.type = set(inputcols, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputcol, value)

  def setExpr(value: String): this.type = set(expr, value)

  /**
    * Number of features.  Should be > 0.
    * (default = 2^18^)
    * @group param
    */
  val numFeatures = new IntParam(this, "numFeatures", "number of features (> 0)", ParamValidators.gt(0))

  /** @group setParam */
  def setNumFeatures(value: Int): this.type = set(numFeatures, value)

//  @Since("2.0.0")
  override def transform(dataset: DataFrame): DataFrame = {
      val outputSchema = transformSchema(dataset.schema)
      val inputs = $(inputcols)
      val func:  (Seq[Double] => Double) = (elem : Seq[Double]) => {
          var x = 0
          val e : String = $(expr)
          for(i <- inputs){
              e.replace(i, elem(x).toString())
              x += 1
          }
          val vr = new VariableRegistry()
          val exp = Expression.parse(e, vr)
          exp.evaluate()
      }
      val f = udf(func )
      def makecols(): Seq[Column] = {
          val x : Seq[Column] = Seq()
          for(i <- inputs) {
              x :+ col(i)
          }
          return x
      }
      val metadata = outputSchema($(outputcol)).metadata
      dataset.withColumn($(outputcol), f(makecols()))
  }

  override def transformSchema(schema: StructType): StructType = {
    val inputType = schema($(inputcols)).dataType
    require(inputType.isInstanceOf[ArrayType],
      s"The input column must be ArrayType, but got $inputType.")
    val attrGroup = new AttributeGroup($(outputcol), $(numFeatures))
    SchemaUtils.appendColumn(schema, attrGroup.toStructField())
  }

  override def copy(extra: ParamMap): HashingTF = defaultCopy(extra)
}

@Since("1.6.0")
object HashingTF extends DefaultParamsReadable[HashingTF] {

  @Since("1.6.0")
  override def load(path: String): HashingTF = super.load(path)
}