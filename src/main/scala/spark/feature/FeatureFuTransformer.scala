package spark.feature

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.param.{ParamMap, _}
import org.apache.spark.ml.util._
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, UserDefinedFunction}

/**
  * Created by laxman.jangley on 27/5/16.
  */

trait FFParams extends Params {
  val inputCols = new Param[Seq[String]](this,  "inputcols", "input columns")
  val outputCol = new Param[String](this, "outputcol", "output column")
  val expr = new Param[String](this, "expr", "feature fu expression")
  val function = new Param[String=>Double](this, "external library function", "function")
  val numFeatures = new IntParam(this, "numFeatures", "number of features (> 0)", ParamValidators.gt(0))
  def pvals(pm: ParamMap) =  {
    pm.get(inputCols).getOrElse("topicSet")
    pm.get(outputCol).getOrElse("feature")
    pm.get(expr).getOrElse("expression")
    pm.get(function).getOrElse("function")
    pm.get(numFeatures).getOrElse("number of features")
  }
}

class FeatureFuTransformer(override val uid: String) extends Transformer with FFParams{

  def this() = this(Identifiable.randomUID("Expression Transformer"))

  /** @group setParam */
  def setInputCols(value: Seq[String]): this.type = set(inputCols, value)
  def getInputCols() = $(inputCols).toArray

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)
  def getOutputCols() = $(outputCol)

  /** @group setParam */
  def setExpr(value: String): this.type = set(expr, value)
  def getExpr() = $(expr)

  /** @group setParam */
  def setNumFeatures(value: Int): this.type = set(numFeatures, value)
  def getNumFeatures() = $(numFeatures)

  /** @group setParam */
  def setFunction(value: String=>Double) = set(function, value)
  def getFunction() =  $(function)

  override def transform(dataset: DataFrame): DataFrame = {
    val outputSchema = transformSchema(dataset.schema)
    val metadata = outputSchema($(outputCol)).metadata
    val dummy = udf { x: Any => $(expr) }
    var data = dataset.select(col("*"), dummy(col($(inputCols).head)).as("0"))
    val substitute: (String => ((String, Double) => String)) = name => (exp, elem) => exp.replace(name, elem.toString)
    def subst(v: String) = udf(substitute(v))
    $(inputCols).view.zipWithIndex foreach { case (v, i) => data = data.select(col("*"), subst(v)(data(i.toString), data(v)).as((i + 1).toString)).drop(i.toString) }
    val eval = udf($(function))
    data.select(col("*"), eval(data($(inputCols).length.toString)).as($(outputCol), metadata)).drop($(inputCols).length.toString)
  }


  override def transformSchema(schema: StructType): StructType = {
    // TODO: Assertions on inputCols
    val attrGroup = new AttributeGroup($(outputCol), $(numFeatures))
    val col = attrGroup.toStructField()
    require(!schema.fieldNames.contains(col.name), s"Column ${col.name} already exists.")
    StructType(schema.fields :+ col)
  }

  override def copy(extra: ParamMap): FeatureFuTransformer = defaultCopy(extra)
}
