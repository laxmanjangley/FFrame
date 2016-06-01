package spark.feature

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.param.{ParamMap, _}
import org.apache.spark.ml.util._
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.DataFrame

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

class ExprTransformer (override val uid: String) extends Transformer with FFParams{

  def this() = this(Identifiable.randomUID("Expression Transformer"))

  /** @group setParam */
  def setInputCols(value: Seq[String]): this.type = set(inputCols, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
  def setExpr(value: String): this.type = set(expr, value)

  /** @group setParam */
  def setNumFeatures(value: Int): this.type = set(numFeatures, value)

  /** @group setParam */
  def setFunction(value: String=>Double) = set(function, value)

  override def transform(dataset: DataFrame): DataFrame = {
    val outputSchema = transformSchema(dataset.schema)
    // dummy udf to add the expression column to the dataframe
    val dummy = udf {x : Double => $(expr)}
    var data = dataset.select(col("*"), dummy(dataset($(inputCols)(0))).as("0"))
    // iterate over inputCols to replace each column with its value in the expression
    // TODO: Figure out how to apply this substitution in a much cleaner way
    // func replaces a column with its value in the row in the expression
    def func (name: String) (exp: String , elem : Double): String = exp.replace(name, elem.toString)
    var str = 0
    $(inputCols).foreach(i => {
      val f = udf( func (i) _ )
      data = data.select(col("*"), f(data(str.toString), data(i)).as((str+1).toString)).drop(str.toString)
      str += 1
    })
    //apply evaluation function on the substituted string
    val exprEvaluate = udf($(function) )
    val metadata = outputSchema($(outputCol)).metadata
    data.select(col("*"), exprEvaluate(data(str.toString)).as($(outputCol), metadata)).drop(str.toString)
  }

  override def transformSchema(schema: StructType): StructType = {
    // TODO: Assertions on inputCols
    val attrGroup = new AttributeGroup($(outputCol), $(numFeatures))
    val col = attrGroup.toStructField()
    require(!schema.fieldNames.contains(col.name), s"Column ${col.name} already exists.")
    StructType(schema.fields :+ col)
  }

  override def copy(extra: ParamMap): ExprTransformer = defaultCopy(extra)
}
