package spark.examples


import org.apache.spark.ml.Transformer
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.param.{ParamMap, _}
import org.apache.spark.ml.util._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row}

/**
  * Created by laxman.jangley on 24/5/16.
  */

trait FFParams extends Params {
  val inputcols = new Param[Seq[String]](this,  "inputcols", "input columns")
//  val inputcol = new Param[String] (this, "inputcol", "input column")
  val outputcol = new Param[String](this, "outputcol", "output column")
  val expr = new Param[String](this, "expr", "feature fu expression")
  val function = new Param[String=>Double](this, "external library function", "function")
  def pvals(pm: ParamMap) =  {
    pm.get(inputcols).getOrElse("topicSet")
//    pm.get(inputcols).getOrElse("topicSet")
    pm.get(outputcol).getOrElse("feature")
    pm.get(expr).getOrElse("expression")
    pm.get(function).getOrElse("function")
  }
}
class ExpressionEval(override val uid: String)
  extends Transformer with FFParams {

  def this() = this(Identifiable.randomUID("FeatureFuTransformer"))


  /** @group setParam */
  def setInputCols(value: Seq[String]): this.type = set(inputcols, value)

  def getInputCols() = $(inputcols)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputcol, value)

  def getOutputCol() = $(outputcol)

  def setExpr(value: String): this.type = set(expr, value)

  def getExpr() = $(expr)
  /**
    * @group param
    */
  val numFeatures = new IntParam(this, "numFeatures", "number of features (> 0)", ParamValidators.gt(0))

  /** @group setParam */
  def setNumFeatures(value: Int): this.type = set(numFeatures, value)

  def getNumFeatures() = $(numFeatures)
  /** @group setParam */
  def setFunction(value: String=>Double) = set(function, value)

  def getFunction() = $(function)

  override def transform(dataset: DataFrame): DataFrame = {
    val outputSchema = transformSchema(dataset.schema)
    val metadata = outputSchema($(outputcol)).metadata
    val cols = dataset.columns.toSeq
    val f = udf {(r: Row) => {
      val exp = $(expr)
      for (i <- 1 to $(numFeatures)) {
        exp.replace(cols(i), r.getInt(i).toString)
      }
      $(function)(exp)
    }}
    dataset.select(col("*"), f(struct(dataset.columns.map(dataset(_)) : _*)).as($(outputcol), metadata))
  }

  override def transformSchema(schema: StructType): StructType = {
// TODO: ??
    val attrGroup = new AttributeGroup($(outputcol), $(numFeatures))
    val col = attrGroup.toStructField()
    require(!schema.fieldNames.contains(col.name), s"Column ${col.name} already exists.")
    StructType(schema.fields :+ col)
  }

  override def copy(extra: ParamMap): ExpressionEval = defaultCopy(extra)
}

//@Since("1.6.0")
//object featurefu_test extends featurefu_test {
//
////  @Since("1.6.0")
//  override def load(path: String): featurefu_test = super.load(path)
//}