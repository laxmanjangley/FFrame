package spark.examples


import breeze.linalg.*
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.param.{ParamMap, _}
import org.apache.spark.ml.util._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql._
import org.apache.spark.sql.{Column, DataFrame, Row}

/**
  * Created by laxman.jangley on 24/5/16.
  */

trait FFParams extends Params {
  val inputcols = new Param[Seq[String]](this,  "inputcols", "input columns")
  val inputcol = new Param[String] (this, "inputcol", "input column")
  val outputcol = new Param[String](this, "outputcol", "output column")
  val expr = new Param[String](this, "expr", "feature fu expression")
  val function = new Param[String=>Double](this, "external library function", "function")
  def pvals(pm: ParamMap) =  {
    pm.get(inputcols).getOrElse("topicSet")
    pm.get(inputcols).getOrElse("topicSet")
    pm.get(outputcol).getOrElse("feature")
    pm.get(expr).getOrElse("expression")
    pm.get(function).getOrElse("function")
  }
}
class FeatureFuTransformer (override val uid: String)
  extends Transformer with FFParams {

  def this() = this(Identifiable.randomUID("FeatureFuTransformer"))

  /** @group setParam */
  def setInputCols(value: Seq[String]): this.type = set(inputcols, value)

  /** @group setParam */
  def setInputCol(value: String) : this.type  = set(inputcol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputcol, value)

  def setExpr(value: String): this.type = set(expr, value)

  /**
    * @group param
    */
  val numFeatures = new IntParam(this, "numFeatures", "number of features (> 0)", ParamValidators.gt(0))

  /** @group setParam */
  def setNumFeatures(value: Int): this.type = set(numFeatures, value)

  /** @group setParam */
  def setFunction(value: String=>Double) = set(function, value)

  override def transform(dataset: DataFrame): DataFrame = {
    val outputSchema = transformSchema(dataset.schema)
    val metadata = outputSchema($(outputcol)).metadata
    val arg1 = dataset.columns.toSeq
    val f = udf {(r: Row) => {
      var exp = $(expr)
      r.toSeq.zipWithIndex foreach {case (v,i) => {
        exp = exp.replace(arg1(i), r.getInt(i).toString)
      }}
      $(function) (exp)
    }}
    dataset.select(col("*"), f(struct(dataset.columns.map(dataset(_)) : _*)).as($(outputcol), metadata))
  }


//    override def transform(dataset: DataFrame): DataFrame = {
//      val outputSchema = transformSchema(dataset.schema)
//      val inputs = $(inputcols)
//      def func (name: String) (exp: String , elem : Double) = {
//          exp.replace(name, elem.toString)
//      }
//      val temp = udf {x : Double => $(expr)}
//      val calc : String => Double = (exp  : String) => {
//        val vr = new VariableRegistry()
//        val expression = Expression.parse(exp, vr)
//        expression.evaluate()
//      }
//      var dst = dataset
//      var data = dataset.select(col("*"), temp(dataset(inputs(0))).as("0"))
//      var str = 0
//      inputs.foreach(i => {
//        val f = udf( func (i) _ )
//        data = data.select(col("*"), f(data(str.toString), data(i)).as((str+1).toString))
//        str += 1
//      })
//      val c = udf(calc )
//      val metadata = outputSchema($(outputcol)).metadata
//      var st = str
//      inputs.foreach(i => {
//        st -= 1
//        data = data.drop(st.toString)
//      })
//      data.select(col("*"), c(data(str.toString)).as($(outputcol), metadata)).drop(str.toString)
//
//    }




/* transform done on only one input column*/
//
//  override def transform(dataset: DataFrame): DataFrame = {
//    val outputSchema = transformSchema(dataset.schema)
//    val input = $(inputcol)
//    val func:  (Double => Double) = (elem : Double) => {
//      var e = $(expr)
//      e = e.replace(input, elem.toString)
//      val vr = new VariableRegistry()
//      val exp = Expression.parse(e, vr)
//      exp.evaluate()
//    }
//    val f = udf(func )
//    val metadata = outputSchema($(outputcol)).metadata
//    dataset.select(col("*"), f(col($(inputcol))).as($(outputcol), metadata))
////    dataset.withColumn($(outputcol), f(col(input)))
//  }

  override def transformSchema(schema: StructType): StructType = {
//    val inputType = schema($(inputcol)).dataType
//    require(inputType.isInstanceOf[ArrayType],
//      s"The input column must be ArrayType, but got $inputType.")
    val attrGroup = new AttributeGroup($(outputcol), $(numFeatures))
    val col = attrGroup.toStructField()
    require(!schema.fieldNames.contains(col.name), s"Column ${col.name} already exists.")
    StructType(schema.fields :+ col)
  }

  override def copy(extra: ParamMap): FeatureFuTransformer = defaultCopy(extra)
}

//@Since("1.6.0")
//object featurefu_test extends featurefu_test {
//
////  @Since("1.6.0")
//  override def load(path: String): featurefu_test = super.load(path)
//}