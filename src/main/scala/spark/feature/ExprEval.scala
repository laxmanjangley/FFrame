package spark.feature

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.param.{ParamMap, _}
import org.apache.spark.ml.util._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.Metadata
import org.apache.spark.sql.{DataFrame, Row, UserDefinedFunction}
import scala.collection.mutable.Map
/**
  * Created by root on 13/6/16.
  */
class ExprEval (override val uid: String)
  extends Transformer {
  def this() = this(Identifiable.randomUID("treeTransformer"))
  /*@group param*/
  val inputCols = new Param[Seq[String]](this, "input cols", "yoyo")
  val outputTuples = new Param[Seq[(String, String)]](this, "exp and outputcols", "Ll")
  val function = new Param[Array[(Object, Object)] => (Map[String,Object]=>Object)](this, "eval", "")
  val numFeatures = new Param[Int](this, "no of cols", "studd")
  val tt = new Param[String => Array[(Object, Object)]](this, "that", "that")

  /*@group setter*/
  def setInputCols(value : Seq[String]) = set(inputCols, value)
  def setFunction(value : (Array[(Object, Object)] => (Map[String,Object]=>Object))) =  set(function, value)
  def setNumFeatures(value : Int) = set(numFeatures, value)
  def setTt (value : String => Array[(Object,  Object)]) = set(tt, value)
  def setoutputTuples (value : Seq[(String, String)]) = set(outputTuples, value)

  def transformSchema1(x: String, num:Int , schema: StructType): StructType = {
    // TODO: Assertions on inputCols
    val attrGroup = new AttributeGroup(x, num)
    val col = attrGroup.toStructField()
    require(!schema.fieldNames.contains(col.name), s"Column ${col.name} already exists.")
    StructType(schema.fields :+ col)
  }

  override def transformSchema(schema: StructType): StructType = {
    // TODO: Assertions on inputCols
    var sc = schema
    var num = $(numFeatures)
    $(outputTuples).foreach(x => {sc = transformSchema1(x._1, num, sc); num += 1})
    sc
  }

  def transform1 (dataset: DataFrame) (outputcol : String ,exp : String, metadata : Metadata): DataFrame = {
    val x = $(function) ($(tt) (exp))
    val m = {
      val map : Map[String, Int] = Map()
      dataset.columns.zipWithIndex foreach {
        case (x, y) => {
          if($(inputCols).contains(x)) map(x) = y
        }
      }
      map
    }
    val f = udf {r : Row =>
      val env : Map[String, Object] = Map()
      $(inputCols).foreach(x => env(x) = r.get(m(x)).asInstanceOf[Object])
      //todo: type inference
      (x (env)).toString
    }
//    dataset.select(col("*"), f(struct(dataset.columns.map(dataset(_)) : _*)).as(outputcol, metadata))
    dataset.select(col("*"), f(struct($(inputCols).map(dataset(_)) : _*)).as(outputcol, metadata))
  }

  override def transform(dataset: DataFrame): DataFrame = {
    val outputSchema = transformSchema(dataset.schema)
    var df = dataset
    $(outputTuples).foreach {case (x, y) => df = transform1 (df) (x, y, metadata = outputSchema(x).metadata)}
    df
  }

  override def copy(extra: ParamMap): ExprEval = defaultCopy(extra)
}

