package spark.feature

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.param.{ParamMap, _}
import org.apache.spark.ml.util._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, UserDefinedFunction}

import scala.collection.mutable
import scala.collection.mutable.Map
/**
  * Created by root on 13/6/16.
  */
class ExprEval (override val uid: String)
  extends Transformer {
  def this() = this(Identifiable.randomUID("treeTransformer"))
  /*@group param*/
  val inputCols = new Param[Seq[String]](this, "input cols", "yoyo")
  val function = new Param[Map[String,Object]=>(Map[String,Object]=>Object)](this, "eval", "")
  val outputCol = new Param[String](this, "output column", "yoyoy")
  val numFeatures = new Param[Int](this, "no of cols", "studd")
  val map = new Param[Map[String, Object]] (this, "sdas", "sadasd")

  /*@group setter*/
  def setOutputCol(value : String) = set(outputCol, value)
  def setInputCols(value : Seq[String]) = set(inputCols, value)
  def setFunction(value : (Map[String,Object]=>(Map[String,Object]=>Object))) =  set(function, value)
  def setNumFeatures(value : Int) = set(numFeatures, value)
  def setMap(value : Map[String,Object]) = set(map, value)

  override def transformSchema(schema: StructType): StructType = {
    // TODO: Assertions on inputCols
    val attrGroup = new AttributeGroup($(outputCol), $(numFeatures))
    val col = attrGroup.toStructField()
    require(!schema.fieldNames.contains(col.name), s"Column ${col.name} already exists.")
    StructType(schema.fields :+ col)
  }

  override def transform(dataset: DataFrame): DataFrame = {
    val outputSchema = transformSchema(dataset.schema)
    val metadata = outputSchema($(outputCol)).metadata
    val cols = dataset.columns.toSeq
    val f = udf { r: Row =>
      val env : Map[String, Object] = mutable.Map[String,Object]()
      cols.zipWithIndex foreach {case (x,y) =>
        if($(inputCols).contains(x)) {
          env(x) = r.get(y).asInstanceOf[Object]
        }
      }
      ($(function) ($(map)) (env)).toString
    }
    dataset.select(col("*"), f(struct(dataset.columns.map(dataset(_)) : _*)).as($(outputCol), metadata))
  }

  override def copy(extra: ParamMap): ExprEval = defaultCopy(extra)
}

