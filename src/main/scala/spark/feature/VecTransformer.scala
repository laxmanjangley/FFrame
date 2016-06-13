package spark.feature

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.param.{ParamMap, _}
import org.apache.spark.ml.util._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, UserDefinedFunction}
/**
  * Created by root on 13/6/16.
  */
class VecTransformer (override val uid: String) extends Transformer {
  val inputCol1 = new Param[String](this, "input column1", "yoyo")
  val inputCol2 = new Param[String](this, "input column2", "yoyo")
  val outputCol = new Param[String](this, "output column", "yoyoy")
  val numFeatures = new Param[Int](this, "no of cols", "studd")

  /*@group setter*/
  def setInputCol1(value : String) = set(inputCol1, value)
  def setInputCol2(value : String) = set(inputCol2, value)
  def setOutputCol(value : String) = set(outputCol, value)
  def setNumFeatures(value : Int) = set(numFeatures, value)

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
    dataset
  }

  override def copy(extra: ParamMap): VecTransformer = defaultCopy(extra)
}

