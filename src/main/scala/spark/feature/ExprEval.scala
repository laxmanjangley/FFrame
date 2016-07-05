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
  def this() = this(Identifiable.randomUID("Expression Transformer"))
  /*@group param*/
  val inputCols = new Param[Seq[String]](this, "input columns", "input columns pertaining to  all the exporessions specified")
  val outputTuples = new Param[Seq[(String, String)]](this, "list of tuples containing the output column and its corresponding expression", "list of tuples containing the output column and its corresponding expression")
  val function = new Param[Array[(Object, Object)] => (Map[String,Object]=>Object)](this, "eval function", "evaluation function for solving each of these expressions")
  val numFeatures = new Param[Int](this, "no of cols", "number of featuures in the input dataframe")
  val tt = new Param[String => Array[(Object, Object)]](this, "tokenizer", "tokenizer for the expressions")

  /*@group setter*/
  def setInputCols(value : Seq[String]) = set(inputCols, value)
  def setFunction(value : (Array[(Object, Object)] => (Map[String,Object]=>Object))) =  set(function, value)
  def setNumFeatures(value : Int) = set(numFeatures, value)
  def setTt (value : String => Array[(Object,  Object)]) = set(tt, value)
  def setoutputTuples (value : Seq[(String, String)]) = set(outputTuples, value)


  //helper function for getting the schema of the dataframe including the column x
  def transformSchema1(x: String, num:Int , schema: StructType): StructType = {
    // TODO: Assertions on inputCols
    val attrGroup = new AttributeGroup(x, num)
    val col = attrGroup.toStructField()
    require(!schema.fieldNames.contains(col.name), s"Column ${col.name} already exists.")
    StructType(schema.fields :+ col)
  }

  //transformSchema inherited from the Transformer class, implemented using the above helper
  override def transformSchema(schema: StructType): StructType = {
    // TODO: Assertions on inputCols
    var sc = schema
    var num = $(numFeatures)
    //iterative step
    $(outputTuples).foreach(x => {sc = transformSchema1(x._1, num, sc); num += 1})
    sc
  }

  //helper function for generating outputcol using exp, pass metadata as argument
  def transform1 (dataset: DataFrame) (outputcol : String ,exp : String, metadata : Metadata): DataFrame = {
    //call by value
    val x = $(function) ($(tt) (exp))
    //map from column name to int, i.e. index in row
    val m = {
      val map : Map[String, Int] = Map()
      dataset.columns.zipWithIndex foreach {
        case (x, y) => {
          if($(inputCols).contains(x)) map(x) = y
        }
      }
      map
    }
    //udf for calculating the new columns
    val f = udf {r : Row =>
      val env : Map[String, Object] = Map()
      $(inputCols).foreach(x => env(x) = r.get(m(x)).asInstanceOf[Object])
      //todo: type inference
      (x (env)).toString
    }
//    dataset.select(col("*"), f(struct(dataset.columns.map(dataset(_)) : _*)).as(outputcol, metadata))
    dataset.select(col("*"), f(struct($(inputCols).map(dataset(_)) : _*)).as(outputcol, metadata))
  }

  //transform method inherited from the Transformer class
  override def transform(dataset: DataFrame): DataFrame = {
    val outputSchema = transformSchema(dataset.schema)
    var df = dataset
    //iterative evaluation of list of tuples in $(outputTuples)
    $(outputTuples).foreach {case (x, y) => df = transform1 (df) (x, y, metadata = outputSchema(x).metadata)}
    df
  }

  override def copy(extra: ParamMap): ExprEval = defaultCopy(extra)
}

