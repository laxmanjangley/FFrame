package spark.progress

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

class ExpressionEval(override val uid: String)
	extends Transformer {

	def this() = this(Identifiable.randomUID("FeatureFuTransformer"))
	/**
		* @group params
		*/
	val inputcols = new Param[Seq[String]](this,  "inputcols", "input columns")
	val outputcol = new Param[String](this, "outputcol", "output column")
	val expr = new Param[String](this, "expr", "feature fu expression")
	val function = new Param[String=>Double](this, "external library function", "function")
	val numFeatures = new IntParam(this, "numFeatures", "number of features (> 0)", ParamValidators.gt(0))

	/** @group setParams */
	def setNumFeatures(value: Int): this.type = set(numFeatures, value)

	def setInputCols(value: Seq[String]): this.type = set(inputcols, value)

	def setExpr(value: String): this.type = set(expr, value)

	def setOutputCol(value: String): this.type = set(outputcol, value)

	def setFunction(value: String=>Double) = set(function, value)

	/** @group getParams */
	def getOutputCol() = $(outputcol)

	def getExpr() = $(expr)

	def getInputCols() = $(inputcols).toArray

	def getNumFeatures() = $(numFeatures)

	def getFunction() = $(function)

	override def transform(dataset: DataFrame): DataFrame = {
		val outputSchema = transformSchema(dataset.schema)
		val metadata = outputSchema($(outputcol)).metadata
		val f = udf {(r: Row) => {
			val exp = $(expr)
			for (i <- 1 to $(numFeatures)) {
				exp.replace(dataset.columns.toSeq(i), r.getInt(i).toString)
			}
			$(function)(exp)
		}}
		val x = lit($(expr))
		dataset.select(col("*"), f(struct(dataset.columns.map(dataset(_)) : _*)).as($(outputcol), metadata))
	}


	override def transformSchema(schema: StructType): StructType = {
		val attrGroup = new AttributeGroup($(outputcol), $(numFeatures))
		val col = attrGroup.toStructField()
		require(!schema.fieldNames.contains(col.name), s"Column ${col.name} already exists.")
		StructType(schema.fields :+ col)
	}

	override def copy(extra: ParamMap): ExpressionEval = defaultCopy(extra)
}
