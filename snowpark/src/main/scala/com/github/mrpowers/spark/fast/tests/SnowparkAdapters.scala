package com.github.mrpowers.spark.fast.tests

import com.github.mrpowers.spark.fast.tests.api._
import com.snowflake.snowpark.types.StructType

/**
 * Adapter to convert Snowpark Row to RowLike.
 */
class SnowparkRowAdapter(private[tests] val row: com.snowflake.snowpark.Row, rowSchema: StructType) extends RowLike {

  override def length: Int = row.length

  override def get(index: Int): Any = row.get(index) match {
    case r: com.snowflake.snowpark.Row =>
      SnowparkRowAdapter(r, rowSchema.fields(index).dataType.asInstanceOf[StructType])
    case other => other
  }

  override def isNullAt(index: Int): Boolean = row.isNullAt(index)

  override def toSeq: Seq[Any] = for (i <- 0 until row.length) yield get(i)

  override def equals(obj: Any): Boolean = obj match {
    case other: SnowparkRowAdapter => row.equals(other.row)
    case other: RowLike            => super.equals(other)
    case _                         => false
  }

  override def hashCode(): Int = row.hashCode()

  override def toString: String = row.toString

  override def schema: SchemaLike = SnowparkSchemaAdapter(rowSchema)
}

object SnowparkRowAdapter {
  def apply(row: com.snowflake.snowpark.Row, schema: StructType): SnowparkRowAdapter = new SnowparkRowAdapter(row, schema)
}

/**
 * Adapter to convert Snowpark StructField to FieldLike.
 */
class SnowparkFieldAdapter(field: com.snowflake.snowpark.types.StructField) extends FieldLike {

  override def name: String = field.name

  override def dataType: DataTypeLike = SnowparkDataTypeAdapter.convert(field.dataType)

  override def nullable: Boolean = field.nullable

  override def metadata: Map[String, Any] = Map.empty // Snowpark doesn't have field metadata
}

object SnowparkFieldAdapter {
  def apply(field: com.snowflake.snowpark.types.StructField): SnowparkFieldAdapter =
    new SnowparkFieldAdapter(field)
}

/**
 * Adapter to convert Snowpark StructType to SchemaLike.
 */
class SnowparkSchemaAdapter(schema: com.snowflake.snowpark.types.StructType) extends SchemaLike {

  override def fields: Seq[FieldLike] = schema.map(SnowparkFieldAdapter.apply)
}

object SnowparkSchemaAdapter {
  def apply(schema: com.snowflake.snowpark.types.StructType): SnowparkSchemaAdapter =
    new SnowparkSchemaAdapter(schema)
}

/**
 * Converter for Snowpark DataType to DataTypeLike.
 */
object SnowparkDataTypeAdapter {
  import com.snowflake.snowpark.types._

  def convert(dt: DataType): DataTypeLike = dt match {
    case StringType     => StringTypeLike
    case BooleanType    => BooleanTypeLike
    case ByteType       => ByteTypeLike
    case ShortType      => ShortTypeLike
    case IntegerType    => IntegerTypeLike
    case LongType       => LongTypeLike
    case FloatType      => FloatTypeLike
    case DoubleType     => DoubleTypeLike
    case BinaryType     => BinaryTypeLike
    case DateType       => DateTypeLike
    case TimestampType  => TimestampTypeLike
    case d: DecimalType => DecimalTypeLike(d.precision, d.scale)
    case a: ArrayType   => ArrayTypeLike(convert(a.elementType))
    case m: MapType     => MapTypeLike(convert(m.keyType), convert(m.valueType))
    case s: StructType  => StructTypeLike(s.map(f => SnowparkFieldAdapter(f)))
    case other          => UnknownTypeLike(other.toString)
  }
}

/**
 * DataFrameLike instance for Snowpark DataFrame.
 */
object SnowparkDataFrameLike extends DataFrameLike[com.snowflake.snowpark.DataFrame, RowLike] {
  import com.snowflake.snowpark.functions.col

  override def schema(df: com.snowflake.snowpark.DataFrame): SchemaLike =
    SnowparkSchemaAdapter(df.schema)

  override def collect(df: com.snowflake.snowpark.DataFrame): Seq[RowLike] =
    df.collect().map(SnowparkRowAdapter.apply(_, df.schema))

  override def columns(df: com.snowflake.snowpark.DataFrame): Array[String] =
    df.schema.names.toArray

  override def count(df: com.snowflake.snowpark.DataFrame): Long = df.count()

  override def select(df: com.snowflake.snowpark.DataFrame, columns: Seq[String]): com.snowflake.snowpark.DataFrame =
    df.select(columns)

  override def sort(df: com.snowflake.snowpark.DataFrame): com.snowflake.snowpark.DataFrame = {
    val sortCols = df.schema.names.map(col)
    df.sort(sortCols)
  }

  override def dtypes(df: com.snowflake.snowpark.DataFrame): Array[(String, String)] =
    df.schema.map(f => (f.name, f.dataType.toString)).toArray

  implicit val instance: DataFrameLike[com.snowflake.snowpark.DataFrame, RowLike] = this
}
