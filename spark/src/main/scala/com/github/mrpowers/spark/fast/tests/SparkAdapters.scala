package com.github.mrpowers.spark.fast.tests

import com.github.mrpowers.spark.fast.tests.api._
import org.apache.spark.sql.{DataFrame, Encoder, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.col

/**
 * Adapter to convert Spark Row to RowLike.
 */
case class SparkRowAdapter(private[tests] val row: Row) extends RowLike {
  override def length: Int = row.length

  override def get(index: Int): Any = row.get(index) match {
    case nestedRow: Row => SparkRowAdapter(nestedRow)
    case other          => other
  }

  override def isNullAt(index: Int): Boolean = row.isNullAt(index)

  override def toSeq: Seq[Any] = for (i <- 0 until row.length) yield get(i)

  override def equals(obj: Any): Boolean = obj match {
    case other: SparkRowAdapter => row.equals(other.row)
    case other: RowLike         => super.equals(other)
    case _                      => false
  }

  override def hashCode(): Int = row.hashCode()

  override def toString: String = row.toString

  override def schema: SchemaLike = SparkSchemaAdapter(row.schema)
}

object SparkRowAdapter {
  def apply(row: Row): SparkRowAdapter = new SparkRowAdapter(row)
}

/**
 * Adapter to convert Spark StructField to FieldLike
 */
class SparkFieldAdapter(field: StructField) extends FieldLike {

  override def name: String = field.name

  override def dataType: DataTypeLike = SparkDataTypeAdapter.convert(field.dataType)

  override def nullable: Boolean = field.nullable

  override def metadata: Map[String, Any] = {
    val m = field.metadata
    if (m.equals(Metadata.empty)) {
      Map.empty
    } else {
      m.json.hashCode() match {
        case _ => Map("_sparkMetadata" -> m)
      }
    }
  }

  override def productElement(n: Int): Any = n match {
    case 0 => name
    case 1 => dataType.typeName.capitalize + "Type"
    case 2 => nullable
    case 3 => field.metadata.json // Use Spark's JSON format directly
    case _ => throw new IndexOutOfBoundsException(n.toString)
  }

  override def toString: String = field.toString
}

object SparkFieldAdapter {
  def apply(field: StructField): SparkFieldAdapter = new SparkFieldAdapter(field)
}

/**
 * Adapter to convert Spark StructType to SchemaLike
 */
class SparkSchemaAdapter(schema: StructType) extends SchemaLike {

  override def fields: Seq[FieldLike] = schema.fields.map(SparkFieldAdapter.apply).toSeq
}

object SparkSchemaAdapter {
  def apply(schema: StructType): SparkSchemaAdapter = new SparkSchemaAdapter(schema)
}

/**
 * Converter for Spark DataType to DataTypeLike
 */
object SparkDataTypeAdapter {

  def apply(dt: DataType): DataTypeLike = convert(dt)

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
    case a: ArrayType   => ArrayTypeLike(convert(a.elementType), a.containsNull)
    case m: MapType     => MapTypeLike(convert(m.keyType), convert(m.valueType), m.valueContainsNull)
    case s: StructType  => StructTypeLike(s.fields.map(f => SparkFieldAdapter(f)).toSeq)
    case other          => UnknownTypeLike(other.typeName)
  }
}
