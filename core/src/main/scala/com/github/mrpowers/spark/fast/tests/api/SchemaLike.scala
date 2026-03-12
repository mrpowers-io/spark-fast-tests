package com.github.mrpowers.spark.fast.tests.api

/**
 * Abstract representation of a DataFrame schema that works across different DataFrame implementations (Spark, Snowpark, etc.)
 */
trait SchemaLike extends Serializable {

  /** Returns all fields in this schema */
  def fields: Seq[FieldLike]

  /** Returns the number of fields */
  def length: Int = fields.length

  /** Returns the field at the given index */
  def apply(index: Int): FieldLike = fields(index)

  /** Returns the field names */
  def fieldNames: Seq[String] = fields.map(_.name)

  /** Returns fields sorted by name */
  def sortedByName: Seq[FieldLike] = fields.sortBy(_.name)
}

/**
 * Abstract representation of a schema field that works across different DataFrame implementations (Spark, Snowpark, etc.)
 */
trait FieldLike extends Serializable with Product {

  /** Field name */
  def name: String

  /** Field data type */
  def dataType: DataTypeLike

  /** Whether the field is nullable */
  def nullable: Boolean

  /** Field metadata (optional) */
  def metadata: Map[String, Any] = Map.empty

  override def productArity: Int = 4
  override def productElement(n: Int): Any = n match {
    case 0 => name
    case 1 => dataType.typeName.capitalize + "Type" // Format like Spark: "StringType", "LongType"
    case 2 => nullable
    case 3 => metadata.map { case (k, v) => s"$k=$v" }.mkString("{", ",", "}")
    case _ => throw new IndexOutOfBoundsException(n.toString)
  }
  override def canEqual(that: Any): Boolean = that.isInstanceOf[FieldLike]

  override def toString: String = s"StructField($name,${dataType.typeName.capitalize}Type,$nullable,${productElement(3)})"
}

/**
 * Abstract representation of a data type that works across different DataFrame implementations (Spark, Snowpark, etc.)
 */
sealed trait DataTypeLike extends Serializable {
  def typeName: String
}

case object StringTypeLike extends DataTypeLike {
  override def typeName: String = "string"
}

case object BooleanTypeLike extends DataTypeLike {
  override def typeName: String = "boolean"
}

case object ByteTypeLike extends DataTypeLike {
  override def typeName: String = "byte"
}

case object ShortTypeLike extends DataTypeLike {
  override def typeName: String = "short"
}

case object IntegerTypeLike extends DataTypeLike {
  override def typeName: String = "integer"
}

case object LongTypeLike extends DataTypeLike {
  override def typeName: String = "long"
}

case object FloatTypeLike extends DataTypeLike {
  override def typeName: String = "float"
}

case object DoubleTypeLike extends DataTypeLike {
  override def typeName: String = "double"
}

case object BinaryTypeLike extends DataTypeLike {
  override def typeName: String = "binary"
}

case object DateTypeLike extends DataTypeLike {
  override def typeName: String = "date"
}

case object TimestampTypeLike extends DataTypeLike {
  override def typeName: String = "timestamp"
}

case class DecimalTypeLike(precision: Int, scale: Int) extends DataTypeLike {
  override def typeName: String = s"decimal($precision,$scale)"
}

case class ArrayTypeLike(elementType: DataTypeLike, containsNull: Boolean = true) extends DataTypeLike {
  override def typeName: String = "array"
}

case class MapTypeLike(keyType: DataTypeLike, valueType: DataTypeLike, valueContainsNull: Boolean = true) extends DataTypeLike {
  override def typeName: String = "map"
}

case class StructTypeLike(fields: Seq[FieldLike]) extends DataTypeLike with SchemaLike {
  override def typeName: String = "struct"
}

case class UnknownTypeLike(originalTypeName: String) extends DataTypeLike {
  override def typeName: String = originalTypeName
}

/**
 * A generic implementation of FieldLike
 */
case class GenericFieldLike(
    name: String,
    dataType: DataTypeLike,
    nullable: Boolean = true,
    override val metadata: Map[String, Any] = Map.empty
) extends FieldLike

/**
 * A generic implementation of SchemaLike
 */
case class GenericSchemaLike(fields: Seq[FieldLike]) extends SchemaLike
