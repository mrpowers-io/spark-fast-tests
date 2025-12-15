package com.github.mrpowers.spark.fast.tests.comparer.schema

import org.apache.spark.sql.types._

case class FieldInfo(
    typeName: String,
    nullable: Boolean,
    metadata: Metadata,
    containsNull: Option[Boolean] = None, // For array types
    name: Option[String] = None
)

object FieldInfo {
  def apply(field: StructField): FieldInfo = {
    FieldInfo(field.dataType.typeName, field.nullable, field.metadata, name = Some(field.name))
  }

  def apply(dataType: DataType): FieldInfo = dataType match {
    case arrayType: ArrayType =>
      FieldInfo(arrayType.typeName, nullable = true, Metadata.empty, Some(arrayType.containsNull))
    case mapType: MapType =>
      FieldInfo(mapType.typeName, nullable = true, Metadata.empty, Some(mapType.valueContainsNull))
    case _: DataType =>
      FieldInfo(dataType.typeName, nullable = true, Metadata.empty)
  }

  // Special apply for array elements - shows the element type with containsNull as nullable
  def applyForArrayElement(arrayType: ArrayType): FieldInfo = arrayType.elementType match {
    case innerArray: ArrayType =>
      FieldInfo(innerArray.typeName, nullable = true, Metadata.empty, Some(innerArray.containsNull))
    case mapType: MapType =>
      FieldInfo(mapType.typeName, nullable = true, Metadata.empty, Some(mapType.valueContainsNull))
    case elementType =>
      FieldInfo(elementType.typeName, arrayType.containsNull, Metadata.empty)
  }
}
