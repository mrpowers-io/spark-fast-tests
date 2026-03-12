package com.github.mrpowers.spark.fast.tests.comparer.schema

import com.github.mrpowers.spark.fast.tests.api._

case class FieldInfo(
    typeName: String,
    nullable: Boolean,
    metadata: Map[String, Any],
    containsNull: Option[Boolean] = None, // For array types
    name: Option[String] = None
)

object FieldInfo {
  def apply(field: FieldLike): FieldInfo = {
    FieldInfo(field.dataType.typeName, field.nullable, field.metadata, name = Some(field.name))
  }

  def apply(dataType: DataTypeLike): FieldInfo = dataType match {
    case arrayType: ArrayTypeLike =>
      FieldInfo(arrayType.typeName, nullable = true, Map.empty, Some(arrayType.containsNull))
    case mapType: MapTypeLike =>
      FieldInfo(mapType.typeName, nullable = true, Map.empty, Some(mapType.valueContainsNull))
    case _: DataTypeLike =>
      FieldInfo(dataType.typeName, nullable = true, Map.empty)
  }

  // Special apply for array elements - shows the element type with containsNull as nullable
  def applyForArrayElement(arrayType: ArrayTypeLike): FieldInfo = arrayType.elementType match {
    case innerArray: ArrayTypeLike =>
      FieldInfo(innerArray.typeName, nullable = true, Map.empty, Some(innerArray.containsNull))
    case mapType: MapTypeLike =>
      FieldInfo(mapType.typeName, nullable = true, Map.empty, Some(mapType.valueContainsNull))
    case elementType =>
      FieldInfo(elementType.typeName, arrayType.containsNull, Map.empty)
  }
}
