package com.github.mrpowers.spark.fast.tests

import org.apache.spark.sql.types.{DataType, StructField, StructType}

object SchemaComparer {

  def equals(s1: StructType, s2: StructType, ignoreNullable: Boolean = false, ignoreColumnNames: Boolean = false): Boolean = {
    def structTypesAreEqual(st1: StructType, st2: StructType): Boolean =
      (st1.length == st2.length) && st1.zip(st2).forall {
        case (f1: StructField, f2: StructField) =>
          fieldsAreEqual(f1, f2)
      }

    def fieldsAreEqual(f1: StructField, f2: StructField): Boolean = {
      val nullEquality = (f1.nullable == f2.nullable) || ignoreNullable
      val nameEquality = (f1.name == f2.name) || ignoreColumnNames
      val typeEquality = dataTypesAreEqual(f1.dataType, f2.dataType)
      nullEquality && nameEquality && typeEquality
    }

    def dataTypesAreEqual(dt1: DataType, dt2: DataType): Boolean = (dt1, dt2) match {
      case (x: StructType, y: StructType) => structTypesAreEqual(x, y)
      case (_: StructType, _)             => false
      case (_, _: StructType)             => false
      case (x, y)                         => x == y
    }

    structTypesAreEqual(s1, s2)
  }

}
