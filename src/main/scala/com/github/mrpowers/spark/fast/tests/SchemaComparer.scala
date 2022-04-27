package com.github.mrpowers.spark.fast.tests

import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructField, StructType}

object SchemaComparer {

  def equals(s1: StructType, s2: StructType, ignoreNullable: Boolean = false, ignoreColumnNames: Boolean = false): Boolean = {
    if (s1.length != s2.length) {
      false
    } else {
      val structFields: Seq[(StructField, StructField)] = s1.zip(s2)
      structFields.forall { t =>
        ((t._1.nullable == t._2.nullable) || ignoreNullable) &&
        ((t._1.name == t._2.name) || ignoreColumnNames) &&
        equals(t._1.dataType, t._2.dataType, ignoreNullable, ignoreColumnNames)
      }
    }
  }

  def equals(dt1: DataType, dt2: DataType, ignoreNullable: Boolean, ignoreColumnNames: Boolean): Boolean = {
    (ignoreNullable, dt1, dt2) match {
      case (true, st1: StructType, st2: StructType)       => equals(st1, st2, ignoreNullable, ignoreColumnNames)
      case (true, ArrayType(vdt1, _), ArrayType(vdt2, _)) => equals(vdt1, vdt2, ignoreNullable, ignoreColumnNames)
      case (true, MapType(kdt1, vdt1, _), MapType(kdt2, vdt2, _)) =>
        equals(kdt1, kdt2, ignoreNullable, ignoreColumnNames) && equals(vdt1, vdt2, ignoreNullable, ignoreColumnNames)
      case _ => dt1 == dt2
    }
  }

}
