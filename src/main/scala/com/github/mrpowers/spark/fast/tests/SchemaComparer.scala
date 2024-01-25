package com.github.mrpowers.spark.fast.tests

import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructField, StructType}

object SchemaComparer {

  def equals(s1: StructType,
             s2: StructType,
             ignoreNullable: Boolean = false,
             ignoreColumnNames: Boolean = false,
             ignoreColumnOrder: Boolean = false): Boolean = {
    if (s1.length != s2.length) {
      false
    } else {
      val structFields: Seq[(StructField, StructField)] = if (ignoreColumnOrder) {
        s1.sortBy(_.name) zip s2.sortBy(_.name)
      } else {
        s1 zip s2
      }

      structFields.forall { t =>
        ((t._1.nullable == t._2.nullable) || ignoreNullable) &&
        ((t._1.name == t._2.name) || ignoreColumnNames) &&
        equals(t._1.dataType, t._2.dataType, ignoreNullable, ignoreColumnNames, ignoreColumnOrder)
      }
    }
  }

  def equals(dt1: DataType, dt2: DataType, ignoreNullable: Boolean, ignoreColumnNames: Boolean, ignoreColumnOrder: Boolean): Boolean = {
    (dt1, dt2) match {
      case (st1: StructType, st2: StructType)       => equals(st1, st2, ignoreNullable, ignoreColumnNames, ignoreColumnOrder)
      case (ArrayType(vdt1, _), ArrayType(vdt2, _)) => equals(vdt1, vdt2, ignoreNullable, ignoreColumnNames, ignoreColumnOrder)
      case (MapType(kdt1, vdt1, _), MapType(kdt2, vdt2, _)) =>
        equals(kdt1, kdt2, ignoreNullable, ignoreColumnNames, ignoreColumnOrder) && equals(vdt1,
                                                                                           vdt2,
                                                                                           ignoreNullable,
                                                                                           ignoreColumnNames,
                                                                                           ignoreColumnOrder)
      case _ => dt1 == dt2
    }
  }

}
