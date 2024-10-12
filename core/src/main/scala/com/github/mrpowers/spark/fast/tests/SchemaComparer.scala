package com.github.mrpowers.spark.fast.tests

import com.github.mrpowers.spark.fast.tests.ProductUtil.showProductDiff
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, NullType, StructField, StructType}

object SchemaComparer {
  case class DatasetSchemaMismatch(smth: String) extends Exception(smth)
  private def betterSchemaMismatchMessage[T](actualDS: Dataset[T], expectedDS: Dataset[T]): String = {
    showProductDiff(
      ("Actual Schema", "Expected Schema"),
      actualDS.schema.fields,
      expectedDS.schema.fields,
      truncate = 200,
      defaultVal = StructField("SPARK_FAST_TEST_MISSING_FIELD", NullType)
    )
  }

  def assertSchemaEqual[T](
      actualDS: Dataset[T],
      expectedDS: Dataset[T],
      ignoreNullable: Boolean = false,
      ignoreColumnNames: Boolean = false,
      ignoreColumnOrder: Boolean = true
  ): Unit = {
    require((ignoreColumnNames, ignoreColumnOrder) != (true, true), "Cannot set both ignoreColumnNames and ignoreColumnOrder to true.")
    if (!SchemaComparer.equals(actualDS.schema, expectedDS.schema, ignoreNullable, ignoreColumnNames, ignoreColumnOrder)) {
      throw DatasetSchemaMismatch(
        "Diffs\n" + betterSchemaMismatchMessage(actualDS, expectedDS)
      )
    }
  }

  def equals(
      s1: StructType,
      s2: StructType,
      ignoreNullable: Boolean = false,
      ignoreColumnNames: Boolean = false,
      ignoreColumnOrder: Boolean = true
  ): Boolean = {
    if (s1.length != s2.length) {
      false
    } else {
      if (s1.length != s2.length) {
        false
      } else {
        val zipStruct = if (ignoreColumnOrder) s1.sortBy(_.name) zip s2.sortBy(_.name) else s1 zip s2
        zipStruct.forall { t =>
          (t._1.nullable == t._2.nullable || ignoreNullable) &&
          (t._1.name == t._2.name || ignoreColumnNames) &&
          equals(t._1.dataType, t._2.dataType, ignoreNullable, ignoreColumnNames, ignoreColumnOrder)
        }
      }
    }
  }

  def equals(dt1: DataType, dt2: DataType, ignoreNullable: Boolean, ignoreColumnNames: Boolean, ignoreColumnOrder: Boolean): Boolean = {
    (ignoreNullable, dt1, dt2) match {
      case (ignoreNullable, st1: StructType, st2: StructType) if ignoreNullable || ignoreColumnOrder =>
        equals(st1, st2, ignoreNullable, ignoreColumnNames, ignoreColumnOrder)
      case (true, ArrayType(vdt1, _), ArrayType(vdt2, _)) =>
        equals(vdt1, vdt2, ignoreNullable, ignoreColumnNames, ignoreColumnOrder)
      case (true, MapType(kdt1, vdt1, _), MapType(kdt2, vdt2, _)) =>
        equals(kdt1, kdt2, ignoreNullable, ignoreColumnNames, ignoreColumnOrder) &&
        equals(vdt1, vdt2, ignoreNullable, ignoreColumnNames, ignoreColumnOrder)
      case _ => dt1 == dt2
    }
  }
}
