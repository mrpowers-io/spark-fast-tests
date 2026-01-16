package com.github.mrpowers.spark.fast.tests

import com.github.mrpowers.spark.fast.tests.ProductUtil.showProductDiff
import com.github.mrpowers.spark.fast.tests.SchemaDiffOutputFormat.SchemaDiffOutputFormat
import com.github.mrpowers.spark.fast.tests.comparer.schema.FieldComparison.{areFieldsEqual, buildComparison, treeSchemaMismatchMessage}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types._

object SchemaComparer {
  case class DatasetSchemaMismatch(smth: String) extends Exception(smth)
  private def betterSchemaMismatchMessage(actualSchema: StructType, expectedSchema: StructType): String = {
    showProductDiff(
      Array("Actual Schema", "Expected Schema"),
      actualSchema.fields,
      expectedSchema.fields,
      truncate = 200
    )
  }

  def assertDatasetSchemaEqual[T](
      actualDS: Dataset[T],
      expectedDS: Dataset[T],
      ignoreNullable: Boolean = false,
      ignoreColumnNames: Boolean = false,
      ignoreColumnOrder: Boolean = true,
      ignoreMetadata: Boolean = true,
      outputFormat: SchemaDiffOutputFormat = SchemaDiffOutputFormat.Table
  ): Unit = {
    assertSchemaEqual(actualDS.schema, expectedDS.schema, ignoreNullable, ignoreColumnNames, ignoreColumnOrder, ignoreMetadata, outputFormat)
  }

  def assertSchemaEqual(
      actualSchema: StructType,
      expectedSchema: StructType,
      ignoreNullable: Boolean = false,
      ignoreColumnNames: Boolean = false,
      ignoreColumnOrder: Boolean = true,
      ignoreMetadata: Boolean = true,
      outputFormat: SchemaDiffOutputFormat = SchemaDiffOutputFormat.Table
  ): Unit = {
    require((ignoreColumnNames, ignoreColumnOrder) != (true, true), "Cannot set both ignoreColumnNames and ignoreColumnOrder to true.")
    val diffTree = buildComparison(actualSchema, expectedSchema, ignoreColumnOrder)
    if (!areFieldsEqual(diffTree, ignoreNullable, ignoreColumnNames, ignoreMetadata)) {
      val diffString = outputFormat match {
        case SchemaDiffOutputFormat.Tree  => treeSchemaMismatchMessage(diffTree)
        case SchemaDiffOutputFormat.Table => betterSchemaMismatchMessage(actualSchema, expectedSchema)
      }

      throw DatasetSchemaMismatch(s"Diffs\n$diffString")
    }
  }

  def equals(
      s1: StructType,
      s2: StructType,
      ignoreNullable: Boolean = false,
      ignoreColumnNames: Boolean = false,
      ignoreColumnOrder: Boolean = true,
      ignoreMetadata: Boolean = true
  ): Boolean = {
    if (s1.length != s2.length) {
      false
    } else {
      if (s1.length != s2.length) {
        false
      } else {
        val zipStruct = if (ignoreColumnOrder) s1.sortBy(_.name) zip s2.sortBy(_.name) else s1 zip s2
        zipStruct.forall { case (f1, f2) =>
          (f1.nullable == f2.nullable || ignoreNullable) &&
          (f1.name == f2.name || ignoreColumnNames) &&
          (f1.metadata == f2.metadata || ignoreMetadata) &&
          equals(f1.dataType, f2.dataType, ignoreNullable, ignoreColumnNames, ignoreColumnOrder, ignoreMetadata)
        }
      }
    }
  }

  def equals(
      dt1: DataType,
      dt2: DataType,
      ignoreNullable: Boolean,
      ignoreColumnNames: Boolean,
      ignoreColumnOrder: Boolean,
      ignoreMetadata: Boolean
  ): Boolean = {
    (dt1, dt2) match {
      case (st1: StructType, st2: StructType) =>
        equals(st1, st2, ignoreNullable, ignoreColumnNames, ignoreColumnOrder)
      case (ArrayType(vdt1, _), ArrayType(vdt2, _)) =>
        equals(vdt1, vdt2, ignoreNullable, ignoreColumnNames, ignoreColumnOrder, ignoreMetadata)
      case (MapType(kdt1, vdt1, _), MapType(kdt2, vdt2, _)) =>
        equals(kdt1, kdt2, ignoreNullable, ignoreColumnNames, ignoreColumnOrder, ignoreMetadata) &&
        equals(vdt1, vdt2, ignoreNullable, ignoreColumnNames, ignoreColumnOrder, ignoreMetadata)
      case _ => dt1 == dt2
    }
  }
}
