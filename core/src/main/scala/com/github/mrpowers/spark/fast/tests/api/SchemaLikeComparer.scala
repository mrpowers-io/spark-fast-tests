package com.github.mrpowers.spark.fast.tests.api

import com.github.mrpowers.spark.fast.tests.SchemaDiffOutputFormat.SchemaDiffOutputFormat
import com.github.mrpowers.spark.fast.tests.SchemaDiffOutputFormat
import com.github.mrpowers.spark.fast.tests.comparer.schema.FieldComparison.{areFieldsEqual, buildComparison, treeSchemaMismatchMessage}

/**
 * Generic schema comparison that works with SchemaLike abstractions.
 */
object SchemaLikeComparer {

  private def schemaLikeMismatchMessage(actualSchema: SchemaLike, expectedSchema: SchemaLike): String = {
    ProductLikeUtil.showProductDiffWithHeader(
      Seq("Actual Schema", "Expected Schema"),
      Array.empty[String],
      actualSchema.fields,
      expectedSchema.fields,
      truncate = 200
    )
  }

  /**
   * Asserts that two SchemaLike schemas are equal.
   */
  def assertSchemaEqual(
      actualSchema: SchemaLike,
      expectedSchema: SchemaLike,
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
        case SchemaDiffOutputFormat.Table => schemaLikeMismatchMessage(actualSchema, expectedSchema)
      }
      throw DatasetSchemaMismatch(s"Diffs\n$diffString")
    }
  }

  /**
   * Compares two SchemaLike schemas for equality.
   */
  def equals(
      s1: SchemaLike,
      s2: SchemaLike,
      ignoreNullable: Boolean = false,
      ignoreColumnNames: Boolean = false,
      ignoreColumnOrder: Boolean = true,
      ignoreMetadata: Boolean = true
  ): Boolean = {
    if (s1.length != s2.length) {
      false
    } else {
      val zipStruct = if (ignoreColumnOrder) {
        s1.sortedByName zip s2.sortedByName
      } else {
        s1.fields zip s2.fields
      }
      zipStruct.forall { case (f1, f2) =>
        (f1.nullable == f2.nullable || ignoreNullable) &&
        (f1.name == f2.name || ignoreColumnNames) &&
        (f1.metadata == f2.metadata || ignoreMetadata) &&
        dataTypeEquals(f1.dataType, f2.dataType, ignoreNullable, ignoreColumnNames, ignoreColumnOrder, ignoreMetadata)
      }
    }
  }

  /**
   * Compares two DataTypeLike types for equality.
   */
  def dataTypeEquals(
      dt1: DataTypeLike,
      dt2: DataTypeLike,
      ignoreNullable: Boolean,
      ignoreColumnNames: Boolean,
      ignoreColumnOrder: Boolean,
      ignoreMetadata: Boolean
  ): Boolean = {
    (dt1, dt2) match {
      case (st1: StructTypeLike, st2: StructTypeLike) =>
        equals(st1, st2, ignoreNullable, ignoreColumnNames, ignoreColumnOrder, ignoreMetadata)
      case (ArrayTypeLike(vdt1, _), ArrayTypeLike(vdt2, _)) =>
        dataTypeEquals(vdt1, vdt2, ignoreNullable, ignoreColumnNames, ignoreColumnOrder, ignoreMetadata)
      case (MapTypeLike(kdt1, vdt1, _), MapTypeLike(kdt2, vdt2, _)) =>
        dataTypeEquals(kdt1, kdt2, ignoreNullable, ignoreColumnNames, ignoreColumnOrder, ignoreMetadata) &&
        dataTypeEquals(vdt1, vdt2, ignoreNullable, ignoreColumnNames, ignoreColumnOrder, ignoreMetadata)
      case _ => dt1 == dt2
    }
  }
}
