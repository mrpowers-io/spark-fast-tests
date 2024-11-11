package com.github.mrpowers.spark.fast.tests

import com.github.mrpowers.spark.fast.tests.ProductUtil.showProductDiff
import com.github.mrpowers.spark.fast.tests.ufansi.Color.{DarkGray, Green, Red}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types._

object SchemaComparer {
  case class DatasetSchemaMismatch(smth: String) extends Exception(smth)
  private def betterSchemaMismatchMessage[T](actualDS: Dataset[T], expectedDS: Dataset[T]): String = {
    showProductDiff(
      ("Actual Schema", "Expected Schema"),
      actualDS.schema.fields,
      expectedDS.schema.fields,
      truncate = 200
    )
  }

  private def treeSchemaMismatchMessage[T](actualSchema: StructType, expectedSchema: StructType): String = {
    def flattenStrucType(s: StructType, indent: Int): List[(Int, Int, StructField)] = s
      .foldLeft(List.empty[(Int, Int, StructField)]) { case (fieldPair, f) =>
        // 5 char for each level of indentation, 21 char for gap, and description words
        val gap  = indent * 5 + 21 + f.name.length + f.dataType.typeName.length + f.nullable.toString.length
        val pair = fieldPair :+ (indent, gap, f)
        f.dataType match {
          case st: StructType => pair ++ flattenStrucType(st, indent + 1)
          case _              => pair
        }
      }

    def depthToIndentStr(depth: Int): String = Range(0, depth).map(_ => "|    ").mkString + "|--"
    val treeSpaces                           = 6
    val treeFieldPair1                       = flattenStrucType(actualSchema, 0)
    val treeFieldPair2                       = flattenStrucType(expectedSchema, 0)
    val headerGap                            = treeFieldPair1.groupBy(_._2).maxBy(_._1)._1 + treeSpaces
    val treePair = treeFieldPair1
      .zipAll(treeFieldPair2, (0, 0, null), (0, 0, null))
      .map { case ((indent1, _, field1), (indent2, _, field2)) =>
        val prefix1 = depthToIndentStr(indent1)
        val prefix2 = depthToIndentStr(indent2)
        val (sprefix1, sprefix2) = if (indent1 != indent2) {
          (Red(prefix1), Green(prefix2))
        } else {
          (DarkGray(prefix1), DarkGray(prefix2))
        }

        if (field1 != null && field2 != null) {
          val (name1, name2) =
            if (field1.name != field2.name)
              (Red(field1.name), Green(field2.name))
            else
              (DarkGray(field1.name), DarkGray(field2.name))

          val (dtype1, dtype2) =
            if (field1.dataType != field2.dataType)
              (Red(field1.dataType.typeName), Green(field2.dataType.typeName))
            else
              (DarkGray(field1.dataType.typeName), DarkGray(field2.dataType.typeName))

          val (nullable1, nullable2) =
            if (field1.nullable != field2.nullable)
              (Red(field1.nullable.toString), Green(field2.nullable.toString))
            else
              (DarkGray(field1.nullable.toString), DarkGray(field2.nullable.toString))

          val structString1 = s"$sprefix1 $name1 : $dtype1 (nullable = $nullable1)"
          val structString2 = s"$sprefix2 $name2 : $dtype2 (nullable = $nullable2)"
          (structString1, structString2)
        } else {
          val structString1 = if (field1 != null) {
            val name     = Red(field1.name)
            val dtype    = Red(field1.dataType.typeName)
            val nullable = Red(field1.nullable.toString)
            s"$prefix1 $name : $dtype (nullable = $nullable)"
          } else ""

          val structString2 = if (field2 != null) {
            val name     = Green(field2.name)
            val dtype    = Green(field2.dataType.typeName)
            val nullable = Green(field2.nullable.toString)
            s"$prefix2 $name : $dtype (nullable = $nullable)"
          } else ""
          (structString1, structString2)
        }
      }

    val schemaGap = treePair.groupBy(_._1.length).maxBy(_._1)._1 + treeSpaces

    treePair
      .foldLeft(new StringBuilder("\nActual Schema".padTo(headerGap, ' ') + "Expected Schema\n")) { case (sb, (s1, s2)) =>
        val gap = if (s1.isEmpty) headerGap else schemaGap
        sb.append(s1.padTo(gap, ' ') + s2 + "\n")
      }
      .toString()
  }

  def assertDatasetSchemaEqual[T](
      actualDS: Dataset[T],
      expectedDS: Dataset[T],
      ignoreNullable: Boolean = false,
      ignoreColumnNames: Boolean = false,
      ignoreColumnOrder: Boolean = true,
      ignoreMetadata: Boolean = true
  ): Unit = {
    assertSchemaEqual(actualDS.schema, expectedDS.schema, ignoreNullable, ignoreColumnNames, ignoreColumnOrder, ignoreMetadata)
  }

  def assertSchemaEqual(
      actualSchema: StructType,
      expectedSchema: StructType,
      ignoreNullable: Boolean = false,
      ignoreColumnNames: Boolean = false,
      ignoreColumnOrder: Boolean = true,
      ignoreMetadata: Boolean = true
  ): Unit = {
    require((ignoreColumnNames, ignoreColumnOrder) != (true, true), "Cannot set both ignoreColumnNames and ignoreColumnOrder to true.")
    if (!SchemaComparer.equals(actualSchema, expectedSchema, ignoreNullable, ignoreColumnNames, ignoreColumnOrder, ignoreMetadata)) {
      throw DatasetSchemaMismatch(
        "Diffs\n" + treeSchemaMismatchMessage(actualSchema, expectedSchema)
      )
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
