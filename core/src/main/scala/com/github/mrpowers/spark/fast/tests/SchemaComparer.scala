package com.github.mrpowers.spark.fast.tests

import com.github.mrpowers.spark.fast.tests.ProductUtil.showProductDiff
import com.github.mrpowers.spark.fast.tests.SchemaDiffOutputFormat.SchemaDiffOutputFormat
import com.github.mrpowers.spark.fast.tests.ufansi.Color.{DarkGray, Green, Red}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types._

import scala.util.Try

object SchemaComparer {
  private val INDENT_GAP      = 5
  private val DESCRIPTION_GAP = 21
  private val TREE_GAP        = 6
  private val CONTAIN_NULLS =
    "sparkFastTestContainsNull" // Distinguishable metadata key for spark fast tests to avoid potential ambiguity with other metadata keys
  case class DatasetSchemaMismatch(smth: String) extends Exception(smth)
  private def betterSchemaMismatchMessage(actualSchema: StructType, expectedSchema: StructType): String = {
    showProductDiff(
      ("Actual Schema", "Expected Schema"),
      actualSchema.fields,
      expectedSchema.fields,
      truncate = 200
    )
  }

  private def treeSchemaMismatchMessage[T](actualSchema: StructType, expectedSchema: StructType): String = {
    def flattenStrucType(s: StructType, indent: Int, additionalGap: Int = 0): (Seq[(Int, StructField)], Int) = s
      .foldLeft((Seq.empty[(Int, StructField)], Int.MinValue)) { case ((fieldPair, maxWidth), f) =>
        val gap  = indent * INDENT_GAP + DESCRIPTION_GAP + f.name.length + f.dataType.typeName.length + f.nullable.toString.length + additionalGap
        val pair = fieldPair :+ (indent, f)
        val newMaxWidth = scala.math.max(maxWidth, gap)
        f.dataType match {
          case st: StructType =>
            val (flattenPair, width) = flattenStrucType(st, indent + 1)
            (pair ++ flattenPair, scala.math.max(newMaxWidth, width))
          case ArrayType(elementType, containsNull) =>
            val arrStruct = StructType(
              Seq(
                StructField(
                  "element",
                  elementType,
                  metadata = new MetadataBuilder()
                    .putBoolean(CONTAIN_NULLS, value = containsNull)
                    .build()
                )
              )
            )
            val (flattenPair, width) = flattenStrucType(arrStruct, indent + 1, 5)
            (pair ++ flattenPair, scala.math.max(newMaxWidth, width))
          case _ => (pair, newMaxWidth)
        }
      }

    val (treeFieldPair1, tree1MaxWidth) = flattenStrucType(actualSchema, 0)
    val (treeFieldPair2, _)             = flattenStrucType(expectedSchema, 0)
    val (treePair, maxWidth) = treeFieldPair1
      .zipAll(treeFieldPair2, (0, null), (0, null))
      .foldLeft((Seq.empty[(String, String)], 0)) { case ((acc, maxWidth), ((indent1, field1), (indent2, field2))) =>
        val (prefix1, prefix2)             = getIndentPair(indent1, indent2)
        val (name1, name2)                 = getNamePair(field1, field2)
        val (dtype1, dtype2)               = getDataTypePair(field1, field2)
        val (nullable1, nullable2)         = getNullablePair(field1, field2)
        val (containNulls1, containNulls2) = getContainNullsPair(field1, field2)
        val structString1                  = formatField(field1, prefix1, name1, dtype1, nullable1, containNulls1)
        val structString2                  = formatField(field2, prefix2, name2, dtype2, nullable2, containNulls2)

        (acc :+ (structString1, structString2), math.max(maxWidth, structString1.length))
      }

    val schemaGap = maxWidth + TREE_GAP
    val headerGap = tree1MaxWidth + TREE_GAP
    treePair
      .foldLeft(new StringBuilder("\nActual Schema".padTo(headerGap, ' ') + "Expected Schema\n")) { case (sb, (s1, s2)) =>
        val gap = if (s1.isEmpty) headerGap else schemaGap
        val s   = if (s2.isEmpty) s1 else s1.padTo(gap, ' ')
        sb.append(s + s2 + "\n")
      }
      .toString()
  }

  private def getDescriptionPair(nullable: String, containNulls: String): String =
    if (containNulls.isEmpty) s"(nullable = $nullable)" else s"(containsNull = $containNulls)"

  private def formatField(field: StructField, prefix: String, name: String, dtype: String, nullable: String, containNulls: String): String = {
    if (field == null) {
      ""
    } else {
      val description = getDescriptionPair(nullable, containNulls)
      s"$prefix $name : $dtype $description"
    }
  }

  private def getContainNullsPair(field1: StructField, field2: StructField): (String, String) = {
    (field1, field2) match {
      case (f, null) =>
        val containNulls = Try(f.metadata.getBoolean(CONTAIN_NULLS).toString).getOrElse("")
        Red(containNulls).toString -> ""
      case (null, f) =>
        val containNulls = Try(f.metadata.getBoolean(CONTAIN_NULLS).toString).getOrElse("")
        "" -> Green(containNulls).toString
      case (StructField(_, _, _, m1), StructField(_, _, _, m2)) =>
        val isArrayElement1 = m1.contains(CONTAIN_NULLS)
        val isArrayElement2 = m2.contains(CONTAIN_NULLS)
        if (isArrayElement1 && isArrayElement2) {
          val containNulls1 = m1.getBoolean(CONTAIN_NULLS)
          val containNulls2 = m2.getBoolean(CONTAIN_NULLS)
          val (cn1, cn2) = if (containNulls1 != containNulls2) {
            (Red(containNulls1.toString), Green(containNulls2.toString))
          } else {
            (DarkGray(containNulls1.toString), DarkGray(containNulls2.toString))
          }
          (cn1.toString, cn2.toString)
        } else if (isArrayElement1) {
          (DarkGray(m1.getBoolean(CONTAIN_NULLS).toString).toString, "")
        } else if (isArrayElement2) {
          ("", DarkGray(m2.getBoolean(CONTAIN_NULLS).toString).toString)
        } else {
          ("", "")
        }
    }
  }

  private def getIndentPair(indent1: Int, indent2: Int): (String, String) = {
    def depthToIndentStr(depth: Int): String = Range(0, depth).map(_ => "|    ").mkString + "|--"
    val prefix1                              = depthToIndentStr(indent1)
    val prefix2                              = depthToIndentStr(indent2)
    val (p1, p2) = if (indent1 != indent2) {
      (Red(prefix1), Green(prefix2))
    } else {
      (DarkGray(prefix1), DarkGray(prefix2))
    }
    (p1.toString, p2.toString)
  }

  private def getNamePair(field1: StructField, field2: StructField): (String, String) = (field1, field2) match {
    case (_: StructField, null)         => (Red(field1.name).toString, "")
    case (null, _: StructField)         => ("", Green(field2.name).toString)
    case (f1, f2) if f1.name == f2.name => (DarkGray(field1.name).toString, DarkGray(field2.name).toString)
    case (f1, f2) if f1.name != f2.name => (Red(field1.name).toString, Green(field2.name).toString)
  }

  private def getDataTypePair(field1: StructField, field2: StructField): (String, String) = {
    (field1, field2) match {
      case (f: StructField, null)                 => (Red(f.dataType.typeName).toString, "")
      case (null, f: StructField)                 => ("", Green(f.dataType.typeName).toString)
      case (f1, f2) if f1.dataType == f2.dataType => (DarkGray(f1.dataType.typeName).toString, DarkGray(f2.dataType.typeName).toString)
      case (f1, f2) if f1.dataType != f2.dataType => (Red(f1.dataType.typeName).toString, Green(f2.dataType.typeName).toString)
    }
  }

  private def getNullablePair(field1: StructField, field2: StructField): (String, String) = {
    (field1, field2) match {
      case (f: StructField, null)                 => (Red(f.nullable.toString).toString, "")
      case (null, f: StructField)                 => ("", Green(f.nullable.toString).toString)
      case (f1, f2) if f1.nullable == f2.nullable => (DarkGray(f1.nullable.toString).toString, DarkGray(f2.nullable.toString).toString)
      case (f1, f2) if f1.nullable != f2.nullable => (Red(f1.nullable.toString).toString, Green(f2.nullable.toString).toString)
    }
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
    if (!SchemaComparer.equals(actualSchema, expectedSchema, ignoreNullable, ignoreColumnNames, ignoreColumnOrder, ignoreMetadata)) {
      val diffString = outputFormat match {
        case SchemaDiffOutputFormat.Tree  => treeSchemaMismatchMessage(actualSchema, expectedSchema)
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
