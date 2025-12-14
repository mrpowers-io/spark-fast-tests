package com.github.mrpowers.spark.fast.tests.comparer.schema

import com.github.mrpowers.spark.fast.tests.ufansi.Color.{DarkGray, Green, Red}
import com.github.mrpowers.spark.fast.tests.ufansi.EscapeAttr
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructField, StructType}

import scala.annotation.tailrec

case class FieldComparison(
    name: String,
    // Store additional info for when we need it
    leftInfo: Option[FieldInfo],
    rightInfo: Option[FieldInfo],
    children: Seq[FieldComparison]
)

object FieldComparison {
  val ELEMENT = "element"
  val KEY     = "key"
  val VALUE   = "value"

  def apply(
      name: String,
      leftField: Option[StructField],
      rightField: Option[StructField],
      matchFieldByName: Boolean
  ): FieldComparison = {
    val leftInfo  = leftField.map(f => FieldInfo(f))
    val rightInfo = rightField.map(f => FieldInfo(f))
    val children  = buildChildComparisons(leftField.map(_.dataType), rightField.map(_.dataType), isTopLevel = true, matchFieldByName)
    FieldComparison(name, leftInfo, rightInfo, children)
  }

  def buildComparison(s1: StructType, s2: StructType, matchFieldByName: Boolean): Seq[FieldComparison] = {
    val (leftFields, rightFields) = if (matchFieldByName) {
      (
        s1.fields.map(f => f.name -> f).toMap,
        s2.fields.map(f => f.name -> f).toMap
      )
    } else {
      (
        s1.fields.zipWithIndex.map { case (f, i) => i.toString -> f }.toMap,
        s2.fields.zipWithIndex.map { case (f, i) => i.toString -> f }.toMap
      )
    }

    // Maintain order: left fields first, then right-only fields
    val leftKeys      = leftFields.keys.toSeq
    val rightOnlyKeys = rightFields.keys.filterNot(leftKeys.contains).toSeq
    (leftKeys ++ rightOnlyKeys).map { name =>
      FieldComparison(name, leftFields.get(name), rightFields.get(name), matchFieldByName)
    }
  }

  private def buildChildComparisons(
      leftType: Option[DataType],
      rightType: Option[DataType],
      isTopLevel: Boolean,
      matchFieldByName: Boolean
  ): Seq[FieldComparison] = {
    (leftType, rightType) match {
      // Both are structs
      case (Some(st1: StructType), Some(st2: StructType)) =>
        buildComparison(st1, st2, matchFieldByName)
      case (Some(st: StructType), None) =>
        buildComparison(st, new StructType(), matchFieldByName)
      case (None, Some(st: StructType)) =>
        buildComparison(new StructType(), st, matchFieldByName)

      // Array types
      case (Some(l: ArrayType), Some(r: ArrayType)) =>
//        if (isTopLevel) {
//          buildChildComparisons(Some(l.elementType), Some(r.elementType), isTopLevel = false, matchFieldByName)
//        } else {
        Seq(
          FieldComparison(
            ELEMENT,
            Some(FieldInfo(l)),
            Some(FieldInfo(r)),
            buildChildComparisons(Some(l.elementType), Some(r.elementType), isTopLevel = false, matchFieldByName)
          )
        )
//        }

      case (Some(l: ArrayType), None) =>
        if (isTopLevel) {
          buildChildComparisons(Some(l.elementType), None, isTopLevel = false, matchFieldByName)
        } else {
          val leftInfo = Some(FieldInfo(l))
          Seq(
            FieldComparison(
              ELEMENT,
              leftInfo,
              None,
              buildChildComparisons(Some(l.elementType), None, isTopLevel = false, matchFieldByName)
            )
          )
        }

      case (None, Some(r: ArrayType)) =>
        if (isTopLevel) {
          buildChildComparisons(None, Some(r.elementType), isTopLevel = false, matchFieldByName)
        } else {
          Seq(
            FieldComparison(
              ELEMENT,
              Some(FieldInfo(r)),
              None,
              buildChildComparisons(None, Some(r.elementType), isTopLevel = false, matchFieldByName)
            )
          )
        }

      // Map types
      case (Some(l: MapType), Some(r: MapType)) =>
        Seq(
          FieldComparison(
            KEY,
            Some(FieldInfo(l.keyType)),
            Some(FieldInfo(r.keyType)),
            buildChildComparisons(Some(l.keyType), Some(r.keyType), isTopLevel = false, matchFieldByName)
          ),
          FieldComparison(
            VALUE,
            Some(FieldInfo(l.valueType)),
            Some(FieldInfo(r.valueType)),
            buildChildComparisons(Some(l.valueType), Some(r.valueType), isTopLevel = false, matchFieldByName)
          )
        )
      case (Some(l: MapType), None) =>
        Seq(
          FieldComparison(
            KEY,
            Some(FieldInfo(l.keyType)),
            None,
            buildChildComparisons(Some(l.keyType), None, isTopLevel = false, matchFieldByName)
          ),
          FieldComparison(
            VALUE,
            Some(FieldInfo(l.valueType)),
            None,
            buildChildComparisons(Some(l.valueType), None, isTopLevel = false, matchFieldByName)
          )
        )

      case (None, Some(r: MapType)) =>
        Seq(
          FieldComparison(
            KEY,
            None,
            Some(FieldInfo(r.keyType)),
            buildChildComparisons(None, Some(r.keyType), isTopLevel = false, matchFieldByName)
          ),
          FieldComparison(
            VALUE,
            None,
            Some(FieldInfo(r.valueType)),
            buildChildComparisons(None, Some(r.valueType), isTopLevel = false, matchFieldByName)
          )
        )

      // Primitive types or mismatches - no children
      case _ =>
        if (isTopLevel)
          Seq.empty
        else {
          val leftInfo  = leftType.map(t => FieldInfo(t))
          val rightInfo = rightType.map(t => FieldInfo(t))
          Seq(FieldComparison(ELEMENT, leftInfo, rightInfo, Seq.empty))
        }
    }
  }

  @tailrec
  def areFieldsEqual(
      fields: Seq[FieldComparison],
      ignoreNullable: Boolean = false,
      ignoreColumnNames: Boolean = false,
      ignoreMetadata: Boolean = true
  ): Boolean =
    fields match {
      case _ if fields.isEmpty => true
      case head +: tail =>
        val currentEqual = (head.leftInfo, head.rightInfo) match {
          case (Some(left), Some(right)) =>
            left.typeName == right.typeName &&
            (ignoreColumnNames || left.name == right.name) &&
            (ignoreNullable || left.nullable == right.nullable) &&
            (ignoreMetadata || left.metadata == right.metadata)
          case (None, None) => true
          case _            => false
        }

        if (!currentEqual)
          false
        else {
          areFieldsEqual(head.children ++ tail, ignoreNullable, ignoreColumnNames, ignoreMetadata)
        }
    }

  private val TREE_GAP = 6

  def treeSchemaMismatchMessage(
      fieldComparisons: Seq[FieldComparison],
      ignoreNullable: Boolean = false,
      ignoreMetadata: Boolean = true
  ): String = {
    def getColoredFieldInfo(
        fieldInfo: Option[FieldInfo],
        otherInfo: Option[FieldInfo],
        fallbackName: String,
        indent: Int,
        isLeftSide: Boolean
    ): String = {
      fieldInfo match {
        case None => ""
        case Some(info) =>
          val prefix       = Range(0, indent).map(_ => "|    ").mkString + "|--"
          val fieldName    = info.name.getOrElse(fallbackName)
          val typeName     = info.typeName
          val nullable     = if (ignoreNullable) "" else s"(nullable = ${info.nullable})"
          val containsNull = info.containsNull.map(cn => s"(containsNull = $cn)").getOrElse("")
          val description  = if (containsNull.nonEmpty) containsNull else nullable

          otherInfo match {
            case None =>
              val color           = if (isLeftSide) Red else Green
              val formattedPrefix = color(prefix).toString
              val formattedName   = color(fieldName).toString
              val formattedType   = color(typeName).toString
              val formattedDesc   = if (description.nonEmpty) color(description).toString else ""
              s"$formattedPrefix $formattedName : $formattedType $formattedDesc"

            case Some(other) =>
              val prefixColor     = DarkGray
              val formattedPrefix = prefixColor(prefix).toString

              val nameColor     = if (info.name == other.name) DarkGray else (if (isLeftSide) Red else Green)
              val formattedName = nameColor(fieldName).toString

              val typeColor     = if (info.typeName == other.typeName) DarkGray else (if (isLeftSide) Red else Green)
              val formattedType = typeColor(typeName).toString

              val formattedDesc = if (description.nonEmpty) {
                val descMatches = (info.nullable == other.nullable || ignoreNullable) &&
                  info.containsNull == other.containsNull
                val descColor = if (descMatches) DarkGray else (if (isLeftSide) Red else Green)
                descColor(description).toString
              } else ""

              s"$formattedPrefix $formattedName : $formattedType $formattedDesc"
          }
      }
    }

    def areFieldInfoEqual(left: Option[FieldInfo], right: Option[FieldInfo]): Boolean = {
      (left, right) match {
        case (Some(l), Some(r)) =>
          l.typeName == r.typeName &&
          (ignoreNullable || l.nullable == r.nullable) &&
          l.containsNull == r.containsNull &&
          (ignoreMetadata || l.metadata == r.metadata)
        case (None, None) => true
        case _            => false
      }
    }

    def processComparisons(comparisons: Seq[FieldComparison], indent: Int): (StringBuilder, Int) = {
      def calculateMaxWidth(comps: Seq[FieldComparison], currentIndent: Int): Int = {
        if (comps.isEmpty) 0
        else {
          val widths = comps.map { fc =>
            val leftStr       = getColoredFieldInfo(fc.leftInfo, fc.rightInfo, fc.name, currentIndent, isLeftSide = true)
            val plainLeftStr  = leftStr.replaceAll("\u001b\\[[0-9;]*m", "")
            val currentWidth  = plainLeftStr.length
            val childrenWidth = if (fc.children.nonEmpty) calculateMaxWidth(fc.children, currentIndent + 1) else 0
            math.max(currentWidth, childrenWidth)
          }
          widths.max
        }
      }

      val maxWidth = calculateMaxWidth(comparisons, indent)

      def buildOutput(comps: Seq[FieldComparison], currentIndent: Int, sb: StringBuilder): StringBuilder = {
        comps.foreach { fc =>
          val leftStr  = getColoredFieldInfo(fc.leftInfo, fc.rightInfo, fc.name, currentIndent, isLeftSide = true)
          val rightStr = getColoredFieldInfo(fc.rightInfo, fc.leftInfo, fc.name, currentIndent, isLeftSide = false)

          // Calculate padding based on max width
          val schemaGap = maxWidth + TREE_GAP
          val line = (leftStr, rightStr) match {
            case ("", r) => " " * schemaGap + r
            case (l, "") => l
            case (l, r) =>
              val plainLeftStr = l.replaceAll("\u001b\\[[0-9;]*m", "")
              val padding      = schemaGap - plainLeftStr.length
              l + " " * padding + r
          }

          sb.append(line).append("\n")

          // Process children
          if (fc.children.nonEmpty) {
            buildOutput(fc.children, currentIndent + 1, sb)
          }
        }
        sb
      }

      val contentBuilder = buildOutput(comparisons, indent, new StringBuilder())
      (contentBuilder, maxWidth)
    }

    val (contentBuilder, maxWidth) = processComparisons(fieldComparisons, 0)
    val schemaGap                  = maxWidth + TREE_GAP
    val header                     = "Actual Schema".padTo(schemaGap, ' ') + "Expected Schema\n"

    s"\n$header$contentBuilder"
  }
}
