package com.github.mrpowers.spark.fast.tests.api

/**
 * Abstract representation of a Row that works across different DataFrame implementations (Spark, Snowpark, etc.)
 */
trait RowLike extends Serializable {

  /** Total number of columns in this row */
  def length: Int

  /** Total number of columns in this row. Alias of [[length]] */
  def size: Int = length

  /** Returns the value at the given index */
  def get(index: Int): Any

  /** Returns the value at the given index. Alias of [[get]] */
  def apply(index: Int): Any = get(index)

  /** Returns true if the value at the given index is null */
  def isNullAt(index: Int): Boolean = get(index) == null

  /** Converts this row to a Seq */
  def toSeq: Seq[Any]

  def schema: SchemaLike
}
