package com.github.mrpowers.spark.fast.tests

import org.apache.spark.sql.types.{StructField, StructType}

object SchemaComparer {

  def equals(s1: StructType,
             s2: StructType,
             ignoreNullable: Boolean = false): Boolean = {
    if (s1.length != s2.length) {
      false
    } else {
      val structFields: Seq[(StructField, StructField)] = s1.zip(s2)
      structFields.forall { t =>
        t._1.name == t._2.name && t._1.dataType == t._2.dataType
      }
    }
  }
}
