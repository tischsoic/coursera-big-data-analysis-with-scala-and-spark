package timeusage

import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{Column, Row}
import org.junit.{Assert, Test}
import org.junit.Assert.assertEquals

import scala.util.Random

class TimeUsageSuite {

  @Test def `'row' should convert row properly`: Unit = {
    val line = List("20030101030173","1","-1","40")

    assertEquals(Row.fromSeq(Seq("20030101030173",1,-1,40)), TimeUsage.row(line))
  }

  @Test def `'classifiedColumns' should return correct columns`: Unit = {
    val allColumns = List("t01_1", "t1801_1", "t05_2", "t1805_2", "t02_3", "t1809_3")
    val (primaryNeedsNames, workingNames, otherNames) = TimeUsage.classifiedColumns(allColumns)

    assertEquals(List("t01_1", "t1801_1").map(new Column(_)), primaryNeedsNames)
    assertEquals(List("t05_2", "t1805_2").map(new Column(_)), workingNames)
    assertEquals(List("t02_3", "t1809_3").map(new Column(_)), otherNames)
  }

  @Test def `'timeUsageGrouped', 'timeUsageGroupedSql', 'timeUsageGroupedTyped'`: Unit = {
    val correctResultsData = List(
      ("not working", "female", "active",	1.1, 1d, 1.8),
      ("not working", "female", "elder", 1.3, 1.7, 1d),
      ("not working", "female", "young", 1.4, 1.4, 1.9),
      ("not working", "male", "active",	0.8, 3.3, 1.6),
      ("not working", "male", "elder", 1.8,	2.4, 0.4),
      ("not working", "male", "young", 0.7,	0.8, 1.8),
      ("working", "female", "active",	1.3, 1.3, 0.7),
      ("working", "female", "elder", 1.6,	1d, 1.2),
      ("working", "female", "young", 1.4,	1.2, 0.9),
      ("working", "male", "active", 0.5, 1.9,	1d),
      ("working", "male", "elder", 1.4,	0.8, 1.9),
      ("working", "male", "young", 1.6,	1.3, 0.6))
    val fieldsString = List("working", "sex", "age").map(StructField(_, StringType, nullable = false))
    val fieldsDouble = List("primaryNeeds", "work", "other").map(StructField(_, DoubleType, nullable = true))
    val fields = fieldsString ++ fieldsDouble
    val schema = StructType(fields)
    val rdd = TimeUsage.spark.sparkContext.parallelize(correctResultsData.map(Row.fromTuple(_)))
    val expectedDf = TimeUsage.spark.createDataFrame(rdd, schema)
    val testDataFilePath = "src/test/scala/timeusage/atussum_for-tests.csv"
    val (columns, initDf) = TimeUsage.read(testDataFilePath)
    val (primaryNeedsColumns, workColumns, otherColumns) = TimeUsage.classifiedColumns(columns)
    val summaryDf = TimeUsage.timeUsageSummary(primaryNeedsColumns, workColumns, otherColumns, initDf)

    val finalDf = TimeUsage.timeUsageGrouped(summaryDf)
    assertEquals(expectedDf.schema, finalDf.schema)
    assertEquals(expectedDf.collect().toList, finalDf.collect().toList)

    val finalDfSql = TimeUsage.timeUsageGroupedSql(summaryDf)
    assertEquals(expectedDf.schema, finalDfSql.schema)
    assertEquals(expectedDf.collect().toList, finalDfSql.collect().toList)

    val correctResultsDs = correctResultsData.map({
      case (working, sex, age, primaryNeeds, work, other) =>
        TimeUsageRow(working, sex, age, primaryNeeds, work, other)
    })
    val summaryDs = TimeUsage.timeUsageSummaryTyped(summaryDf)
    val finalDfTyped = TimeUsage.timeUsageGroupedTyped(summaryDs)
    assertEquals(correctResultsDs, finalDfTyped.collect().toList)
  }

}
