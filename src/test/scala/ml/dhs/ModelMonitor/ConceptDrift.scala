package ml.dhs.modelmonitor

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}

import org.json4s._
import org.json4s.native.JsonMethods._
import org.json4s.jackson.Serialization.{read, write}

object CreateDataTests {
    def create_train_dataset(sc:SparkContext, sqlCtx:SQLContext):DataFrame={
        //val sqlContext = new SQLContext(sc)
        import sqlCtx.implicits._
        return sc.parallelize(Seq(
            ("text1","val1", 2.0),
            ("text2","val2", 2.4),
            ("text3","val1", 1.4),
            ("text2","val1", -1.2),
            ("text2","val2", 1.2),
            ("text1","val1", 5.4)
        )).toDF("v1", "v2", "exampleNumeric")
    }
    def create_test_dataset(sc:SparkContext, sqlCtx:SQLContext):DataFrame={
        //val sqlContext = new SQLContext(sc)
        import sqlCtx.implicits._
        return sc.parallelize(Seq(
            ("text1","val1", 2.0),
            ("text2","val2", 2.4),
            ("text3","val1", 1.4),
            ("text2","val1", -1.2),
            ("text1","val2", 1.2),
            ("text1","val1", 5.4),
            ("text2","val2", 2.4),
            ("text3","val1", 1.4),
            ("text2","val1", -1.2),
            ("text2","val2", 1.2),
            ("text1","val1", 5.4)
        )).toDF("v1", "v2", "exampleNumeric")
    }
}

class ComputeBreaksTest extends FunSuite   {
    test("min and max inclusive") {
        val expected=Array(Double.NegativeInfinity, 2.0, Double.PositiveInfinity)
        val min=1.0
        val max=3.0
        val numBins=2
        val results=ConceptDrift.computeBreaks(min, max, numBins)
        assert(expected.length === results.length)
        for ((e, r) <- expected.zip(results)){
            assert(e === r)
        }
    }
    test("returns one more break than bins") {
        val min=1.0
        val max=3.0
        val numBins=2
        val results=ConceptDrift.computeBreaks(min, max, numBins)
        assert(numBins+1 === results.length)
    }
}
class HellingerNumericalTest extends FunSuite {
    test("returns correct value"){
        val prevDist=Array(0.05, 0.15, 0.3, 0.3, 0.15, 0.05)
        val newDist=Array(0.06, 0.15, 0.29, 0.29, 0.15, 0.06)
        val result=ConceptDrift.hellingerNumerical(prevDist, newDist)
        assert(result === 0.02324307098603245)
    }
}

class GetColumnNameAndTypeArrayTest extends FunSuite {
    test("it returns name and type"){
        val distribution=FieldsBins(
            Map(
                "v1" -> DistributionHolder(
                    Right(Map(
                        "text1"->0.3333333333333333,
                        "text2"->0.5,
                        "text3"->0.16666666666666666
                    )),      
                    ColumnType.Categorical.toString        
                ),
                "v2" -> DistributionHolder(
                    Right(Map(
                        "val1"->0.6666666666666666,
                        "val2"->0.3333333333333333
                    )),
                    ColumnType.Categorical.toString
                ),
                "exampleNumeric" -> DistributionHolder(
                    Left(Array(
                        0.16666666666666666,
                        0.6666666666666666,
                        0.16666666666666666
                    )),
                    ColumnType.Numeric.toString
                )
            ),
            3
        )
        val result=ConceptDrift.getColumnNameAndTypeArray(distribution)
        assert(result(0).name=="v1")
        assert(result(0).columnType==ColumnType.Categorical.toString)
        assert(result(1).name=="v2")
        assert(result(1).columnType==ColumnType.Categorical.toString)
        assert(result(2).name=="exampleNumeric")
        assert(result(2).columnType==ColumnType.Numeric.toString)
    }
}

class InitialElementIfNoNumericTest extends FunSuite {
    test("it gets name of first category"){
        val columnNameAnyTypeArray=Array(
            ColumnDescription("hello", ColumnType.Categorical.toString),
            ColumnDescription("world", ColumnType.Categorical.toString)
        )
        val numericArray:Array[String]=Array()
        val result=ConceptDrift.getInitialElementIfNoNumeric(numericArray, columnNameAnyTypeArray)
        val expected=Array("hello")
        for ((e, r) <- expected.zip(result)){
            assert(e === r)
        }
        assert(result.length === 1)
    }
    test("it returns numeric array if it exists"){
        val columnNameAnyTypeArray=Array(
            ColumnDescription("hello", ColumnType.Categorical.toString),
            ColumnDescription("world", ColumnType.Categorical.toString)
        )
        val numericArray:Array[String]=Array("goodbye", "cruel world")
        val result=ConceptDrift.getInitialElementIfNoNumeric(numericArray, columnNameAnyTypeArray)
        for ((e, r) <- numericArray.zip(result)){
            assert(e === r)
        }
        assert(result.length === numericArray.length)
    }
}

class GetNumericColumnsTest extends FunSuite {
    test("it gets only numeric columns"){
        val expected=Array("hello", "world")
        val columnNameAnyTypeArray=Array(
            ColumnDescription("goodbye", ColumnType.Categorical.toString),
            ColumnDescription("hello", ColumnType.Numeric.toString),
            ColumnDescription("world", ColumnType.Numeric.toString)
        ) 
        val result=ConceptDrift.getNamesOfNumericColumns(columnNameAnyTypeArray)
        for ((e, r) <- expected.zip(result)){
            assert(e === r)
        }
    }
}
class ZipperTest extends FunSuite {
    test("it gets all frequency from original list when less from new list"){
        val expected=Array(
            ("hello", 4.0, 0.0), 
            ("goodbye", 5.0, 2.0)
        )
        val l1=Map("hello"->4.0, "goodbye"->5.0)
        val l2=Map("goodbye"->2.0)
        val result=ConceptDrift.zipper(l1, l2)
        for ((e, r) <- expected.zip(result)){
            assert(e._1 === r._1)
            assert(e._2 === r._2)
            assert(e._3 === r._3)
        }
    }
    test("it gets all frequency from new list when v2 less than new list"){
        val expected=Array(
            ("hello", 4.0, 3.0), 
            ("goodbye", 5.0, 2.0),
            ("farewell", 0.0, 2.0)
        )
        val l1=Map("hello"->4.0, "goodbye"->5.0)
        val l2=Map("hello"->3.0, "goodbye"->2.0, "farewell"->2.0)
        val result=ConceptDrift.zipper(l1, l2)
        for ((e, r) <- expected.zip(result)){
            assert(e._1 === r._1)
            assert(e._2 === r._2)
            assert(e._3 === r._3)
        }
    }
}

class ComputeMinMaxTest extends FunSuite with DataFrameSuiteBase /*with SharedSparkContext*/{
    test("It returns min and max") {
        val sqlCtx = sqlContext
        import sqlCtx.implicits._
        val trainDataset=CreateDataTests.create_train_dataset(sc, sqlCtx)

        val minMax=ConceptDrift.computeMinMax(trainDataset, Array("exampleNumeric"))
        assert(minMax("min(exampleNumeric)")===(-1.2))
        assert(minMax("max(exampleNumeric)")===5.4)
        assert(minMax("count(exampleNumeric)")===6)
    }
}

class GetNumericDistributionTest extends FunSuite with DataFrameSuiteBase /*with SharedSparkContext*/{
    test("It gets numeric distribution"){
        val sqlCtx = sqlContext
        import sqlCtx.implicits._
        val trainDataset=CreateDataTests.create_train_dataset(sc, sqlCtx)
        val bins=ConceptDrift.computeBreaks(-1.2, 5.4, 3)
        val result=ConceptDrift.getNumericDistribution(
            trainDataset, "exampleNumeric", bins, 6
        )
        val expected=Array(0.16666666666666666, 0.6666666666666666, 0.16666666666666666)
        for ((e, r) <- expected.zip(result)){
            assert(e === r)
        }
    }
}
class GetCategoricalDistributionTest extends FunSuite with DataFrameSuiteBase /*with SharedSparkContext*/{
    test("It gets numeric distribution"){
        val sqlCtx = sqlContext
        import sqlCtx.implicits._
        val trainDataset=CreateDataTests.create_train_dataset(sc, sqlCtx)
        val result=ConceptDrift.getCategoricalDistribution(
            trainDataset, "v2", 6
        )
        val expected=Map(
            "val1"->0.666666666666666666,
            "val2"->0.333333333333333333
        )
        assert(result("val1")===expected("val1"))
        assert(result("val2")===expected("val2"))
    }
}

class GetDistributionsTest extends FunSuite with DataFrameSuiteBase  {
    test("It returns map of distributions") {
        val sqlCtx = sqlContext
        import sqlCtx.implicits._
        val trainDataset=CreateDataTests.create_train_dataset(sc, sqlCtx)
        val columnNameAnyTypeArray=Array(
            ColumnDescription("v1", ColumnType.Categorical.toString),
            ColumnDescription("v2", ColumnType.Categorical.toString),
            ColumnDescription("exampleNumeric", ColumnType.Numeric.toString)
        )
        val results=ConceptDrift.getDistributions(
            trainDataset, columnNameAnyTypeArray
        )
        val expected=FieldsBins(
            Map(
                "v1" -> DistributionHolder(
                    Right(Map(
                        "text3"->0.16666666666666666,
                        "text2"->0.5,
                        "text1"->0.3333333333333333
                    )),                    
                    ColumnType.Categorical.toString
                ),
                "v2" -> DistributionHolder(
                    Right(Map(
                        "val1"->0.6666666666666666,
                        "val2"->0.3333333333333333
                    )),
                    ColumnType.Categorical.toString
                ),
                "exampleNumeric" -> DistributionHolder(
                    Left(Array(
                        0.16666666666666666,
                        0.6666666666666666,
                        0.16666666666666666
                    )),
                    ColumnType.Numeric.toString
                )
            ),
            3
        )
        implicit val formats = DefaultFormats
        val expectedJson=write(expected)
        val resultsJson=write(results)

        assert(expectedJson === resultsJson)
    }
    test("It works with only numeric fields"){
        val sqlCtx = sqlContext
        import sqlCtx.implicits._
        val trainDataset=CreateDataTests.create_train_dataset(sc, sqlCtx)
        val columnNameAnyTypeArray=Array(
            ColumnDescription("exampleNumeric", ColumnType.Numeric.toString)
        )
        val results=ConceptDrift.getDistributions(
            trainDataset, columnNameAnyTypeArray
        )
        val expected=Map(
            "fields" -> Map(
                "exampleNumeric" -> Map(
                    "distribution" -> Array(
                        0.16666666666666666,
                        0.6666666666666666,
                        0.16666666666666666
                    ),
                    "columnType" -> ColumnType.Numeric.toString
                )
            ),
            "numNumericalBins"->3
        )
        implicit val formats = DefaultFormats
        val expectedJson=write(expected)
        val resultsJson=write(results)

        assert(expectedJson === resultsJson)
    }
    test("It works with only categorical fields"){
        val sqlCtx = sqlContext
        import sqlCtx.implicits._
        val trainDataset=CreateDataTests.create_train_dataset(sc, sqlCtx)
        val columnNameAnyTypeArray=Array(
            ColumnDescription("v1", ColumnType.Categorical.toString),
            ColumnDescription("v2", ColumnType.Categorical.toString)
        )
        val results=ConceptDrift.getDistributions(
            trainDataset, columnNameAnyTypeArray
        )
        val expected=Map(
            "fields" -> Map(
                "v1" -> Map(
                    "distribution" -> Map(
                        "text3"->0.16666666666666666,
                        "text2"->0.5,
                        "text1"->0.3333333333333333
                    ),                    
                    "columnType" -> ColumnType.Categorical.toString
                ),
                "v2" -> Map(
                    "distribution" -> Map(
                        "val1"->0.6666666666666666,
                        "val2"->0.3333333333333333
                    ),
                    "columnType" -> ColumnType.Categorical.toString
                )
            ),
            "numNumericalBins"->3
        )
        implicit val formats = DefaultFormats
        val expectedJson=write(expected)
        val resultsJson=write(results)

        assert(expectedJson === resultsJson)
    }

}

class GetNewDistributionsAndCompareTest extends FunSuite with DataFrameSuiteBase  {
    test("It compares distributions") {
        val sqlCtx = sqlContext
        import sqlCtx.implicits._
        val testDataset=CreateDataTests.create_test_dataset(sc, sqlCtx)
        val distribution=FieldsBins(Map(
                "v1" -> DistributionHolder(
                    Right(Map(
                        "text1"->0.3333333333333333,
                        "text2"->0.5,
                        "text3"->0.16666666666666666
                    )),                    
                    ColumnType.Categorical.toString
                ),
                "v2" -> DistributionHolder(
                    Right(Map(
                        "val1"->0.6666666666666666,
                        "val2"->0.3333333333333333
                    )),
                    ColumnType.Categorical.toString
                ),
                "exampleNumeric" -> DistributionHolder(
                    Left(Array(
                        0.16666666666666666,
                        0.6666666666666666,
                        0.16666666666666666
                    )),
                    ColumnType.Numeric.toString
                )
            ),
            3
        )
        val result=ConceptDrift.getNewDistributionsAndCompare(testDataset, distribution)
        assert(result("v1")>0.0)
        assert(result("v2")>0.0)
        assert(result("exampleNumeric")>0.0)

    }
    test("It returns zero when given same data"){
        val sqlCtx = sqlContext
        import sqlCtx.implicits._
        val testDataset=CreateDataTests.create_test_dataset(sc, sqlCtx)
        val columnNameAnyTypeArray=Array(
            ColumnDescription("v1", ColumnType.Categorical.toString),
            ColumnDescription("v2", ColumnType.Categorical.toString),
            ColumnDescription("exampleNumeric", ColumnType.Numeric.toString)
        )
        val distribution=ConceptDrift.getDistributions(testDataset, columnNameAnyTypeArray)
        val result=ConceptDrift.getNewDistributionsAndCompare(testDataset, distribution)
        assert(result("v1")===0.0)
        assert(result("v2")===0.0)
        assert(result("exampleNumeric")===0.0)
    }
    test("End to end integration"){
        val sqlCtx = sqlContext
        import sqlCtx.implicits._
        val testDataset=CreateDataTests.create_test_dataset(sc, sqlCtx)
        val trainDataset=CreateDataTests.create_train_dataset(sc, sqlCtx)
        val columnNameAnyTypeArray=Array(
            ColumnDescription("v1", ColumnType.Categorical.toString),
            ColumnDescription("v2", ColumnType.Categorical.toString),
            ColumnDescription("exampleNumeric", ColumnType.Numeric.toString)
        )
        val distribution=ConceptDrift.getDistributions(trainDataset, columnNameAnyTypeArray)
        val isSaved=ConceptDrift.saveDistribution(distribution, "test.json")
        val resultSaved=ConceptDrift.loadDistribution("test.json")
        val result=ConceptDrift.getNewDistributionsAndCompare(testDataset, resultSaved)
        assert(result("v1")>0.0)
        assert(result("v2")>0.0)
        assert(result("exampleNumeric")>0.0)

    }

    
}