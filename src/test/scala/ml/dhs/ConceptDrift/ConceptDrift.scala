package ml.dhs.ModelMonitor

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class CreateDataTests {
    def create_train_dataset(spark:SparkSession):DataFrame={
        val data=spark.sparkContext.parallelize(Seq(
            ("Closed with non-monetary relief","Branch"),
            ("Closed with monetary relief","Customer Meeting"),
            ("Closed with explanation","Branch"),
            ("Closed with monetary relief","Branch"),
            ("Closed with monetary relief","Customer Meeting"),
            ("Closed with non-monetary relief","Branch")
        ))
        return spark.createDataFrame(data).toDF("actioncode", "origin")
    }
    def create_test_dataset(spark:SparkSession):DataFrame={
        val data=spark.sparkContext.parallelize(Seq(
            ("Closed with non-monetary relief","Branch"),
            ("Closed with monetary relief","Customer Meeting"),
            ("Closed with explanation","Branch"),
            ("Closed with monetary relief","Branch"),
            ("Closed with non-monetary relief","Customer Meeting"),
            ("Closed with non-monetary relief","Branch"),
            ("Closed with monetary relief","Customer Meeting"),
            ("Closed with explanation","Branch"),
            ("Closed with monetary relief","Branch"),
            ("Closed with monetary relief","Customer Meeting"),
            ("Closed with non-monetary relief","Branch")
        ))
        return spark.createDataFrame(data).toDF("actioncode", "origin")
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
class ComputeHellingerNumerical extends FunSuite {
    test("returns correct value"){
        val prevDist=Array(0.05, 0.15, 0.3, 0.3, 0.15, 0.05)
        val newDist=Array(0.06, 0.15, 0.29, 0.29, 0.15, 0.06)
        val result=ConceptDrift.hellingerNumerical(prevDist, newDist)
        assert(result === 0.02324307098603245)
    }
}

/*class ComputeBreaksTest extends FunSpec with BeforeAndAfterAll  {
    private SparkSession spark
    override def beforeAll() {
        spark = SparkSession.builder.master("local").appName("Test Application").getOrCreate()
    }
    override def afterAll() {
        spark.stop()
    }


    test("CubeCalculator.cube") {
        assert(CubeCalculator.cube(3) === 27)
    }
}*/