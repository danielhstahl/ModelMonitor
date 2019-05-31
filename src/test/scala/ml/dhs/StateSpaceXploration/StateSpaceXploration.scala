package ml.dhs.ModelMonitor

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkContext}
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}

object CreateResultsTests {
    def create_results_dataset(
        sc:SparkContext, sqlCtx:SQLContext, 
        featuresCol:String, predictionCol:String
    ):DataFrame={
        import sqlCtx.implicits._
        sc.parallelize(Seq(
            (Vectors.dense(1,2, 2), 1),
            (Vectors.dense(1,2, 1), 2),
            (Vectors.dense(1,3,2), 3),
            (Vectors.dense(1,3, 5), 4),
            (Vectors.dense(2,1, 3), 5)
        )).toDF(featuresCol, predictionCol)
    }
}

class SimulateNumericalColumnTest extends FunSuite {
    test("returns in range"){
        val minRange=0.0
        val maxRange=10.0
        val ssx=new StateSpaceXploration(42)
        for (i<-1 to 100){
            val result=ssx.simulateNumericalColumn(
                minRange, maxRange
            )
            assert(result>minRange)
            assert(result<maxRange)
        }       
    }
}

class SimulateCategoricalColumnTest extends FunSuite {
    test("returns in range"){
        val minRange=0.0
        val maxRange=10.0
        val ssx=new StateSpaceXploration(42)
        val result=ssx.simulateCategoricalColumn(Array("hello", "world"))
        assert(result=="hello"||result=="world")
    }
}

class GenerateDataSetTest extends FunSuite with DataFrameSuiteBase {
    val columns=Array(
        Column("actioncode", "Categorical", Right(Array(
            "Closed with non-monetary relief",
            "Closed with monetary relief",
            "Closed with explanation" 
        ))),
        Column("origin", "Categorical", Right(Array(
            "Branch",
            "Customer Meeting" 
        ))),
        Column("numericExample", "Numeric", Left(Array(
            -3.0, 6.0 
        )))
    )
    test("creates data frame with correct number of rows"){
        val ssx=new StateSpaceXploration(42)
        val sqlCtx = sqlContext
        val results=ssx.generateDataSet(sc, sqlCtx, 30, columns)
        assert(results.collect().toArray.length===30)
    }
    test("creates data frame with correct columns"){
        val ssx=new StateSpaceXploration(42)
        val sqlCtx = sqlContext
        val results=ssx.generateDataSet(sc, sqlCtx, 30, columns)
        val expected=Array("actioncode", "origin", "numericExample")
        for ((e, r) <- expected.zip(results.columns)){
            assert(e === r)
        }
    }
}

class GetPredictionsHelper extends FunSuite with DataFrameSuiteBase {
    val dataset=CreateResultsTests.create_results_dataset(sc, sqlContext, "features", "prediction")
    val results=getPredictionsHelper(dataset)
    
}