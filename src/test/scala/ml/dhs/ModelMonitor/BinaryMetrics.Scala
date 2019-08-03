package ml.dhs.modelmonitor

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}

object CreateDataTests {
    def create_dataset(sc:SparkContext, sqlCtx:SQLContext):DataFrame={
        import sqlCtx.implicits._
        return sc.parallelize(Seq(
            ("val1", 1.0, 0.0),
            ("val1", 1.0, 0.0),
            ("val1", 1.0, 1.0),
            ("val1", 0.0, 1.0),
            ("val1", 0.0, 1.0),
            ("val1", 0.0, 1.0),
            ("val1", 0.0, 0.0),
            ("val1", 0.0, 0.0),
            ("val1", 0.0, 0.0),
            ("val1", 0.0, 0.0),
            ("val2", 1.0, 0.0),
            ("val2", 1.0, 0.0),
            ("val2", 1.0, 0.0),
            ("val2", 1.0, 1.0),
            ("val2", 1.0, 1.0),
            ("val2", 0.0, 1.0),
            ("val2", 0.0, 1.0),
            ("val2", 0.0, 1.0),
            ("val2", 0.0, 1.0),
            ("val2", 0.0, 1.0),
            ("val2", 0.0, 0.0)
        )).toDF("group", "label", "prediction")
    }
}

class ConfusionMatrixTest extends FunSuite with DataFrameSuiteBase{
    test("it returns confusion matrix correctly") {
        val sqlCtx = sqlContext
        import sqlCtx.implicits._
        val dataset=CreateDataTests.create_dataset(sc, sqlCtx)
        val expected=BinaryConfusionMatrix(
            5, 3, 8, 5
        )
        
        val results=BinaryMetrics.getConfusionMatrix(
            dataset
        )
        assert(results===expected)
    }
}

class ConfusionMatrixByGroupTest extends FunSuite with DataFrameSuiteBase{
    test("it returns confusion matrix by group") {
        val sqlCtx = sqlContext
        import sqlCtx.implicits._
        val dataset=CreateDataTests.create_dataset(sc, sqlCtx)
        val expected=Map(
            "val1"->BinaryConfusionMatrix(
                4, 1, 3, 2
            ),
            "val2"->BinaryConfusionMatrix(
                1, 2, 5, 3
            )
        )
        
        val results=BinaryMetrics.getBinaryConfusionMatrix(
            dataset
        )
        assert(results===expected)
    }
}