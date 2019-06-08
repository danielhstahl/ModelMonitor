package ml.dhs.modelmonitor
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.{DataFrame, SQLContext, Row, SparkSession}
import org.apache.spark.{SparkContext}
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.ml.classification.{RandomForestClassifier}
import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer}
import scala.util.Random
import org.apache.spark.sql.functions

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
    def create_train_dataset(
        sc:SparkContext, sqlCtx:SQLContext
    ):DataFrame={
        import sqlCtx.implicits._
        val ssx=new StateSpaceXploration(42)
        val columns=Array(
            ColumnSummary("v1", "Categorical", Right(Array(
                "a", "b", "c"
            ))),
            ColumnSummary("v2", "Numeric", Left(Array(-5.0, 5.0))),
            ColumnSummary("v3", "Numeric", Left(Array(-5.0, 5.0))),
            ColumnSummary("v4", "Numeric", Left(Array(-5.0, 5.0))),
            ColumnSummary("v5", "Categorical", Right(Array(
                "f", "g", "h", "i"
            )))
        )
        val numSims=500
        val spark=SparkSession.builder.config(sc.getConf).getOrCreate()
        val explanatory=ssx.generateDataSet(spark, numSims, columns)
        val r=new Random(42)
        val convertToLabel=(v1:String, v2:Double, v3:Double, v4:Double, v5:String)=>{
            val doubleV1=if(v1=="a"){0.0} else if(v1=="b"){0.25} else {0.5}
            val doubleV5=if(v5=="f"){0.0} else if(v5=="g"){0.2} else if(v5=="h") {0.3} else {0.6}
            val threshold=doubleV1+((15.0+v2+v3+v4)/50.0)+doubleV5
            if (r.nextDouble>threshold) {1.0} else {0.0}
        }
            
        val udfConvertToLabel = functions.udf(convertToLabel)
        explanatory.withColumn("label", udfConvertToLabel($"v1", $"v2", $"v3", $"v4", $"v5"))
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
        ColumnSummary("actioncode", "Categorical", Right(Array(
            "Closed with non-monetary relief",
            "Closed with monetary relief",
            "Closed with explanation" 
        ))),
        ColumnSummary("origin", "Categorical", Right(Array(
            "Branch",
            "Customer Meeting" 
        ))),
        ColumnSummary("numericExample", "Numeric", Left(Array(
            -3.0, 6.0 
        )))
    )
    test("creates data frame with correct number of rows"){
        val ssx=new StateSpaceXploration(42)
        val spark=SparkSession.builder.config(sc.getConf).getOrCreate()
        val results=ssx.generateDataSet(spark, 30, columns)
        assert(results.collect().toArray.length===30)
    }
    test("creates data frame with correct columns"){
        val ssx=new StateSpaceXploration(42)
        val spark=SparkSession.builder.config(sc.getConf).getOrCreate()
        val results=ssx.generateDataSet(spark,  30, columns)
        val expected=Array("actioncode", "origin", "numericExample")
        for ((e, r) <- expected.zip(results.columns)){
            assert(e === r)
        }
    }
}

class GetPredictionsHelperTest extends FunSuite with DataFrameSuiteBase {
    test("columns match"){
        val sqlCtx = sqlContext
        val dataset=CreateResultsTests.create_results_dataset(sc, sqlCtx, "features", "prediction")
        val ssx=new StateSpaceXploration(42)
        val results=ssx.getPredictionsHelper(dataset)
        val expected=Array("pca1buckets", "pca2buckets", "avg(prediction)")
        for ((e, r)<-expected.zip(results.columns)){
            assert(e===r)
        }
    }
    test("columns match with different names"){
        val sqlCtx = sqlContext
        val dataset=CreateResultsTests.create_results_dataset(sc, sqlCtx, "features_col", "prediction_col")
        val ssx=new StateSpaceXploration(42)
        val results=ssx.getPredictionsHelper(dataset, "features_col", "prediction_col")
        val expected=Array("pca1buckets", "pca2buckets", "avg(prediction_col)")
        for ((e, r)<-expected.zip(results.columns)){
            assert(e===r)
        }
    }
   

}
class GetPredictionsTest extends FunSuite with DataFrameSuiteBase {
    test("full integration"){
        val sqlCtx = sqlContext
        val dataset=CreateResultsTests.create_train_dataset(sc, sqlCtx)
        val encodeV1=new StringIndexer().setInputCol("v1").setOutputCol("v1_idx")
        val encodeV5=new StringIndexer().setInputCol("v5").setOutputCol("v5_idx")
        val assembleV=new VectorAssembler().setInputCols(Array("v1_idx", "v2", "v3", "v4", "v5_idx")).setOutputCol("features")
        val model=new RandomForestClassifier().setLabelCol("label").setFeaturesCol("features").setNumTrees(10)
        val p=new Pipeline().setStages(Array(encodeV1, encodeV5, assembleV, model)).fit(dataset)
        val ssx=new StateSpaceXploration(42)
        val columns=Array(
            ColumnSummary("v1", "Categorical", Right(Array(
                "a", "b", "c"
            ))),
            ColumnSummary("v2", "Numeric", Left(Array(-5.0, 5.0))),
            ColumnSummary("v3", "Numeric", Left(Array(-5.0, 5.0))),
            ColumnSummary("v4", "Numeric", Left(Array(-5.0, 5.0))),
            ColumnSummary("v5", "Categorical", Right(Array(
                "f", "g", "h", "i"
            )))
        )
        val spark=SparkSession.builder.config(sc.getConf).getOrCreate()
        val simulatedDataSet=ssx.generateDataSet(spark, 100000, columns)
        val result=ssx.getPredictions(
            simulatedDataSet,
            p
        ).collect()
        assert(result.length<=100) //10x10, but duplicates may prevent exact match

    }
    
   

}