package ml.dhs.modelmonitor
import org.apache.spark.sql.{SparkSession, DataFrame, Row, SQLContext}
import org.apache.spark.sql.types.{StructType, ArrayType, DoubleType, StringType, StructField}
import org.apache.spark.ml.feature.{PCA, QuantileDiscretizer}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.functions
import scala.util.Random
import org.apache.spark.SparkContext
import org.apache.spark.ml.PipelineModel
case class ColumnSummary(
    name: String,
    columnType: String,
    values: Either[Array[Double], Array[String]]
)

class StateSpaceXploration(val seed: Int) {
    final val DIMENSION=2
    final val NUM_BUCKETS=10
    final val INPUT_COLUMN_NAMES=Array("pca1", "pca2")
    val r=new Random(seed)
    def simulateNumericalColumn(min: Double, max: Double):Double={
        min+(max-min)*r.nextDouble
    }
    def simulateCategoricalColumn(factors: Array[String]):String={
        factors(r.nextInt(factors.length))
    }
    def convertColumnsToRow(columns:Array[ColumnSummary]):Row={
        Row(columns.map(v=> v.values match {
            case Left(cval)=>simulateNumericalColumn(cval(0), cval(1))
            case Right(cval)=>simulateCategoricalColumn(cval)
        }):_*)
    }
    def generateDataSet(sc:SparkContext, sqlCtx:SQLContext, numSims:Int, columns:Array[ColumnSummary]):DataFrame={
        val rows=(1 to numSims).map(v=>convertColumnsToRow(columns))
        val rdd=sc.makeRDD[Row](rows)
        val schema=StructType(
            columns.map(v=>{
                val colType=if(v.columnType==ColumnType.Numeric.toString){DoubleType} else {StringType}
                StructField(v.name, colType, false)
            })
        )
        sqlCtx.createDataFrame(rdd, schema)
    }
    def getPredictionsHelper(modelResults:DataFrame):DataFrame={
        getPredictionsHelper(modelResults,  "features", "prediction")
    }
    def getPredictionsHelper(
        modelResults:DataFrame,
        featuresCol:String,
        predictionCol:String
    ):DataFrame={
        val pca= new PCA()
            .setK(DIMENSION)
            .setInputCol(featuresCol)
            .setOutputCol("pcaFeatures")
        val pcaModel=pca.fit(modelResults)
        val pcaData=pcaModel.transform(modelResults)
            .select("pcaFeatures", predictionCol)

        val vecToSeq = functions.udf((v: Vector) => v.toArray)

        
        val pcaWithColumns_1=pcaData
            .select(pcaData(predictionCol), vecToSeq(pcaData("pcaFeatures")).alias("_tmp"))
        val exprs = INPUT_COLUMN_NAMES
            .zipWithIndex.map{ case (c, i) => pcaWithColumns_1("_tmp").getItem(i).alias(c) } :+ 
            pcaWithColumns_1(predictionCol)
        var pcaWithColumns=pcaWithColumns_1.select(exprs:_*)
        
        for (col <- INPUT_COLUMN_NAMES){
            val buckets=new QuantileDiscretizer()
                .setNumBuckets(NUM_BUCKETS)
                .setInputCol(col)
                .setOutputCol(col+"buckets")
            val bucketizer=buckets.fit(pcaWithColumns)
            pcaWithColumns=bucketizer.transform(pcaWithColumns)
        }
        val results=pcaWithColumns
            .groupBy(INPUT_COLUMN_NAMES.map(col=>pcaWithColumns(col+"buckets")):_*)
            .agg(functions.avg(predictionCol))
        return results
    }

    def getPredictions(
        generatedDataSet: DataFrame,
        fittedPipeline: PipelineModel,
        featuresCol:String,
        predictionCol:String
    ):DataFrame={
        val results=fittedPipeline.transform(generatedDataSet)
        getPredictionsHelper(results, featuresCol, predictionCol)
    }
    def getPredictions(
        generatedDataSet: DataFrame,
        fittedPipeline: PipelineModel
    ):DataFrame={
        getPredictions(generatedDataSet, fittedPipeline,"features", "prediction")
    }

}