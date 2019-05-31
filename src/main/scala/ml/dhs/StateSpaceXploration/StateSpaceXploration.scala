package ml.dhs.ModelMonitor
import org.apache.spark.sql.{SparkSession, DataFrame, Row, SQLContext}
import org.apache.spark.sql.types.{StructType, ArrayType, DoubleType, StringType, StructField}
import org.apache.spark.ml.feature.{PCA, QuantileDiscretizer}
import scala.util.Random
import org.apache.spark.SparkContext
case class Column(
    name: String,
    columnType: String,
    values: Either[Array[Double], Array[String]]
)

class StateSpaceXploration(val seed: Int) {
    final val DIMENSION=3
    final val NUM_BUCKETS=10
    final val INPUT_COLUMN_NAMES=(1 to DIMENSION).map(index=>"_"+(index+1).toString)
    val r=new Random(seed)
    def simulateNumericalColumn(min: Double, max: Double):Double={
        min+(max-min)*r.nextDouble
    }
    def simulateCategoricalColumn(factors: Array[String]):String={
        factors(r.nextInt(factors.length))
    }
    def convertColumnsToRow(columns:Array[Column]):Row={
        Row(columns.map(v=> v.values match {
            case Left(cval)=>simulateNumericalColumn(cval(0), cval(1))
            case Right(cval)=>simulateCategoricalColumn(cval)
        }):_*)
    }
    def generateDataSet(sc:SparkContext, sqlCtx:SQLContext, numSims:Int, columns:Array[Column]):DataFrame={
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
        getPredictionsHelper(modelResults, "features", "prediction")
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
        val predictionName="prediction"
        pcaData.withColumnRenamed(predictionCol, predictionName)
        var pcaWithColumns=pcaData.rdd.map(row=>(row.prediction, row.pcaFeatures:_*)).toDF(predictionName)
        for (col <- INPUT_COLUMN_NAMES){
            val buckets=new QuantileDiscretizer()
                .setNumBuckets(NUM_BUCKETS)
                .setInputCol(col)
                .setOutputCol("buckets"+col)
            val bucketizer=buckets.fit(pcaWithColumns)
            pcaWithColumns=bucketizer.transform(pcaWithColumns)
        }
        val results=pcaWithColumns.groupBy(INPUT_COLUMN_NAMES.map(col=>"buckets"_col):_*)
            .agg(functions.avg(predictionName))
        return results
    }
}