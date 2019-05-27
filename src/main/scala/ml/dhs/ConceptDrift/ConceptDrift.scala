package ml.dhs.ModelMonitor
import scala.math
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions
import org.apache.spark.ml.feature.Bucketizer
import org.apache.spark.sql.Row

import org.json4s._
import org.json4s.native.JsonMethods._
import org.json4s.jackson.Serialization.{read, write}

import java.io._
import scala.io.Source

object ConceptDrift {
    final val SQRT2=math.sqrt(2.0)
    final val NUM_BINS=3
    def computeBreaks(min: Double, max: Double, numBins: Int):Array[Double]={
        val binWidth:Double=(max-min)/numBins
        val breaks=Array.tabulate(numBins+1)(i => min+binWidth*i)
        breaks(0)=Double.NegativeInfinity
        breaks(numBins)=Double.PositiveInfinity
        return breaks
    }
    def hellingerNumerical(
        prevDist:Array[Double], newDist:Array[Double]
    ):Double={
        return math.sqrt(prevDist.zip(newDist).map({ case (p, q) => math.pow(math.sqrt(p)-math.sqrt(q), 2)}).sum)/SQRT2
    }
    def getInitialElementIfNoNumeric(
        numericColumnNameArray:Array[String], 
        columnNameAndTypeArray:Array[(String, String)]
    ):Array[String]={
        return if (numericColumnNameArray.length>0) numericColumnNameArray else Array(columnNameAndTypeArray(0)._1)
    }
    def getNamesOfNumericColumns(
        columnNameAndTypeArray:Array[(String, String)]
    ):Array[String]={
        return columnNameAndTypeArray
            .filter({case (name, value)=>value==ColumnType.Numeric.toString})
            .map({case (name, value)=>name})
    }

    def zipper(map1: Map[String, Double], map2: Map[String, Double]):Array[(String, Double, Double)] = {
        (map1.keys ++ map2.keys)
            .map(key=>(key, map1.getOrElse(key, 0.0), map2.getOrElse(key, 0.0)))
            .toArray
    }

    def hellingerCategorical(
        prevDist:Map[String, Double],
        newDist:Map[String, Double]
    ):Double={
        val dists=zipper(prevDist, newDist)
        val prevNum=dists.map(v=>v._2)
        val newNum=dists.map(v=>v._3)
        hellingerNumerical(prevNum, newNum)
    }

    def computeMinMax(sparkDataFrame:DataFrame, columnNameArray:Array[String]): Map[String, AnyVal]={
        val arrayOfAggregations=columnNameArray.flatMap(v=>List(
            functions.min(sparkDataFrame(v)), functions.max(sparkDataFrame(v))
        ))
        val row= sparkDataFrame.agg(functions.count(sparkDataFrame(columnNameArray(0))), arrayOfAggregations:_*).first
        return row.getValuesMap[AnyVal](row.schema.fieldNames)
    }

    def getNumericDistribution(
        sparkDataFrame:DataFrame,
        colName:String,
        bins:Array[Double],
        n:Long
    ):Array[Double]={
        val newColName=colName+"buckets"
        val bucketizer= new Bucketizer()
            .setInputCol(colName)
            .setOutputCol(newColName)
            .setSplits(bins)
        val df_buck=bucketizer.setHandleInvalid("keep").transform(sparkDataFrame.select(colName))

        val frame=df_buck.groupBy(newColName)
            .agg(functions.count(colName).as("count"))
            .orderBy(functions.asc(newColName))
        return frame.collect.toArray.map(r=>r.getLong(1).toDouble/n)
    }

    def getCategoricalDistribution(
        sparkDataFrame:DataFrame,
        colName:String,
        n:Long
    ):Map[String, Double]={
        val frame=sparkDataFrame.groupBy(colName)
            .agg(functions.count(colName).as("count"))
        return frame.collect.toArray.map(r=>(r.getString(0), r.getLong(1).toDouble/n)).toMap
    }


    def getDistributionsHelper(
        computeMinMax: (DataFrame, Array[String])=>Map[String, AnyVal],
        getNumericDistribution: (DataFrame, String, Array[Double], Long)=>Array[Double],
        getCategoricalDistribution: (DataFrame, String, Long)=>Map[String, Double],
        numBins: Int
    ):(DataFrame, Array[(String, String)])=>Map[String, Any]={
        (sparkDataFrame: DataFrame, columnNameAndTypeArray:Array[(String, String)])=>{
            val numericColumnArray=getNamesOfNumericColumns(columnNameAndTypeArray)
            val minMaxArray=getInitialElementIfNoNumeric(numericColumnArray, columnNameAndTypeArray)
            val minAndMax=computeMinMax(sparkDataFrame, minMaxArray)
            val n=minAndMax(s"count(${minMaxArray(0)})").asInstanceOf[Long] //make sure to test with all numeric and all categorical
            val numericalBins=if (numBins==0) math.max(math.floor(math.sqrt(n)), NUM_BINS).toInt else numBins
            val fields=columnNameAndTypeArray.map({case (name, columnType)=>(
                name, 
                Map(
                    "distribution"-> (
                        if (columnType==ColumnType.Numeric.toString) getNumericDistribution(
                            sparkDataFrame, name, 
                            computeBreaks(
                                minAndMax(s"min(${name})").asInstanceOf[Double], 
                                minAndMax(s"max(${name})").asInstanceOf[Double], 
                                numericalBins
                            ), n
                        ) 
                        else getCategoricalDistribution(
                            sparkDataFrame, name, n
                        )
                    ),
                    "type"->columnType
                )
            )}).toMap
            Map(
                "fields"->fields,
                "numNumericalBins"->numericalBins
            )
        }
    }

    def getDistributions=getDistributionsHelper(
        computeMinMax, getNumericDistribution, getCategoricalDistribution, 0
    )

    def saveDistribution(distribution:Map[String, Any], file:String):Boolean={
        implicit val formats = DefaultFormats
        val f = new File(file)
        val bw = new BufferedWriter(new FileWriter(f))
        bw.write(write(distribution))
        bw.close()
        return true
    }
    def loadDistribution(file:String):Map[String, Any]={
        implicit val formats = DefaultFormats
        val bufSrc=Source.fromFile(file)
        val fileContents =bufSrc.getLines.mkString
        bufSrc.close
        return parse(fileContents).extract[Map[String, Any]]
        
    }
    

}