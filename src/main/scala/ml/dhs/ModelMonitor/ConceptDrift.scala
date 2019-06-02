package ml.dhs.modelmonitor
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

case class DistributionHolder(
    distribution: Either[Array[Double], Map[String, Double]],
    columnType: String
)
case class FieldsBins(
    fields: Map[String, DistributionHolder],
    numNumericalBins: Int
)

/**
  * A class to help perform identify and track concept drift.
  *
  * All methods are "static" (pure) and don't require instantiation,
  */
object ConceptDrift {
    final val SQRT2=math.sqrt(2.0)
    final val NUM_BINS=3
    /**
    * Helper function
    * @return Breaks for use by Spark's Bucketizer.
    * @param min The minimum value in the column dataset.
    * @param max The maximum value in the column dataset.
    * @param numBins The number of bins desired.  Note that 
    the number of breaks is one more than the desired number 
    of bins, since the breaks are augmented by negative and 
    positive infinity.
    */
    def computeBreaks(min: Double, max: Double, numBins: Int):Array[Double]={
        val binWidth:Double=(max-min)/numBins
        val breaks=Array.tabulate(numBins+1)(i => min+binWidth*i)
        breaks(0)=Double.NegativeInfinity
        breaks(numBins)=Double.PositiveInfinity
        return breaks
    }
    /**
    * Helper function
    * @return The Hellinger distance between two arrays.
    * @param prevDist Distribution of values.  Should 
    * sum to one.
    * @param newDist Distribution of values.  Should 
    * sum to one.  Compared with prevDist
    */
    def hellingerNumerical(
        prevDist:Array[Double], newDist:Array[Double]
    ):Double={
        return math.sqrt(prevDist.zip(newDist).map({ case (p, q) => math.pow(math.sqrt(p)-math.sqrt(q), 2)}).sum)/SQRT2
    }
    /**
    * Helper function
    * @return Array of column names.
    * @param numericColumnNameArray The names of the numeric
    * columns.
    * @param columnNameAndTypeArray The names and types of
    * all columns in the dataset.
    */
    def getInitialElementIfNoNumeric(
        numericColumnNameArray:Array[String], 
        columnNameAndTypeArray:Array[(String, String)]
    ):Array[String]={
        return if (numericColumnNameArray.length>0) numericColumnNameArray else Array(columnNameAndTypeArray(0)._1)
    }
    /**
    * Helper function
    * @return Array of numeric column names.
    * @param columnNameAndTypeArray The names and types of
    * all columns in the dataset.
    */
    def getNamesOfNumericColumns(
        columnNameAndTypeArray:Array[(String, String)]
    ):Array[String]={
        return columnNameAndTypeArray
            .filter({case (name, value)=>value==ColumnType.Numeric.toString})
            .map({case (name, value)=>name})
    }
    /**
    * Helper function
    * @return Joining of two maps.  If a key exists in
    * one map and not the other, the map gets a value of
    * 0.
    * @param map1 First map.
    * @param map2 Second map.
    */
    def zipper(map1: Map[String, Double], map2: Map[String, Double]):Array[(String, Double, Double)] = {
        (map1.keys ++ map2.keys)
            .map(key=>(key, map1.getOrElse(key, 0.0), map2.getOrElse(key, 0.0)))
            .toArray
    }
    /**
    * Helper function
    * @return Hellinger distance between two distributions
    * represented by categorical variables
    * @param prevDist Distribution of categorical variable.
    * @param newDist Distribution of categorical variable.
    * Is compared with prevDist.
    */
    def hellingerCategorical(
        prevDist:Map[String, Double],
        newDist:Map[String, Double]
    ):Double={
        val dists=zipper(prevDist, newDist)
        val prevNum=dists.map(v=>v._2)
        val newNum=dists.map(v=>v._3)
        hellingerNumerical(prevNum, newNum)
    }

    /**
    * Helper function
    * @return Summary values of the variables 
    * (min, max, count)
    * @param sparkDataFrame Dataframe to operate on.
    * @param columnNameArray Variable names to include.
    */
    def computeMinMax(sparkDataFrame:DataFrame, columnNameArray:Array[String]): Map[String, AnyVal]={
        val arrayOfAggregations=columnNameArray.flatMap(v=>List(
            functions.min(sparkDataFrame(v)), functions.max(sparkDataFrame(v))
        ))
        val row= sparkDataFrame.agg(functions.count(sparkDataFrame(columnNameArray(0))), arrayOfAggregations:_*).first
        return row.getValuesMap[AnyVal](row.schema.fieldNames)
    }
    /**
    * Helper function
    * @return Distribution of numeric variable
    * @param sparkDataFrame Dataframe to operate on.
    * @param colName Name of column within the 
    * sparkDataFrame to find the distribution of.
    * @param bins Values to split on.  Usually the
    * output from computeBreaks.
    * @param n Total number of rows in sparkDataFrame.
    */
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

    /**
    * Helper function
    * @return Distribution of categorical variable
    * @param sparkDataFrame Dataframe to operate on.
    * @param colName Name of column within the 
    * sparkDataFrame to find the distribution of.
    * @param n Total number of rows in sparkDataFrame.
    */
    def getCategoricalDistribution(
        sparkDataFrame:DataFrame,
        colName:String,
        n:Long
    ):Map[String, Double]={
        val frame=sparkDataFrame.groupBy(colName)
            .agg(functions.count(colName).as("count"))
        return frame.collect.toArray.map(r=>(r.getString(0), r.getLong(1).toDouble/n)).toMap
    }

    /**
    * Helper function
    * @return Names and types of variables, 
    * extracted from summary distribution structure.
    * @param trainingDistributions Structure that
    * holds the summary information of the 
    * variables' distributions.
    */
    def getColumnNameAndTypeArray(
        trainingDistributions:FieldsBins
    ):Array[(String, String)]={
        trainingDistributions.fields.map({case (key, value)=>{
            (key, value.columnType)
        }}).toArray
    }

    /**
    * Helper function
    * @return Function which computes the distribution
    * structure.
    * @param computeMinMax Function to compute the min
    * and max for each column in the dataset
    * @param getNumericDistribution Function to compute
    * the distribution for numeric columns
    * @param getCategoricalDistribution Function to 
    * compute the distribution for categorical 
    * columns
    * @param numBins The number of bins to split each
    * numeric variable
    */
    def getDistributionsHelper(
        computeMinMax: (DataFrame, Array[String])=>Map[String, AnyVal],
        getNumericDistribution: (DataFrame, String, Array[Double], Long)=>Array[Double],
        getCategoricalDistribution: (DataFrame, String, Long)=>Map[String, Double],
        numBins: Int
    ):(DataFrame, Array[(String, String)])=>FieldsBins={
        (sparkDataFrame: DataFrame, columnNameAndTypeArray:Array[(String, String)])=>{
            val numericColumnArray=getNamesOfNumericColumns(columnNameAndTypeArray)
            val minMaxArray=getInitialElementIfNoNumeric(numericColumnArray, columnNameAndTypeArray)
            val minAndMax=computeMinMax(sparkDataFrame, minMaxArray)
            val n=minAndMax(s"count(${minMaxArray(0)})").asInstanceOf[Long] 
            val numericalBins=if (numBins==0) math.max(math.floor(math.sqrt(n)), NUM_BINS).toInt else numBins
            val fields=columnNameAndTypeArray.map({case (name, columnType)=>(
                name,
                DistributionHolder(
                    if (columnType==ColumnType.Numeric.toString) Left(
                        getNumericDistribution(
                            sparkDataFrame, name, 
                            computeBreaks(
                                minAndMax(s"min(${name})").asInstanceOf[Double], 
                                minAndMax(s"max(${name})").asInstanceOf[Double], 
                                numericalBins
                            ), n
                        )
                    )
                    else Right(
                        getCategoricalDistribution(
                            sparkDataFrame, name, n
                        )
                    ),
                    columnType
                )
            )}).toMap
            FieldsBins(fields, numericalBins)
        }
    }

    /**
    * Main function to call at model training
    * @return Structure summarizing the distribution of
    * training data set.
    * @param sparkDataFrame Training data set
    * @param columnNameAndTypeArray Names and types for
    * each variable included in the model.
    */
    def getDistributions=getDistributionsHelper(
        computeMinMax, getNumericDistribution, getCategoricalDistribution, 0
    )

    /**
    * Main function to save the summary at model training
    * @return Success.
    * @param distribution Summary structure output from
    * getDistributions.
    * @param file Name of the file to write to.
    */
    def saveDistribution(distribution:FieldsBins, file:String):Boolean={
        implicit val formats = DefaultFormats
        val f = new File(file)
        val bw = new BufferedWriter(new FileWriter(f))
        bw.write(write(distribution))
        bw.close()
        return true
    }
    /**
    * Main function to load the summary structure
    * at model monitoring
    * @return Summary structure from model training.  The
    * same structure from getDistributions.
    * @param file Name of the file to read from.
    */
    def loadDistribution(file:String):FieldsBins={
        implicit val json4sFormats: Formats = DefaultFormats.withLong
        val bufSrc=Source.fromFile(file)
        val fileContents =bufSrc.getLines.mkString
        bufSrc.close
        return parse(fileContents).extract[FieldsBins]
        
    }
    /**
    * Helper function 
    * @return Hellinger distance for each variable.
    * @param trainingDistributions Distribution of each
    * variable from the training dataset
    * @param currentDistributions Distribution of each
    * variable for testing drift
    */
    def compareDistributions(
        trainingDistributions:Map[String, DistributionHolder],
        currentDistributions:Map[String, DistributionHolder]
    ):Map[String, Double]={
        trainingDistributions.map({ case (key, value)=>{
            (
                key, 
                value.distribution match {
                    case Left(dist)=>hellingerNumerical(
                        dist,
                        currentDistributions(key).distribution.left.get
                    )
                    case Right(dist)=>hellingerCategorical(
                        dist,
                        currentDistributions(key).distribution.right.get
                    )
                }
            )
        }}).toMap
    }
    /**
    * Main function for use at monitoring 
    * @return Hellinger distance for each variable.
    * @param sparkDataFrame Dataset to monitor
    * @param trainingDistributions Summary structure
    * loaded from loadDistribution
    */
    def getNewDistributionsAndCompare(
        sparkDataFrame: DataFrame,
        trainingDistributions: FieldsBins
    ):Map[String, Double]={
        val columnNameAndTypeArray=getColumnNameAndTypeArray(trainingDistributions)
        val currentDistributions=getDistributionsHelper(
            computeMinMax, getNumericDistribution, getCategoricalDistribution,
            trainingDistributions.numNumericalBins
        )(sparkDataFrame, columnNameAndTypeArray)
        compareDistributions(
            trainingDistributions.fields, 
            currentDistributions.fields
        )
    }

}