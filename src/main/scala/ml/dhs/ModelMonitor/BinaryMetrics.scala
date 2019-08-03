package ml.dhs.modelmonitor
import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.functions

case class BinaryConfusionMatrix(
    TN: Double,
    TP: Double,
    FN: Double,
    FP: Double
)

object BinaryMetrics{
    /**
    * Calculates confusion matrix for a binary classification problem
    * @return Confusion Matrix 
    * @param modelResults Dataframe to analyze with label and prediction columns.
    * @param labelCol Name of the label column.
    * @param predictionCol Name of the prediction column.
    */
    def getConfusionMatrix(
        modelResults:DataFrame,
        labelCol:String, //default: label
        predictionCol:String //default: prediction
    ): BinaryConfusionMatrix={
        val TNFn: (Double, Double) => Double = (label: Double, prediction:Double) => {
            if (label==prediction&&prediction==0.0) 1.0 else 0.0
        } 
        val TPFn: (Double, Double) => Double = (label: Double, prediction:Double) => {
            if (label==prediction&&prediction==1.0) 1.0 else 0.0
        } 
        val FNFn: (Double, Double) => Double = (label: Double, prediction:Double) => {
            if (label!=prediction&&prediction==0.0) 1.0 else 0.0
        } 
        val FPFn: (Double, Double) => Double = (label: Double, prediction:Double) => {
            if (label!=prediction&&prediction==1.0) 1.0 else 0.0
        } 
        val TNudf=functions.udf(TNFn)
        val TPudf=functions.udf(TPFn)
        val FNudf=functions.udf(FNFn)
        val FPudf=functions.udf(FPFn)
        val row=modelResults
            .withColumn("TN", TNudf(functions.col(labelCol), functions.col(predictionCol)))
            .withColumn("TP", TPudf(functions.col(labelCol), functions.col(predictionCol)))
            .withColumn("FN", FNudf(functions.col(labelCol), functions.col(predictionCol)))
            .withColumn("FP", FPudf(functions.col(labelCol), functions.col(predictionCol)))
            .agg(
                functions.sum("TN").as("TN"),
                functions.sum("TP").as("TP"),
                functions.sum("FN").as("FN"),
                functions.sum("FP").as("FP")
            )
            .first
        val results=row.getValuesMap[Double](row.schema.fieldNames)
        BinaryConfusionMatrix(
            results("TN"),
            results("TP"),
            results("FN"),
            results("FP")
        )
        
    }
    /**
    * Calculates confusion matrix for a binary classification problem
    * @return Confusion Matrix 
    * @param modelResults Dataframe to analyze with label and prediction columns.
    */
    def getConfusionMatrix(
        modelResults:DataFrame
    ): BinaryConfusionMatrix={
        getConfusionMatrix(
            modelResults,
            "label",
            "prediction"
        )
    }
    /**
    * Calculates confusion matrix for subsets of dataframe.  Useful
    * for determining potential adverse impact.
    * @return Confusion Matrix for each distinct value in column defined by "groupCol"
    * @param modelResults Dataframe to analize with label, prediction, and group columns.
    * @param groupCol Name of the column to group.
    * @param labelCol Name of the label column.
    * @param predictionCol Name of the prediction column.
    */
    def getConfusionMatrixByGroup(
        modelResults:DataFrame,
        groupCol: String,
        labelCol:String,
        predictionCol:String
    ):Map[String, BinaryConfusionMatrix]={
        modelResults.select(groupCol).distinct.collect.flatMap(_.toSeq).map(v=>(
            v.toString, 
            getConfusionMatrix(
                modelResults.filter(functions.col(groupCol)<=>v),
                labelCol,
                predictionCol
            )
        )).toMap
    }
    /**
    * Calculates confusion matrix for subsets of dataframe.  Useful
    * for determining potential adverse impact.
    * @return Confusion Matrix for each distinct value in column defined by "groupCol"
    * @param modelResults Dataframe to analize with label, prediction, and group columns.
    * @param groupCol Name of the column to group.
    */
    def getConfusionMatrixByGroup(
        modelResults:DataFrame,
        groupCol: String
    ):Map[String, BinaryConfusionMatrix]={
        getConfusionMatrixByGroup(
            modelResults, groupCol,
            "label", "prediction"
        )
    }

    def _convertToJava(input:Map[String, BinaryConfusionMatrix]):java.util.Map[String, BinaryConfusionMatrix]={
        import scala.collection.JavaConverters._
        input.asJava
    }

}