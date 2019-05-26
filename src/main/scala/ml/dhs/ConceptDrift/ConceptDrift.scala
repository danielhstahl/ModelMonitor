package ml.dhs.ModelMonitor
import scala.math
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
}