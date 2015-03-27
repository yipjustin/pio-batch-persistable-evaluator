package my.org

import io.prediction.controller.PDataSource
import io.prediction.controller.EmptyEvaluationInfo
import io.prediction.controller.EmptyActualResult
import io.prediction.controller.Params
import io.prediction.controller.EngineParams
import io.prediction.controller.Evaluation
import io.prediction.controller.EmptyEvaluationInfo
import io.prediction.controller.WorkflowParams
import io.prediction.controller.IEngineFactory
import io.prediction.controller.EngineParamsGenerator
import io.prediction.controller.EngineParams
import io.prediction.controller.Engine
import io.prediction.core.BaseEvaluator
import io.prediction.core.BaseEvaluatorResult
import io.prediction.data.storage.Event
import io.prediction.data.storage.Storage

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors

import org.json4s.Formats
import org.json4s.DefaultFormats
import org.json4s.JObject
import org.json4s.native.Serialization

import grizzled.slf4j.Logger

case class BatchDataSourceParams(appId: Int) extends Params

class BatchDataSource(dsp: BatchDataSourceParams)
  extends DataSource(DataSourceParams(appId = dsp.appId, evalK = None)) {
  override
  def readEval(sc: SparkContext)
  : Seq[(TrainingData, EmptyEvaluationInfo, RDD[(Query, ActualResult)])] = {
    // This function only return one evaluation data set

    // Create your own query here.
    val batchQueries = Seq(
      new Query(features = Array(50, 40, 30)),
      new Query(features = Array(60, 30, 0)),
      new Query(features = Array(70, 20, 100)),
      new Query(features = Array(80, 10, 300)))

    // This is a dummy value to make the evaluation pipeline happy, we are not
    // using it.
    val dummyActual = new ActualResult(label = -1.0)

    val trainingData = readTraining(sc)

    val evalDataSet = (
      readTraining(sc),
      new EmptyEvaluationInfo(),
      sc.parallelize(batchQueries).map { q => (q, dummyActual) })

    Seq(evalDataSet)
  }
}

// A helper object for the json4s serialization
case class Row(query: Query, predictedResult: PredictedResult)
    extends Serializable

class BatchPersistableEvaluatorResult extends BaseEvaluatorResult {}

class BatchPersistableEvaluator 
extends BaseEvaluator[
    EmptyEvaluationInfo, Query, PredictedResult, ActualResult, BatchPersistableEvaluatorResult]
{
  @transient lazy val logger = Logger[this.type]

  def evaluateBase(
    sc: SparkContext,
    evaluation: Evaluation,
    engineEvalDataSet: 
      Seq[(EngineParams, Seq[(EmptyEvaluationInfo, RDD[(Query, PredictedResult, ActualResult)])])],
    params: WorkflowParams): BatchPersistableEvaluatorResult = {
    // Extract the first data, as we are only interested in the first evaluation
    // It is possible to relax this restriction, and have the output logic below to 
    // write to different directory for different engine params.
    require(engineEvalDataSet.size == 1, "There should be only one engine params")
    val evalDataSet = engineEvalDataSet.head._2
    require(evalDataSet.size == 1, "There should be only one RDD[(Q, P, A)]")
    val qpaRDD = evalDataSet.head._2

    // qpaRDD contains the 4 queries we specifed in readEval, the corresponding ppredictedResults,
    // and the dummy actual result.

    // The output directory. Better to use absolute path if you run on cluster.
    val outputDir = "batch_result"

    logger.info("Writing result to disk") 
    qpaRDD
    .map { case (q, p, a) => Row(q, p) }
    .map { row => { 
      // Convert into a json
      implicit val formats: Formats = DefaultFormats 
      Serialization.write(row) 
    } }
    .saveAsTextFile(outputDir)

    logger.info(s"Result can be found in $outputDir")

    new BatchPersistableEvaluatorResult()
  }
}

// Define a new engine that includes `BatchDataSource`
object BatchClassificationEngine extends IEngineFactory {
  def apply() = {
    new Engine(
      Map(
        "" -> classOf[DataSource],
        "batch" -> classOf[BatchDataSource]),
      Map("" -> classOf[Preparator]),
      Map("naive" -> classOf[NaiveBayesAlgorithm]),
      Map("" -> classOf[Serving]))
  }
}

object BatchEvaluation extends Evaluation {
  // Define Engine and Evaluator used in Evaluation
  
  // This is not covered in our tutorial. Instead of specifying a Metric, we can actually specify an
  // Evaluator.
  engineEvaluator = (BatchClassificationEngine(), new BatchPersistableEvaluator())
}

object BatchEngineParamsList extends EngineParamsGenerator {
  // We only interest in a single engine params.
  engineParamsList = Seq(
    EngineParams(
      dataSourceName = "batch",
      dataSourceParams = BatchDataSourceParams(appId = 18),
      algorithmParamsList = Seq(("naive", AlgorithmParams(10.0)))))
}

