# Batch Persisting Query and Prediction with PredictionIO

This example is based on the [classification template](
http://docs.prediction.io/templates/classification/quickstart/). Please first
import the data.

```
$ python data/import_eventserver.py --access_key <YOUR_ACCESS_KEY>
```


# Build and Run. Require at least PredictionIO 0.9.1


```
$ pio build
...
$ pio eval my.org.BatchEvaluation my.org.BatchEngineParamsList 
...
[INFO] [BatchPersistableEvaluator] Writing result to disk
[INFO] [BatchPersistableEvaluator] Result can be found in batch_result          
[INFO] [CoreWorkflow$] Updating evaluation instance with result:
my.org.BatchPersistableEvaluatorResult@46761362
[INFO] [CoreWorkflow$] runEvaluation completed
```

You can find the result under `batch_result`

```
$ ls batch_result/
_SUCCESS	part-00000	part-00001	part-00002	part-00003	part-00004	part-00005	part-00006	part-00007
$ cat batch_result/part-00001 
{"query":{"features":[50.0,40.0,30.0]},"predictedResult":{"label":2.0}}
```


# PredictionIO Evaluation Module Explained

When we run `pio eval`, the first parameter is the static object for the
evaluation definition, and the second parameter is the list of `EngineParams` we
want to test. The evaluation definition contains two main components, 1. the
engine, 2. the `BaseEvaluator`. `BaseEvaluator` is responsible to evaluating the
quality of engine instances.

`pio eval` kicks start the evaluation process. For each engine params specified
in the list, PredictionIO instantiates an engine instance. For each engine
instance, `DataSource.readEval` is invoked and generate a list of training and
testing data sets. (Please refer to PIO documentation site for detailed
description). Each engine instance produces (roughly) a list of
`RDD[(Q, P, A)]`, it is the testing data which the groud truth result.

PredictionIO then combines the results from all engine instances, then send to
the `BaseEvaluator` via the `evaluateBase` method. Hence, `evaluateBase` is able
to *see* all query-predictedResult-actualResult tuples.

[BatchPersistablePrediction.scala](src/main/scala/BatchPersistablePrediction.scala) 
contains all the code change you need.




