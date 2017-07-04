Spark application 支持自定义listener，用户可以实时获取任务状态给自己的监控系统，可以获取以下几个状态：

```
trait SparkListener {
  /**
    * 当一个state执行成功或者失败的时候调用，包含了已完成stage的信息
   * Called when a stage completes successfully or fails, with information on the completed stage.
   */
  def onStageCompleted(stageCompleted: SparkListenerStageCompleted) { }

  /**
    * 当一个state提交的时候的时候调用
   * Called when a stage is submitted
   */
  def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted) { }

  /**
    * 当一个task任务开始时候调用
   * Called when a task starts
   */
  def onTaskStart(taskStart: SparkListenerTaskStart) { }

  /**
    * 当一个task执行成功或者失败的时候调用，包含了已完成task的信息
   * Called when a task begins remotely fetching its result (will not be called for tasks that do
   * not need to fetch the result remotely).
   */
  def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult) { }

  /**
    * 当一个task结束开始时候调用
   * Called when a task ends
   */
  def onTaskEnd(taskEnd: SparkListenerTaskEnd) { }

  /**
    * 当一个job启动开始调用
   * Called when a job starts
   */
  def onJobStart(jobStart: SparkListenerJobStart) { }

  /**
    *  当一个job执行成功或者失败的时候调用，包含了已完成job的信息
   * Called when a job ends
   */
  def onJobEnd(jobEnd: SparkListenerJobEnd) { }

  /**
    * 当一个环境变量改变的时候开始调用
   * Called when environment properties have been updated
   */
  def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate) { }

  /**
   * Called when a new block manager has joined
   */
  def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded) { }

  /**
   * Called when an existing block manager has been removed
   */
  def onBlockManagerRemoved(blockManagerRemoved: SparkListenerBlockManagerRemoved) { }

  /**
   * Called when an RDD is manually unpersisted by the application
   */
  def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD) { }

  /**
   * Called when the application starts
   */
  def onApplicationStart(applicationStart: SparkListenerApplicationStart) { }

  /**
   * Called when the application ends
   */
  def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) { }

  /**
   * Called when the driver receives task metrics from an executor in a heartbeat.
   */
  def onExecutorMetricsUpdate(executorMetricsUpdate: SparkListenerExecutorMetricsUpdate) { }

  /**
   * Called when the driver registers a new executor.
   */
  def onExecutorAdded(executorAdded: SparkListenerExecutorAdded) { }

  /**
   * Called when the driver removes an executor.
   */
  def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved) { }

  /**
    * 
   * Called when the driver receives a block update info.
   */
  
  def onBlockUpdated(blockUpdated: SparkListenerBlockUpdated) { }
}

```

只需要extends SparkListener，然后注册到sparkContext 既可以实现自定义listener，代码逻辑如下：

```
package com.suning.spark

import org.apache.spark.scheduler.MySparkListener
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Ricky on 2016/4/14 0014.
  */
object JobProcesser {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("KafkaWordCountProducer").setMaster("local")
    val sc = new SparkContext(sparkConf)
    /*  sc.setJobGroup("test1","testdesc")
      val completedJobs= sc.jobProgressListener*/
    sc.addSparkListener(new MySparkListener)
    val rdd1 = sc.parallelize(List(('a', 'c', 1), ('b', 'a', 1), ('b', 'd', 8)))
    val rdd2 = sc.parallelize(List(('a', 'c', 2), ('b', 'c', 5), ('b', 'd', 6)))
    val rdd3 = rdd1.union(rdd2).map {
      x =
>
 {
        Thread.sleep(500)
        x
      }
    }.count()
    rdd1.map(x =
>
 0.2).map(x =
>
 0).map {
      x =
>
 {
        if (x == 0) {
          throw new Exception("my exeception")
        }
      }
        x
    }.reduce(_ + _)
    println(rdd3)
    sc.stop()
  }
}
```

```
package org.apache.spark.scheduler


/**
  * Created by Ricky on 2016/4/14 0014.
  */
class MySparkListener extends SparkListener {
  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) {
    println("*************************************************")
    println("app:end")
    println("*************************************************")
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd) {
    println("*************************************************")
    println("job:end")
    jobEnd.jobResult match {
      case JobSucceeded =>
        println("job:end:JobSucceeded")
      case JobFailed(exception) =>
        println("job:end:file")
        exception.printStackTrace()
    }
    println("*************************************************")
  }
}

```

执行日志：

```
16/04/15 11:12:29 INFO Remoting: Remoting started; listening on addresses :[akka.tcp://sparkDriver@10.24.74.95:52662]
16/04/15 11:12:29 INFO Utils: Successfully started service 'sparkDriver' on port 52662.
16/04/15 11:12:29 INFO SparkEnv: Registering MapOutputTracker
16/04/15 11:12:29 INFO SparkEnv: Registering BlockManagerMaster
16/04/15 11:12:29 INFO DiskBlockManager: Created local directory at C:\Users\14070345\AppData\Local\Temp\blockmgr-5573ab97-dea7-45e5-ae99-01fc689ae0e4
16/04/15 11:12:29 INFO MemoryStore: MemoryStore started with capacity 950.4 MB
16/04/15 11:12:29 INFO HttpFileServer: HTTP File server directory is C:\Users\14070345\AppData\Local\Temp\spark-6c67028e-c0e3-412d-8eab-31962a1b0cbe\httpd-a348c669-8194-41d1-9be2-2921b4a3da56
16/04/15 11:12:29 INFO HttpServer: Starting HTTP Server
16/04/15 11:12:29 INFO Utils: Successfully started service 'HTTP file server' on port 52663.
16/04/15 11:12:29 INFO SparkEnv: Registering OutputCommitCoordinator
16/04/15 11:12:29 INFO Utils: Successfully started service 'SparkUI' on port 4040.
16/04/15 11:12:29 INFO SparkUI: Started SparkUI at http://10.24.74.95:4040
16/04/15 11:12:30 WARN MetricsSystem: Using default name DAGScheduler for source because spark.app.id is not set.
16/04/15 11:12:30 INFO Executor: Starting executor ID driver on host localhost
16/04/15 11:12:30 INFO Utils: Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' on port 52670.
16/04/15 11:12:30 INFO NettyBlockTransferService: Server created on 52670
16/04/15 11:12:30 INFO BlockManagerMaster: Trying to register BlockManager
16/04/15 11:12:30 INFO BlockManagerMasterEndpoint: Registering block manager localhost:52670 with 950.4 MB RAM, BlockManagerId(driver, localhost, 52670)
16/04/15 11:12:30 INFO BlockManagerMaster: Registered BlockManager
16/04/15 11:12:30 INFO SparkContext: Starting job: count at JobProcesser.scala:25
16/04/15 11:12:30 INFO DAGScheduler: Got job 0 (count at JobProcesser.scala:25) with 2 output partitions
16/04/15 11:12:30 INFO DAGScheduler: Final stage: ResultStage 0(count at JobProcesser.scala:25)
16/04/15 11:12:30 INFO DAGScheduler: Parents of final stage: List()
16/04/15 11:12:30 INFO DAGScheduler: Missing parents: List()
16/04/15 11:12:30 INFO DAGScheduler: Submitting ResultStage 0 (MapPartitionsRDD[3] at map at JobProcesser.scala:20), which has no missing parents
16/04/15 11:12:31 INFO MemoryStore: ensureFreeSpace(2400) called with curMem=0, maxMem=996566630
16/04/15 11:12:31 INFO MemoryStore: Block broadcast_0 stored as values in memory (estimated size 2.3 KB, free 950.4 MB)
16/04/15 11:12:31 INFO MemoryStore: ensureFreeSpace(1546) called with curMem=2400, maxMem=996566630
16/04/15 11:12:31 INFO MemoryStore: Block broadcast_0_piece0 stored as bytes in memory (estimated size 1546.0 B, free 950.4 MB)
16/04/15 11:12:31 INFO BlockManagerInfo: Added broadcast_0_piece0 in memory on localhost:52670 (size: 1546.0 B, free: 950.4 MB)
16/04/15 11:12:31 INFO SparkContext: Created broadcast 0 from broadcast at DAGScheduler.scala:861
16/04/15 11:12:31 INFO DAGScheduler: Submitting 2 missing tasks from ResultStage 0 (MapPartitionsRDD[3] at map at JobProcesser.scala:20)
16/04/15 11:12:31 INFO TaskSchedulerImpl: Adding task set 0.0 with 2 tasks
16/04/15 11:12:31 INFO TaskSetManager: Starting task 0.0 in stage 0.0 (TID 0, localhost, PROCESS_LOCAL, 2408 bytes)
16/04/15 11:12:31 INFO Executor: Running task 0.0 in stage 0.0 (TID 0)
16/04/15 11:12:37 INFO Executor: Finished task 0.0 in stage 0.0 (TID 0). 953 bytes result sent to driver
16/04/15 11:12:37 INFO TaskSetManager: Starting task 1.0 in stage 0.0 (TID 1, localhost, PROCESS_LOCAL, 2413 bytes)
16/04/15 11:12:37 INFO Executor: Running task 1.0 in stage 0.0 (TID 1)
16/04/15 11:12:37 INFO TaskSetManager: Finished task 0.0 in stage 0.0 (TID 0) in 6046 ms on localhost (1/2)
16/04/15 11:12:43 INFO Executor: Finished task 1.0 in stage 0.0 (TID 1). 953 bytes result sent to driver
16/04/15 11:12:43 INFO TaskSetManager: Finished task 1.0 in stage 0.0 (TID 1) in 6012 ms on localhost (2/2)
16/04/15 11:12:43 INFO TaskSchedulerImpl: Removed TaskSet 0.0, whose tasks have all completed, from pool 
16/04/15 11:12:43 INFO DAGScheduler: ResultStage 0 (count at JobProcesser.scala:25) finished in 12.066 s
16/04/15 11:12:43 INFO DAGScheduler: Job 0 finished: count at JobProcesser.scala:25, took 12.731024 s
*************************************************
job:end
job:end:JobSucceeded
*************************************************
16/04/15 11:12:43 INFO SparkContext: Starting job: reduce at JobProcesser.scala:33
16/04/15 11:12:43 INFO DAGScheduler: Got job 1 (reduce at JobProcesser.scala:33) with 1 output partitions
16/04/15 11:12:43 INFO DAGScheduler: Final stage: ResultStage 1(reduce at JobProcesser.scala:33)
16/04/15 11:12:43 INFO DAGScheduler: Parents of final stage: List()
16/04/15 11:12:43 INFO DAGScheduler: Missing parents: List()
16/04/15 11:12:43 INFO DAGScheduler: Submitting ResultStage 1 (MapPartitionsRDD[6] at map at JobProcesser.scala:26), which has no missing parents
16/04/15 11:12:43 INFO MemoryStore: ensureFreeSpace(2336) called with curMem=3946, maxMem=996566630
16/04/15 11:12:43 INFO MemoryStore: Block broadcast_1 stored as values in memory (estimated size 2.3 KB, free 950.4 MB)
16/04/15 11:12:43 INFO MemoryStore: ensureFreeSpace(1410) called with curMem=6282, maxMem=996566630
16/04/15 11:12:43 INFO MemoryStore: Block broadcast_1_piece0 stored as bytes in memory (estimated size 1410.0 B, free 950.4 MB)
16/04/15 11:12:43 INFO BlockManagerInfo: Added broadcast_1_piece0 in memory on localhost:52670 (size: 1410.0 B, free: 950.4 MB)
16/04/15 11:12:43 INFO SparkContext: Created broadcast 1 from broadcast at DAGScheduler.scala:861
16/04/15 11:12:43 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 1 (MapPartitionsRDD[6] at map at JobProcesser.scala:26)
16/04/15 11:12:43 INFO TaskSchedulerImpl: Adding task set 1.0 with 1 tasks
16/04/15 11:12:43 INFO TaskSetManager: Starting task 0.0 in stage 1.0 (TID 2, localhost, PROCESS_LOCAL, 2299 bytes)
16/04/15 11:12:43 INFO Executor: Running task 0.0 in stage 1.0 (TID 2)
16/04/15 11:12:43 ERROR Executor: Exception in task 0.0 in stage 1.0 (TID 2)
java.lang.Exception: my exeception
	at com.suning.spark.JobProcesser$$anonfun$main$2.apply$mcII$sp(JobProcesser.scala:29)
	at com.suning.spark.JobProcesser$$anonfun$main$2.apply(JobProcesser.scala:27)
	at com.suning.spark.JobProcesser$$anonfun$main$2.apply(JobProcesser.scala:27)
	at scala.collection.Iterator$$anon$11.next(Iterator.scala:328)
	at scala.collection.Iterator$class.foreach(Iterator.scala:727)
	at scala.collection.AbstractIterator.foreach(Iterator.scala:1157)
	at scala.collection.TraversableOnce$class.reduceLeft(TraversableOnce.scala:172)
	at scala.collection.AbstractIterator.reduceLeft(Iterator.scala:1157)
	at org.apache.spark.rdd.RDD$$anonfun$reduce$1$$anonfun$14.apply(RDD.scala:993)
	at org.apache.spark.rdd.RDD$$anonfun$reduce$1$$anonfun$14.apply(RDD.scala:991)
	at org.apache.spark.SparkContext$$anonfun$37.apply(SparkContext.scala:1960)
	at org.apache.spark.SparkContext$$anonfun$37.apply(SparkContext.scala:1960)
	at org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:66)
	at org.apache.spark.scheduler.Task.run(Task.scala:88)
	at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:214)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1145)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:615)
	at java.lang.Thread.run(Thread.java:745)
16/04/15 11:12:43 WARN TaskSetManager: Lost task 0.0 in stage 1.0 (TID 2, localhost): java.lang.Exception: my exeception
	at com.suning.spark.JobProcesser$$anonfun$main$2.apply$mcII$sp(JobProcesser.scala:29)
	at com.suning.spark.JobProcesser$$anonfun$main$2.apply(JobProcesser.scala:27)
	at com.suning.spark.JobProcesser$$anonfun$main$2.apply(JobProcesser.scala:27)
	at scala.collection.Iterator$$anon$11.next(Iterator.scala:328)
	at scala.collection.Iterator$class.foreach(Iterator.scala:727)
	at scala.collection.AbstractIterator.foreach(Iterator.scala:1157)
	at scala.collection.TraversableOnce$class.reduceLeft(TraversableOnce.scala:172)
	at scala.collection.AbstractIterator.reduceLeft(Iterator.scala:1157)
	at org.apache.spark.rdd.RDD$$anonfun$reduce$1$$anonfun$14.apply(RDD.scala:993)
	at org.apache.spark.rdd.RDD$$anonfun$reduce$1$$anonfun$14.apply(RDD.scala:991)
	at org.apache.spark.SparkContext$$anonfun$37.apply(SparkContext.scala:1960)
	at org.apache.spark.SparkContext$$anonfun$37.apply(SparkContext.scala:1960)
	at org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:66)
	at org.apache.spark.scheduler.Task.run(Task.scala:88)
	at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:214)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1145)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:615)
	at java.lang.Thread.run(Thread.java:745)

16/04/15 11:12:43 ERROR TaskSetManager: Task 0 in stage 1.0 failed 1 times; aborting job
16/04/15 11:12:43 INFO TaskSchedulerImpl: Removed TaskSet 1.0, whose tasks have all completed, from pool 
16/04/15 11:12:43 INFO TaskSchedulerImpl: Cancelling stage 1
16/04/15 11:12:43 INFO DAGScheduler: ResultStage 1 (reduce at JobProcesser.scala:33) failed in 0.068 s
org.apache.spark.SparkException: Job aborted due to stage failure: Task 0 in stage 1.0 failed 1 times, most recent failure: Lost task 0.0 in stage 1.0 (TID 2, localhost): java.lang.Exception: my exeception
	at com.suning.spark.JobProcesser$$anonfun$main$2.apply$mcII$sp(JobProcesser.scala:29)
	at com.suning.spark.JobProcesser$$anonfun$main$2.apply(JobProcesser.scala:27)
	at com.suning.spark.JobProcesser$$anonfun$main$2.apply(JobProcesser.scala:27)
	at scala.collection.Iterator$$anon$11.next(Iterator.scala:328)
	at scala.collection.Iterator$class.foreach(Iterator.scala:727)
	at scala.collection.AbstractIterator.foreach(Iterator.scala:1157)
	at scala.collection.TraversableOnce$class.reduceLeft(TraversableOnce.scala:172)
	at scala.collection.AbstractIterator.reduceLeft(Iterator.scala:1157)
	at org.apache.spark.rdd.RDD$$anonfun$reduce$1$$anonfun$14.apply(RDD.scala:993)
	at org.apache.spark.rdd.RDD$$anonfun$reduce$1$$anonfun$14.apply(RDD.scala:991)
	at org.apache.spark.SparkContext$$anonfun$37.apply(SparkContext.scala:1960)
	at org.apache.spark.SparkContext$$anonfun$37.apply(SparkContext.scala:1960)
	at org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:66)
	at org.apache.spark.scheduler.Task.run(Task.scala:88)
	at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:214)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1145)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:615)
	at java.lang.Thread.run(Thread.java:745)

Driver stacktrace:
	at org.apache.spark.scheduler.DAGScheduler.org$apache$spark$scheduler$DAGScheduler$$failJobAndIndependentStages(DAGScheduler.scala:1283)
	at org.apache.spark.scheduler.DAGScheduler$$anonfun$abortStage$1.apply(DAGScheduler.scala:1271)
	at org.apache.spark.scheduler.DAGScheduler$$anonfun$abortStage$1.apply(DAGScheduler.scala:1270)
	at scala.collection.mutable.ResizableArray$class.foreach(ResizableArray.scala:59)
	at scala.collection.mutable.ArrayBuffer.foreach(ArrayBuffer.scala:47)
	at org.apache.spark.scheduler.DAGScheduler.abortStage(DAGScheduler.scala:1270)
	at org.apache.spark.scheduler.DAGScheduler$$anonfun$handleTaskSetFailed$1.apply(DAGScheduler.scala:697)
	at org.apache.spark.scheduler.DAGScheduler$$anonfun$handleTaskSetFailed$1.apply(DAGScheduler.scala:697)
	at scala.Option.foreach(Option.scala:236)
	at org.apache.spark.scheduler.DAGScheduler.handleTaskSetFailed(DAGScheduler.scala:697)
	at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.doOnReceive(DAGScheduler.scala:1496)
	at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.onReceive(DAGScheduler.scala:1458)
	at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.onReceive(DAGScheduler.scala:1447)
	at org.apache.spark.util.EventLoop$$anon$1.run(EventLoop.scala:48)
Caused by: java.lang.Exception: my exeception
	at com.suning.spark.JobProcesser$$anonfun$main$2.apply$mcII$sp(JobProcesser.scala:29)
	at com.suning.spark.JobProcesser$$anonfun$main$2.apply(JobProcesser.scala:27)
	at com.suning.spark.JobProcesser$$anonfun$main$2.apply(JobProcesser.scala:27)
	at scala.collection.Iterator$$anon$11.next(Iterator.scala:328)
	at scala.collection.Iterator$class.foreach(Iterator.scala:727)
	at scala.collection.AbstractIterator.foreach(Iterator.scala:1157)
	at scala.collection.TraversableOnce$class.reduceLeft(TraversableOnce.scala:172)
	at scala.collection.AbstractIterator.reduceLeft(Iterator.scala:1157)
	at org.apache.spark.rdd.RDD$$anonfun$reduce$1$$anonfun$14.apply(RDD.scala:993)
	at org.apache.spark.rdd.RDD$$anonfun$reduce$1$$anonfun$14.apply(RDD.scala:991)
	at org.apache.spark.SparkContext$$anonfun$37.apply(SparkContext.scala:1960)
	at org.apache.spark.SparkContext$$anonfun$37.apply(SparkContext.scala:1960)
	at org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:66)
	at org.apache.spark.scheduler.Task.run(Task.scala:88)
	at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:214)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1145)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:615)
	at java.lang.Thread.run(Thread.java:745)
16/04/15 11:12:43 INFO DAGScheduler: Job 1 failed: reduce at JobProcesser.scala:33, took 0.103760 s
Exception in thread "main" org.apache.spark.SparkException: Job aborted due to stage failure: Task 0 in stage 1.0 failed 1 times, most recent failure: Lost task 0.0 in stage 1.0 (TID 2, localhost): java.lang.Exception: my exeception
	at com.suning.spark.JobProcesser$$anonfun$main$2.apply$mcII$sp(JobProcesser.scala:29)
	at com.suning.spark.JobProcesser$$anonfun$main$2.apply(JobProcesser.scala:27)
	at com.suning.spark.JobProcesser$$anonfun$main$2.apply(JobProcesser.scala:27)
	at scala.collection.Iterator$$anon$11.next(Iterator.scala:328)
	at scala.collection.Iterator$class.foreach(Iterator.scala:727)
	at scala.collection.AbstractIterator.foreach(Iterator.scala:1157)
	at scala.collection.TraversableOnce$class.reduceLeft(TraversableOnce.scala:172)
	at scala.collection.AbstractIterator.reduceLeft(Iterator.scala:1157)
	at org.apache.spark.rdd.RDD$$anonfun$reduce$1$$anonfun$14.apply(RDD.scala:993)
	at org.apache.spark.rdd.RDD$$anonfun$reduce$1$$anonfun$14.apply(RDD.scala:991)
	at org.apache.spark.SparkContext$$anonfun$37.apply(SparkContext.scala:1960)
	at org.apache.spark.SparkContext$$anonfun$37.apply(SparkContext.scala:1960)
	at org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:66)
	at org.apache.spark.scheduler.Task.run(Task.scala:88)
	at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:214)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1145)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:615)
	at java.lang.Thread.run(Thread.java:745)

Driver stacktrace:
	at org.apache.spark.scheduler.DAGScheduler.org$apache$spark$scheduler$DAGScheduler$$failJobAndIndependentStages(DAGScheduler.scala:1283)
	at org.apache.spark.scheduler.DAGScheduler$$anonfun$abortStage$1.apply(DAGScheduler.scala:1271)
	at org.apache.spark.scheduler.DAGScheduler$$anonfun$abortStage$1.apply(DAGScheduler.scala:1270)
	at scala.collection.mutable.ResizableArray$class.foreach(ResizableArray.scala:59)
	at scala.collection.mutable.ArrayBuffer.foreach(ArrayBuffer.scala:47)
	at org.apache.spark.scheduler.DAGScheduler.abortStage(DAGScheduler.scala:1270)
	at org.apache.spark.scheduler.DAGScheduler$$anonfun$handleTaskSetFailed$1.apply(DAGScheduler.scala:697)
	at org.apache.spark.scheduler.DAGScheduler$$anonfun$handleTaskSetFailed$1.apply(DAGScheduler.scala:697)
	at scala.Option.foreach(Option.scala:236)
	at org.apache.spark.scheduler.DAGScheduler.handleTaskSetFailed(DAGScheduler.scala:697)
	at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.doOnReceive(DAGScheduler.scala:1496)
	at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.onReceive(DAGScheduler.scala:1458)
	at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.onReceive(DAGScheduler.scala:1447)
	at org.apache.spark.util.EventLoop$$anon$1.run(EventLoop.scala:48)
	at org.apache.spark.scheduler.DAGScheduler.runJob(DAGScheduler.scala:567)
	at org.apache.spark.SparkContext.runJob(SparkContext.scala:1841)
	at org.apache.spark.SparkContext.runJob(SparkContext.scala:1961)
	at org.apache.spark.rdd.RDD$$anonfun$reduce$1.apply(RDD.scala:1007)
	at org.apache.spark.rdd.RDDOperationScope$.withScope(RDDOperationScope.scala:147)
	at org.apache.spark.rdd.RDDOperationScope$.withScope(RDDOperationScope.scala:108)
	at org.apache.spark.rdd.RDD.withScope(RDD.scala:310)
	at org.apache.spark.rdd.RDD.reduce(RDD.scala:989)
	at com.suning.spark.JobProcesser$.main(JobProcesser.scala:33)
	at com.suning.spark.JobProcesser.main(JobProcesser.scala)
	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:57)
	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
*************************************************
	at java.lang.reflect.Method.invoke(Method.java:606)
job:end
job:end:file
	at com.intellij.rt.execution.application.AppMain.main(AppMain.java:144)
Caused by: java.lang.Exception: my exeception
*************************************************
	at com.suning.spark.JobProcesser$$anonfun$main$2.apply$mcII$sp(JobProcesser.scala:29)
	at com.suning.spark.JobProcesser$$anonfun$main$2.apply(JobProcesser.scala:27)
	at com.suning.spark.JobProcesser$$anonfun$main$2.apply(JobProcesser.scala:27)
	at scala.collection.Iterator$$anon$11.next(Iterator.scala:328)
	at scala.collection.Iterator$class.foreach(Iterator.scala:727)
	at scala.collection.AbstractIterator.foreach(Iterator.scala:1157)
	at scala.collection.TraversableOnce$class.reduceLeft(TraversableOnce.scala:172)
	at scala.collection.AbstractIterator.reduceLeft(Iterator.scala:1157)
	at org.apache.spark.rdd.RDD$$anonfun$reduce$1$$anonfun$14.apply(RDD.scala:993)
	at org.apache.spark.rdd.RDD$$anonfun$reduce$1$$anonfun$14.apply(RDD.scala:991)
	at org.apache.spark.SparkContext$$anonfun$37.apply(SparkContext.scala:1960)
	at org.apache.spark.SparkContext$$anonfun$37.apply(SparkContext.scala:1960)
	at org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:66)
	at org.apache.spark.scheduler.Task.run(Task.scala:88)
	at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:214)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1145)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:615)
	at java.lang.Thread.run(Thread.java:745)
*************************************************
16/04/15 11:12:43 INFO SparkContext: Invoking stop() from shutdown hook
app:end
*************************************************
16/04/15 11:12:43 INFO SparkUI: Stopped Spark web UI at http://10.24.74.95:4040
16/04/15 11:12:43 INFO DAGScheduler: Stopping DAGScheduler
16/04/15 11:12:43 INFO MapOutputTrackerMasterEndpoint: MapOutputTrackerMasterEndpoint stopped!
16/04/15 11:12:43 INFO MemoryStore: MemoryStore cleared
16/04/15 11:12:43 INFO BlockManager: BlockManager stopped
16/04/15 11:12:43 INFO BlockManagerMaster: BlockManagerMaster stopped
16/04/15 11:12:43 INFO SparkContext: Successfully stopped SparkContext
16/04/15 11:12:43 INFO OutputCommitCoordinator$OutputCommitCoordinatorEndpoint: OutputCommitCoordinator stopped!
16/04/15 11:12:43 INFO ShutdownHookManager: Shutdown hook called
16/04/15 11:12:43 INFO ShutdownHookManager: Deleting directory C:\Users\14070345\AppData\Local\Temp\spark-6c67028e-c0e3-412d-8eab-31962a1b0cbe

Process finished with exit code 1
```



