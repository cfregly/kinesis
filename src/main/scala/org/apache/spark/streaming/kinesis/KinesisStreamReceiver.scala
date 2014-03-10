package org.apache.spark.streaming.kinesis

import com.amazonaws.services.kinesis.model.GetRecordsResult
import com.amazonaws.services.kinesis.model.GetShardIteratorResult
import com.amazonaws.services.kinesis.model.DescribeStreamResult
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.model.GetRecordsRequest
import org.apache.spark.streaming.dstream.NetworkReceiver
import org.apache.spark.storage.StorageLevel
import com.amazonaws.services.kinesis.model.DescribeStreamRequest
import com.amazonaws.services.kinesis.model.Record
import java.util.List
import java.util.ArrayList
import com.amazonaws.services.kinesis.model.Shard
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain

private[streaming]
class KinesisStreamReceiver(
    streamName : String,
    filters: Seq[String],
    storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
  ) extends NetworkReceiver[String] {

  // TODO:  figure out exactly what this lazy does here - and why it prevents the following
//Exception in thread "Thread-34" org.apache.spark.SparkException: Job aborted: Task not serializable: java.io.NotSerializableException: org.apache.spark.SparkEnv
//	at org.apache.spark.scheduler.DAGScheduler$$anonfun$org$apache$spark$scheduler$DAGScheduler$$abortStage$1.apply(DAGScheduler.scala:1028)
//	at org.apache.spark.scheduler.DAGScheduler$$anonfun$org$apache$spark$scheduler$DAGScheduler$$abortStage$1.apply(DAGScheduler.scala:1026)
//	at scala.collection.mutable.ResizableArray$class.foreach(ResizableArray.scala:59)
//	at scala.collection.mutable.ArrayBuffer.foreach(ArrayBuffer.scala:47)
//	at org.apache.spark.scheduler.DAGScheduler.org$apache$spark$scheduler$DAGScheduler$$abortStage(DAGScheduler.scala:1026)
//	at org.apache.spark.scheduler.DAGScheduler.org$apache$spark$scheduler$DAGScheduler$$submitMissingTasks(DAGScheduler.scala:794)
//	at org.apache.spark.scheduler.DAGScheduler.org$apache$spark$scheduler$DAGScheduler$$submitStage(DAGScheduler.scala:737)
//	at org.apache.spark.scheduler.DAGScheduler.processEvent(DAGScheduler.scala:569)
//	at org.apache.spark.scheduler.DAGScheduler$$anonfun$start$1$$anon$2$$anonfun$receive$1.applyOrElse(DAGScheduler.scala:207)

   lazy val blockGenerator : BlockGenerator = new BlockGenerator(storageLevel);
   lazy val kinesisStream : AmazonKinesisClient = new AmazonKinesisClient(new DefaultAWSCredentialsProviderChain());
   
   protected override def onStart() {
	   logInfo("STARTED!!")
	   		
	   blockGenerator.start()
	  
   		val describeStreamRequest : DescribeStreamRequest = new DescribeStreamRequest()
        describeStreamRequest.setStreamName( streamName )
        val shards : ArrayList[Shard] = new ArrayList[Shard]()
        var lastShardId : String = null
        var describeStreamResult : DescribeStreamResult = null
        do {
            describeStreamRequest.setExclusiveStartShardId( lastShardId )
            describeStreamResult = kinesisStream.describeStream( describeStreamRequest )
            shards.addAll( describeStreamResult.getStreamDescription().getShards() )
            if (shards.size() > 0) {
                lastShardId = shards.get(shards.size() - 1).getShardId()
            }
        } while ( describeStreamResult.getStreamDescription().getHasMoreShards() )
            
        var shardIterator : String = null
        val getShardIteratorRequest : GetShardIteratorRequest = new GetShardIteratorRequest()
        getShardIteratorRequest.setStreamName(streamName)
        getShardIteratorRequest.setShardId(lastShardId)
        getShardIteratorRequest.setShardIteratorType("TRIM_HORIZON")

        val getShardIteratorResult : GetShardIteratorResult = kinesisStream.getShardIterator(getShardIteratorRequest)
        shardIterator = getShardIteratorResult.getShardIterator()
	    
        var records : List[Record] = null

        while (true) {           
			//Create new GetRecordsRequest with existing shardIterator. 
			//Set maximum records to return to 1000.
			val getRecordsRequest : GetRecordsRequest = new GetRecordsRequest()
			getRecordsRequest.setShardIterator(shardIterator)
			getRecordsRequest.setLimit(1000) 
			
			val result : GetRecordsResult = kinesisStream.getRecords(getRecordsRequest)
			  
			//Put result into record list. Result may be empty.
			records = result.getRecords()
			System.out.println("RECORDS!" + records)
			if (records != null && !records.isEmpty()) {			  
				for (i <- 0 until records.size()) {
					 val line : String = new String(records.get(i).getData().array())
					 System.out.println("LINE!" + line)
					 blockGenerator += line
				}
			}
			
			shardIterator = result.getNextShardIterator();
        }	
	  
	  System.out.println("STARTED!!")
      logInfo("Kinesis receiver started")
  }

  protected override def onStop() {
    blockGenerator.stop()
    kinesisStream.shutdown()
    logInfo("Kinesis receiver stopped")
  }
  
  // AWS Kinesis Client Library (KCL)
  // This is unusable at the moment due to lack of Serialization throughout the library classes
  
  //var kinesisProxy: IKinesisProxy = new KinesisProxyFactory(new DefaultAWSCredentialsProviderChain(), "kinesis.us-east-1.amazonaws.com").getProxy("sparkStream")
  
//  var recordProcessorFactory: IRecordProcessorFactory = new IRecordProcessorFactory {
//    def createProcessor() : IRecordProcessor = {
//      new IRecordProcessor {
//        def initialize(shardId : String) {
//          //logInfo("Initialized shardId" + shardId)
//          System.out.println("Initialized shardId" + shardId)
//        }
//        
//        def processRecords(records: List[Record], checkpointer : IRecordProcessorCheckpointer) {
//          //logInfo("Record" + records)
//          System.out.println("Record" + records)
//        } 
//        
//        def shutdown(checkpointer : IRecordProcessorCheckpointer, reason : ShutdownReason) {
//          
//        }         
//      }
//    }
//  }
  
  //var config : KinesisClientLibConfiguration = new KinesisClientLibConfiguration("spark", "sparkStream", new DefaultAWSCredentialsProviderChain(), "spark")
  
  //var kinesisStream: Worker = new Worker(recordProcessorFactory, config)

  
}

//when expliciitly setting this as follows:
//
// 	  	val lines : DStream[String] = ssc.networkStream[String](new KinesisStreamReceiver(null))

//we see this...

//Exception in thread "Thread-34" org.apache.spark.SparkException: Job aborted: Task not serializable: java.io.NotSerializableException: org.apache.spark.SparkEnv
//	at org.apache.spark.scheduler.DAGScheduler$$anonfun$org$apache$spark$scheduler$DAGScheduler$$abortStage$1.apply(DAGScheduler.scala:1028)
//	at org.apache.spark.scheduler.DAGScheduler$$anonfun$org$apache$spark$scheduler$DAGScheduler$$abortStage$1.apply(DAGScheduler.scala:1026)
//	at scala.collection.mutable.ResizableArray$class.foreach(ResizableArray.scala:59)
//	at scala.collection.mutable.ArrayBuffer.foreach(ArrayBuffer.scala:47)
//	at org.apache.spark.scheduler.DAGScheduler.org$apache$spark$scheduler$DAGScheduler$$abortStage(DAGScheduler.scala:1026)
//	at org.apache.spark.scheduler.DAGScheduler.org$apache$spark$scheduler$DAGScheduler$$submitMissingTasks(DAGScheduler.scala:794)
//	at org.apache.spark.scheduler.DAGScheduler.org$apache$spark$scheduler$DAGScheduler$$submitStage(DAGScheduler.scala:737)
//	at org.apache.spark.scheduler.DAGScheduler.processEvent(DAGScheduler.scala:569)
//	at org.apache.spark.scheduler.DAGScheduler$$anonfun$start$1$$anon$2$$anonfun$receive$1.applyOrElse(DAGScheduler.scala:207)
//	at akka.actor.ActorCell.receiveMessage(ActorCell.scala:498)
//	at akka.actor.ActorCell.invoke(ActorCell.scala:456)
//	at akka.dispatch.Mailbox.processMailbox(Mailbox.scala:237)
//	at akka.dispatch.Mailbox.run(Mailbox.scala:219)
//	at akka.dispatch.ForkJoinExecutorConfigurator$AkkaForkJoinTask.exec(AbstractDispatcher.scala:386)
//	at scala.concurrent.forkjoin.ForkJoinTask.doExec(ForkJoinTask.java:260)
//	at scala.concurrent.forkjoin.ForkJoinPool$WorkQueue.runTask(ForkJoinPool.java:1339)
//	at scala.concurrent.forkjoin.ForkJoinPool.runWorker(ForkJoinPool.java:1979)
//	at scala.concurrent.forkjoin.ForkJoinWorkerThread.run(ForkJoinWorkerThread.java:107)