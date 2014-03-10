/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.streaming.examples

import java.util.ArrayList
import java.util.List
import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext.toPairDStreamFunctions
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.dstream.NetworkReceiver
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.model.DescribeStreamRequest
import com.amazonaws.services.kinesis.model.DescribeStreamResult
import com.amazonaws.services.kinesis.model.GetRecordsRequest
import com.amazonaws.services.kinesis.model.GetRecordsResult
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest
import com.amazonaws.services.kinesis.model.GetShardIteratorResult
import com.amazonaws.services.kinesis.model.Record
import com.amazonaws.services.kinesis.model.Shard
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker
import com.amazonaws.services.kinesis.AmazonKinesisClient

/**
 * Kinesis Word Count using the AWS Kinesis Client Library (KCL)
 */
object KinesisWordCountFregly extends Logging {
	/**
	 * args(0) - master URL
	 * args(1) - kinesis streamName
	 */
	def main(args: Array[String]) {
		// TODO:  input validation and usage instructions
	  
	    // create the StreamingContext with args(0) - master URL (or local[n] for local mode)
	  	//val ssc : StreamingContext = new StreamingContext(args(0), "KinesisWordCount", Seconds(10)
	  	    //System.getenv("SPARK_HOME")
	  	    //StreamingContext.jarOfClass(KinesisWordCountFregly.this.getClass)
	  	    //)
	  	//)

		val ssc = new StreamingContext(args(0), "KinesisWordCount", Seconds(10), System.getenv("SPARK_HOME"), Seq(System.getenv("SPARK_EXAMPLES")))
		
		
	  	// setup KinesisStreamReceiver with args(1) - streamName
	  	val stream : DStream[String] = ssc.networkStream[String](new KinesisStreamReceiver(args(1), null))	  	
	  	
	  	// split the stream into words and return the flatMap (collection)
	  	val words = stream.flatMap(_.split(" "))	  	

	  	// map each word to a (word, 1) tuple
	  	val wordCounts = words.map((_, 1))
	  		// reduce (aggregate) by work (key) to get the counts for each word over a rolling window of 10 seconds
	  		.reduceByKeyAndWindow(_ + _, Seconds(10))	
	  		.map{case (word, count) => (count, word)}	  		
	  	
	  	// output action - materializes the RDDs
	  	wordCounts.print()
	  	
	  	// start the StreamingContext
	  	ssc.start()

	  	// wait for execution to stop
	  	//   (exceptions will bubble up in this thread)
	  	ssc.awaitTermination()
  	}   
}

private[streaming]
/**
 * Implementation of NetworkReceiver using a Kinesis Client Library (versus low-level API)
 * 
 * streamName - name of kinesis stream
 * filters - any string filters to be applied (unused at the moment)
 * storageLevel - persistence mechanism (none, memory, disk, # of replicas)
 */
class KinesisStreamReceiver(
    streamName : String,
    filters: Seq[String],
    storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
  ) extends NetworkReceiver[String] {

    // note:  the following instance variables need to be marked lazy, otherwise a NotSerializedException will occur  

    // AWS Kinesis Client Library Configuration (KCL)
	lazy val config : KinesisClientLibConfiguration = new KinesisClientLibConfiguration("spark", streamName, new DefaultAWSCredentialsProviderChain(), "spark")
  
	lazy val recordProcessorFactory: IRecordProcessorFactory = new IRecordProcessorFactory {
	    def createProcessor() : IRecordProcessor = {
	      new IRecordProcessor {
	        def initialize(shardId : String) {
	          logInfo("Initialized shardId" + shardId)	          
	        }
	        
	        def processRecords(records: List[Record], checkpointer : IRecordProcessorCheckpointer) {
	        	logInfo("Records received from kinesis stream:" + records)
				if (records != null && !records.isEmpty()) {			  
					// extract the data from each record and hand off to blockGenerator
					for (i <- 0 until records.size()) {
						 val line : String = new String(records.get(i).getData().array())
						 logInfo("appending line to blockGenerator: " + line)
						 blockGenerator += line
					}
				}
	        } 
	        
	        def shutdown(checkpointer : IRecordProcessorCheckpointer, reason : ShutdownReason) {
	          logInfo("Shutdown: " + reason)
	        }         
	      }
	    }
	}
	
	// kinesis Worker - main facade for kinesis stream data retrieval
	lazy val kinesisStream: Worker = new Worker(recordProcessorFactory, config)
    // RDD block generator 
    lazy val blockGenerator : BlockGenerator = new BlockGenerator(storageLevel);   
  
    protected override def onStart() {
	   	logInfo("KinesisStreamReceiver starting...")
	   	kinesisStream.run();
	   	blockGenerator.start()
	   	logInfo("...KinesisStreamReceiver started")
    }

    protected override def onStop() {
	   	logInfo("KinesisStreamReceiver stopping...")	   		
	    kinesisStream.shutdown()
	    blockGenerator.stop()
	    logInfo("...KinesisStreamReceiver stopped")
    }
}
