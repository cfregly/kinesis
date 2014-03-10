///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// *
// *    http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package org.apache.spark.streaming.kinesis
//
//import java.util.ArrayList
//import java.util.List
//
//import org.apache.spark.storage.StorageLevel
//import org.apache.spark.streaming.dstream.NetworkInputDStream
//import org.apache.spark.streaming.dstream.NetworkReceiver
//
//import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
//import com.amazonaws.services.kinesis.AmazonKinesisClient
//import com.amazonaws.services.kinesis.model.DescribeStreamRequest
//import com.amazonaws.services.kinesis.model.DescribeStreamResult
//import com.amazonaws.services.kinesis.model.GetRecordsRequest
//import com.amazonaws.services.kinesis.model.GetRecordsResult
//import com.amazonaws.services.kinesis.model.GetShardIteratorRequest
//import com.amazonaws.services.kinesis.model.GetShardIteratorResult
//import com.amazonaws.services.kinesis.model.Record
//import com.amazonaws.services.kinesis.model.Shard
//
///* A stream of Kinesis data
//*/
//
//class KinesisReceiver(
//    filters: Seq[String],
//    storageLevel: StorageLevel
//  ) extends NetworkReceiver[String] {
//
//   val blockGenerator : BlockGenerator = new BlockGenerator(StorageLevel.MEMORY_ONLY);
//   val kinesisStream : AmazonKinesisClient = new AmazonKinesisClient(new DefaultAWSCredentialsProviderChain());
//   
//   protected override def onStart() {
//	  logInfo("STARTED!!")
//	
//	   val myStreamName = "sparkStream"
//   		
//	   blockGenerator.start()
//	  
//   		val describeStreamRequest : DescribeStreamRequest = new DescribeStreamRequest()
//        describeStreamRequest.setStreamName( myStreamName )
//        val shards : ArrayList[Shard] = new ArrayList[Shard]()
//        var lastShardId : String = null
//        var describeStreamResult : DescribeStreamResult = null
//        do {
//            describeStreamRequest.setExclusiveStartShardId( lastShardId )
//            describeStreamResult = kinesisStream.describeStream( describeStreamRequest )
//            shards.addAll( describeStreamResult.getStreamDescription().getShards() )
//            if (shards.size() > 0) {
//                lastShardId = shards.get(shards.size() - 1).getShardId()
//            }
//        } while ( describeStreamResult.getStreamDescription().getHasMoreShards() )
//            
//        var shardIterator : String = null
//        val getShardIteratorRequest : GetShardIteratorRequest = new GetShardIteratorRequest()
//        getShardIteratorRequest.setStreamName(myStreamName)
//        getShardIteratorRequest.setShardId(lastShardId)
//        getShardIteratorRequest.setShardIteratorType("TRIM_HORIZON")
//
//        val getShardIteratorResult : GetShardIteratorResult = kinesisStream.getShardIterator(getShardIteratorRequest)
//        shardIterator = getShardIteratorResult.getShardIterator()
//	    
//        var records : List[Record] = null
//
//        while (true) {           
//			//Create new GetRecordsRequest with existing shardIterator. 
//			//Set maximum records to return to 1000.
//			val getRecordsRequest : GetRecordsRequest = new GetRecordsRequest()
//			getRecordsRequest.setShardIterator(shardIterator)
//			getRecordsRequest.setLimit(1000) 
//			
//			val result : GetRecordsResult = kinesisStream.getRecords(getRecordsRequest)
//			  
//			//Put result into record list. Result may be empty.
//			records = result.getRecords()
//			System.out.println("RECORDS!" + records)
//			if (records != null && !records.isEmpty()) {			  
//				for (i <- 0 until records.size()) {
//					 val line : String = new String(records.get(i).getData().array())
//					 System.out.println("LINE!" + line)
//					 blockGenerator += line
//				}
//			}
//			
//			shardIterator = result.getNextShardIterator();
//        }	
//	  
//	  System.out.println("STARTED!!")
//      logInfo("Kinesis receiver started")
//  }
//
//  protected override def onStop() {
//    blockGenerator.stop()
//    kinesisStream.shutdown()
//    logInfo("Kinesis receiver stopped")
//  }
//  
//  // AWS Kinesis Client Library (KCL)
//  // This is unusable at the moment due to lack of Serialization throughout the library classes
//  
//  //var kinesisProxy: IKinesisProxy = new KinesisProxyFactory(new DefaultAWSCredentialsProviderChain(), "kinesis.us-east-1.amazonaws.com").getProxy("sparkStream")
//  
////  var recordProcessorFactory: IRecordProcessorFactory = new IRecordProcessorFactory {
////    def createProcessor() : IRecordProcessor = {
////      new IRecordProcessor {
////        def initialize(shardId : String) {
////          //logInfo("Initialized shardId" + shardId)
////          System.out.println("Initialized shardId" + shardId)
////        }
////        
////        def processRecords(records: List[Record], checkpointer : IRecordProcessorCheckpointer) {
////          //logInfo("Record" + records)
////          System.out.println("Record" + records)
////        } 
////        
////        def shutdown(checkpointer : IRecordProcessorCheckpointer, reason : ShutdownReason) {
////          
////        }         
////      }
////    }
////  }
//  
//  //var config : KinesisClientLibConfiguration = new KinesisClientLibConfiguration("spark", "sparkStream", new DefaultAWSCredentialsProviderChain(), "spark")
//  
//  //var kinesisStream: Worker = new Worker(recordProcessorFactory, config)
//
//  
//}