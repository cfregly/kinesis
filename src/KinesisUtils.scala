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
//import org.apache.spark.storage.StorageLevel
//import org.apache.spark.streaming.StreamingContext
//import org.apache.spark.streaming.api.java.JavaDStream
//import org.apache.spark.streaming.api.java.JavaStreamingContext
//import org.apache.spark.streaming.dstream.DStream
//import com.amazonaws.services.kinesis.model.DescribeStreamRequest
//import com.amazonaws.services.kinesis.model.GetRecordsResult
//import com.amazonaws.services.kinesis.model.GetShardIteratorResult
//import com.amazonaws.services.kinesis.model.DescribeStreamResult
//import com.amazonaws.services.kinesis.model.GetShardIteratorRequest
//import com.amazonaws.services.kinesis.model.GetRecordsRequest
//import java.util.ArrayList
//import com.amazonaws.services.kinesis.model.Shard
//import com.amazonaws.services.kinesis.model.Record
//import com.amazonaws.services.kinesis.AmazonKinesisClient
//import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
//import scala.collection.immutable.List
//import org.apache.spark.streaming.dstream.NetworkInputDStream
//
//
//object KinesisUtils {
//  /**
//   * @param ssc          StreamingContext object
//   * @param filters 	 Set of filter strings 
//   * @param storageLevel Storage level to use for storing the received objects
//   */
//  def createStream(
//      ssc: StreamingContext,
//      filters: Seq[String] = Nil,
//      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
//    ): NetworkInputDStream[String] = {
//    new KinesisInputDStream(ssc, filters, storageLevel)
//  }
//  
////  def ghetto() = {
////   		val myStreamName = "sparkStream"
////   		val blockGenerator : BlockGenerator = new BlockGenerator(StorageLevel.MEMORY_ONLY);
////   		val kinesisClient : AmazonKinesisClient = new AmazonKinesisClient(new DefaultAWSCredentialsProviderChain());
////        
////   		val describeStreamRequest : DescribeStreamRequest = new DescribeStreamRequest()
////        describeStreamRequest.setStreamName( myStreamName )
////        val shards : ArrayList[Shard] = new ArrayList[Shard]()
////        var lastShardId : String = null
////        var describeStreamResult : DescribeStreamResult = null
////        do {
////            describeStreamRequest.setExclusiveStartShardId( lastShardId )
////            describeStreamResult = kinesisClient.describeStream( describeStreamRequest )
////            shards.addAll( describeStreamResult.getStreamDescription().getShards() )
////            if (shards.size() > 0) {
////                lastShardId = shards.get(shards.size() - 1).getShardId()
////            }
////        } while ( describeStreamResult.getStreamDescription().getHasMoreShards() )
////            
////        var shardIterator : String = null
////        val getShardIteratorRequest : GetShardIteratorRequest = new GetShardIteratorRequest()
////        getShardIteratorRequest.setStreamName(myStreamName)
////        getShardIteratorRequest.setShardId(lastShardId)
////        getShardIteratorRequest.setShardIteratorType("TRIM_HORIZON")
////
////        val getShardIteratorResult : GetShardIteratorResult = kinesisClient.getShardIterator(getShardIteratorRequest)
////        shardIterator = getShardIteratorResult.getShardIterator()
////	    
////        var records : scala.List[Record] = null
////
////        while (true) {           
////			//Create new GetRecordsRequest with existing shardIterator. 
////			//Set maximum records to return to 1000.
////			val getRecordsRequest : GetRecordsRequest = new GetRecordsRequest()
////			getRecordsRequest.setShardIterator(shardIterator)
////			getRecordsRequest.setLimit(1000) 
////			
////			val result : GetRecordsResult = kinesisClient.getRecords(getRecordsRequest)
////			  
////			//Put result into record list. Result may be empty.
////			records = result.getRecords()
////			if (records != null && !records.isEmpty()) {
////				blockGenerator.add(wordCounts)
////			}
////        }	    	   
//	    
//	    //logInfo("Kinesis receiver started")
//}