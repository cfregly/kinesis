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

package org.apache.spark.streaming.kinesis

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.NetworkInputDStream
import org.apache.spark.streaming.dstream.NetworkReceiver
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration
import com.amazonaws.services.kinesis.clientlibrary.proxies.IKinesisProxy
import com.amazonaws.services.kinesis.clientlibrary.proxies.KinesisProxyFactory
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason
import com.amazonaws.services.kinesis.model.Record
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker


/* A stream of Kinesis data
*/

private[streaming]
class KinesisInputDStream(
    @transient ssc_ : StreamingContext,
    filters: Seq[String],
    storageLevel: StorageLevel
  ) extends NetworkInputDStream[String](ssc_)  {
  
  
  override def getReceiver(): NetworkReceiver[Any] = {
    new KinesisReceiver(filters, storageLevel)
  }
}

private[streaming]
class KinesisReceiver(
    filters: Seq[String],
    storageLevel: StorageLevel
  ) extends NetworkReceiver[Any] {

  var kinesisProxy: IKinesisProxy = new KinesisProxyFactory(new DefaultAWSCredentialsProviderChain(), "kinesis.us-east-1.amazonaws.com").getProxy("sparkStream")
  
  var recordProcessorFactory: IRecordProcessorFactory = new IRecordProcessorFactory {
    def createProcessor() : IRecordProcessor = {
      return new IRecordProcessor {
        def initialize(shardId : String) {
          logInfo("Initialized shardId" + shardId)
        }
        
        def processRecords(records: Seq[Record], checkpointer : IRecordProcessorCheckpointer) {
          logInfo("Record" + records)
        } 
        
        def shutdown(checkpointer : IRecordProcessorCheckpointer, reason : ShutdownReason) {
          
        }         
      }
    }
  }
  
  var config : KinesisClientLibConfiguration = new KinesisClientLibConfiguration("spark", "sparkStream", new DefaultAWSCredentialsProviderChain(), "spark")
  
  var kinesisStream: Worker = new Worker(recordProcessorFactory, config)
  lazy val blockGenerator = new BlockGenerator(storageLevel)

  protected override def onStart() {
    blockGenerator.start()
    
    //kinesisStream.
//    kinesisStream.addListener(new StatusListener {
//      def onStatus(status: Status) = {
//        blockGenerator += status
//      }
//      // Unimplemented
//      def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice) {}
//      def onTrackLimitationNotice(i: Int) {}
//      def onScrubGeo(l: Long, l1: Long) {}
//      def onStallWarning(stallWarning: StallWarning) {}
//      def onException(e: Exception) { stopOnError(e) }
//    })
    
    logInfo("Kinesis receiver started")
  }

  protected override def onStop() {
    blockGenerator.stop()
    kinesisStream.shutdown()
    logInfo("Kinesis receiver stopped")
  }
}