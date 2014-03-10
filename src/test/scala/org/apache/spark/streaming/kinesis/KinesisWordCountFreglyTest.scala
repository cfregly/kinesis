package org.apache.spark.streaming.kinesis

import org.junit.Test
import org.junit.Assert._
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext.toPairDStreamFunctions
import org.apache.spark.streaming.dstream.DStream

class KinesisWordCountFreglyTest {
	// TODO:  make this a real unit test versus depending on a live Kinesis stream
    @Test
	def testKinesisStreamReceiver() {
        val master : String = "local[1]"
        val streamName : String = "sparkStream"
          
	    // create the StreamingContext with master val
	  	val ssc : StreamingContext = new StreamingContext(master, "KinesisWordCount", Seconds(1),	System.getenv("SPARK_HOME"), StreamingContext.jarOfClass(KinesisWordCountFreglyTest.this.getClass))

	  	// setup KinesisStreamReceiver with streamName val
	  	val stream : DStream[String] = ssc.networkStream[String](new KinesisStreamReceiver(streamName, null))	  	
	  	
	  	// split the stream into words and return the flatMap (collection)
	  	val words = stream.flatMap(_.split(" "))	  	

	  	// map each word to a (word, 1) tuple
	  	val wordCounts = words.map(x => (x, 1))
	  	// reduce (aggregate) by key to get the count for each word
	  	// TODO:  do this over a window using reduceByKeyAndWindow()
	  		.reduceByKey(_ + _)	  		  	
	  		
	  	// print the word count
	  	//  ** this is an output action which causes the RDDs to actually materialize. 
	  	// 	   everything up til now is lazily defined.
	  	wordCounts.print()
    
	  	// start the StreamingContext
	  	ssc.start()
	  	
	  	// wait for execution to stop
	  	//   (exceptions will bubble up in this thread)
	  	ssc.awaitTermination()
  	}  
}
