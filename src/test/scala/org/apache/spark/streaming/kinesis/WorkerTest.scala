package org.apache.spark.streaming.kinesis

import org.junit.Test
import org.junit.Assert._

import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext.toPairDStreamFunctions

class WorkerTest {
	@Test
	def run() {
	  	val ssc = new StreamingContext("local[1]", "WordCountCustomStreamSource", Seconds(1))
	
	  	val lines = KinesisUtils.createStream(ssc, null)
	
	  	val words = lines.flatMap(_.split(" "))

	  	val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)

	  	wordCounts.print()
    
	  	ssc.start()
	
	  	ssc.awaitTermination()
  	}
  
}
