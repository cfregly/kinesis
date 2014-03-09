//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
//import org.apache.spark.storage.StorageLevel;
//import org.apache.spark.streaming.dstream.NetworkReceiver.BlockGenerator;
//
//import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
//import com.amazonaws.services.kinesis.AmazonKinesisClient;
//
//public class KinesisNetworkReceiver 
////extends NetworkReceiver 
//{
//	private BlockGenerator blockGenerator = new BlockGenerator(StorageLevel.MEMORY_ONLY());
//    private static AmazonKinesisClient kinesisClient;
//    private static final Log logger = LogFactory.getLog(KinesisWorker.class);
//
//	public KinesisNetworkReceiver() {
//		init();
//	}
//	
//	private static void init() {
//        /*
//         * This credentials provider implementation loads your AWS credentials
//         * from a properties file at the root of your classpath.
//         */
//    	
//        kinesisClient = new AmazonKinesisClient(new DefaultAWSCredentialsProviderChain());
//	}
//	
//	protected void onStart() {
////	    blockGenerator.start();
////
////	    final String myStreamName = "sparkStream";
////
////        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
////        describeStreamRequest.setStreamName( myStreamName );
////        List<Shard> shards = new ArrayList<>();
////        String lastShardId = null;
////        DescribeStreamResult describeStreamResult = null;
////        do {
////            describeStreamRequest.setExclusiveStartShardId( lastShardId );
////            describeStreamResult = kinesisClient.describeStream( describeStreamRequest );
////            shards.addAll( describeStreamResult.getStreamDescription().getShards() );
////            if (shards.size() > 0) {
////                lastShardId = shards.get(shards.size() - 1).getShardId();
////            }
////        } while ( describeStreamResult.getStreamDescription().getHasMoreShards() );
////            
////        String shardIterator;
////        GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest();
////        getShardIteratorRequest.setStreamName(myStreamName);
////        getShardIteratorRequest.setShardId(lastShardId);
////        getShardIteratorRequest.setShardIteratorType("TRIM_HORIZON");
////
////        GetShardIteratorResult getShardIteratorResult = kinesisClient.getShardIterator(getShardIteratorRequest);
////        shardIterator = getShardIteratorResult.getShardIterator();
////	    
////        List<Record> records;
////
////        while (true) {           
////			//Create new GetRecordsRequest with existing shardIterator. 
////			//Set maximum records to return to 1000.
////			GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
////			getRecordsRequest.setShardIterator(shardIterator);
////			getRecordsRequest.setLimit(1000); 
////			
////			GetRecordsResult result = kinesisClient.getRecords(getRecordsRequest);
////			  
////			//Put result into record list. Result may be empty.
////			records = result.getRecords();
////			if (records != null && !records.isEmpty()) {
////				blockGenerator.add(wordCounts);
////			}
////        }	    	   
////	    
////	    logger.info("Kinesis receiver started");
//	}
//
//	protected void onStop() {
////	    blockGenerator.stop();
//	    
//	    logger.info("Kinesis receiver stopped");
//	}
//}