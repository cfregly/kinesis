/*
 * Copyright 2012-2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;

public class KinesisStreamDataProducer {

    private static AmazonKinesisClient kinesisClient;
    private static final Log logger = LogFactory.getLog(KinesisStreamDataProducer.class);

    private static void init() throws Exception {
        kinesisClient = new AmazonKinesisClient(new DefaultAWSCredentialsProviderChain());
    }

    public static void main(String[] args) throws Exception {
        init();

        final String myStreamName = "sparkStream";

        logger.info("Putting records in stream : " + myStreamName);
        // Write records to the stream
        for (int j = 0; j < 100; j++) {
            PutRecordRequest putRecordRequest = new PutRecordRequest();
            putRecordRequest.setStreamName(myStreamName);
            putRecordRequest.setData(ByteBuffer.wrap(String.format("testData-%d testData-%d testData-%d", j, j, j).getBytes()));
            putRecordRequest.setPartitionKey(String.format("partitionKey-%d", j));
            PutRecordResult putRecordResult = kinesisClient.putRecord(putRecordRequest);
            System.out.println("Successfully putrecord, partition key : " + putRecordRequest.getPartitionKey()
                    + ", ShardID : " + putRecordResult.getShardId());
        }
    }
}