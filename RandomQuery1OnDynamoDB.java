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
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.util.Tables;

/**
 * This sample demonstrates how to perform a few simple operations with the
 * Amazon DynamoDB service.
 */
public class RandomQuery1OnDynamoDB{

    /*
     * WANRNING:
     *      To avoid accidental leakage of your credentials, DO NOT keep
     *      the credentials file in your source directory.
     */

    static AmazonDynamoDBClient dynamoDB;
    static AmazonDynamoDBClient client = new AmazonDynamoDBClient(new ProfileCredentialsProvider());
    static String tableName = "weather-100k";

    /**
     * The only information needed to create a client are security credentials
     * consisting of the AWS Access Key ID and Secret Access Key. All other
     * configuration, such as the service endpoints, are performed
     * automatically. Client parameters, such as proxies, can be specified in an
     * optional ClientConfiguration object when constructing a client.
     *
     * @see com.amazonaws.auth.BasicAWSCredentials
     * @see com.amazonaws.auth.ProfilesConfigFile
     * @see com.amazonaws.ClientConfiguration
     */
    private static void init() throws Exception {
        /*
         * The ProfileCredentialsProvider will return your [default]
         * credential profile by reading from the credentials file located at
         * (C:\\Users\\Ankit\\.aws\\credentials).
         */


        AWSCredentials credentials = null;
        try {
            credentials = new ProfileCredentialsProvider("default").getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct " +
                    "location (C:\\Users\\Ankit\\.aws\\credentials), and is in valid format.",
                    e);
        }
        dynamoDB = new AmazonDynamoDBClient(credentials);
        Region usWest2 = Region.getRegion(Regions.US_WEST_2);
        dynamoDB.setRegion(usWest2);
    }

    public static void main(String[] args) throws Exception {
        init();

        try {
            

            // Describe our new table
           DescribeTableRequest describeTableRequest = new DescribeTableRequest().withTableName(tableName);
           TableDescription tableDescription = dynamoDB.describeTable(describeTableRequest).getTable();
           System.out.println("Table Description: " + tableDescription);
           
           System.out.println("Table count :"+ tableDescription.getItemCount());
           
           long  lStartTime = new Date().getTime();
           System.out.println("start time: "+lStartTime);
           Random rn= new Random();
            // Scan items for movies with a year attribute greater than 1985
           Long lItem = tableDescription.getItemCount();
           int iNoOfItems = lItem.intValue();
           System.out.println("no of item "+lItem);
            for (int i = 0; i <= 49999; i++) {
            	 String randomInt = Integer.toString(rn.nextInt(iNoOfItems));
    			 //System.out.println("Test :"+randomInt);	
    			  HashMap<String, Condition> scanFilter = new HashMap<String, Condition>();
    	            Condition condition = new Condition()
    	                .withComparisonOperator(ComparisonOperator.EQ.toString())
    	                .withAttributeValueList(new AttributeValue().withN(randomInt));
    	            scanFilter.put("id", condition);
    	            ScanRequest scanRequest = new ScanRequest(tableName).withScanFilter(scanFilter);
    	            ScanResult scanResult = dynamoDB.scan(scanRequest);
    	            System.out.println("Random No :"+randomInt+":: Query no: "+(i+1));
    	            //
    	          //  System.out.println("Result: " + scanResult);

			} 
          //calculate time difference for update file time
        	long lEndTime = new Date().getTime();
        	long difference = lEndTime - lStartTime;
        	System.out.println("Elapsed milliseconds: " + difference);
        	System.out.println("Elapsed seconds: " + difference*0.001);
        	System.out.println("Elapsed Minutes: " + (int) ((difference / (1000*60)) % 60));
        	
        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to AWS, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with AWS, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
    }
    
    
}