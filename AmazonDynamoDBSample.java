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
import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Statement;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.services.dynamodbv2.util.Tables;

/**
 * This sample demonstrates how to perform a few simple operations with the
 * Amazon DynamoDB service.
 */
public class AmazonDynamoDBSample {

    /*
     * WANRNING:
     *      To avoid accidental leakage of your credentials, DO NOT keep
     *      the credentials file in your source directory.
     */

    static AmazonDynamoDBClient dynamoDB;

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
         * (C:\\Users\\Divendar\\.aws\\credentials).
         */
        AWSCredentials credentials = null;
        try {
            credentials = new ProfileCredentialsProvider("default").getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct " +
                    "location (C:\\Users\\Divendar\\.aws\\credentials), and is in valid format.",
                    e);
        }
        dynamoDB = new AmazonDynamoDBClient(credentials);
        Region usWest2 = Region.getRegion(Regions.US_WEST_2);
        dynamoDB.setRegion(usWest2);
    }

    public static void main(String[] args) throws Exception {
        init();

        try {
            String tableName = "weather-100k";

            // Create table if it does not exist yet
            if (Tables.doesTableExist(dynamoDB, tableName)) {
                System.out.println("Table " + tableName + " is already ACTIVE");
            } else {
                // Create a table with a primary hash key named 'name', which holds a string
                CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(tableName)
                    .withKeySchema(new KeySchemaElement().withAttributeName("id").withKeyType(KeyType.HASH))
                    .withAttributeDefinitions(new AttributeDefinition().withAttributeName("id").withAttributeType(ScalarAttributeType.N))
                    .withProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(1L).withWriteCapacityUnits(1L));
                    TableDescription createdTableDescription = dynamoDB.createTable(createTableRequest).getTableDescription();
                System.out.println("Created Table: " + createdTableDescription);

                // Wait for it to become active
                System.out.println("Waiting for " + tableName + " to become ACTIVE...");
                Tables.waitForTableToBecomeActive(dynamoDB, tableName);
            }

            // Describe our new table
            DescribeTableRequest describeTableRequest = new DescribeTableRequest().withTableName(tableName);
            TableDescription tableDescription = dynamoDB.describeTable(describeTableRequest).getTable();
            System.out.println("Table Description: " + tableDescription);

            long  lStartTime = new Date().getTime();
            System.out.println("start :"+  lStartTime);

    		// TODO Auto-generated method stub
    		BufferedReader br = new BufferedReader(new FileReader("weather-100K.csv"));
    		int iKey=1;
    		for (String line = br.readLine(); line != null; line = br.readLine()) {
    		// System.out.println("Line:  "+ line);  //... // do stuff to file here  
    			String sEachLine = line.toString();
    			   String[] sTemp = sEachLine.split(Pattern.quote(","));
    		           
    		           /*int noOfCount =1 ; 
    		           String TempManu="";*/
    		          /* for(int i=0;i<sTemp.length;i++)
    		           {*/
    		    
    		        	// Add an item
    		                Map<String, AttributeValue> item = newItem(iKey,sTemp[0],sTemp[1],sTemp[2],sTemp[3],sTemp[4],sTemp[5],sTemp[6],
    		                		sTemp[7],sTemp[8],sTemp[9],sTemp[10],sTemp[11],sTemp[12],sTemp[13],sTemp[14],sTemp[15]);
    		                PutItemRequest putItemRequest = new PutItemRequest(tableName, item);
    		                PutItemResult putItemResult = dynamoDB.putItem(putItemRequest);
    		               // System.out.println("Result: " + putItemResult);
    		                iKey++;
    			/*
    		           }*/
    			
    		}
    	
    		//calculate time difference for update file time
    		long lEndTime = new Date().getTime();
    		long difference = lEndTime - lStartTime;
    		System.out.println("Elapsed milliseconds: " + difference);
    		System.out.println("Elapsed seconds: " + difference*0.001);
    		System.out.println("Elapsed Minutes: " + (int) ((difference / (1000*60)) % 60));
    	    System.out.println("End:"+  lEndTime);
            /*// Add another item
            item = newItem("Airplane", 1980, "*****", "James", "Billy Bob");
            putItemRequest = new PutItemRequest(tableName, item);
            putItemResult = dynamoDB.putItem(putItemRequest);
            System.out.println("Result: " + putItemResult);*/

            /*// Scan items for movies with a year attribute greater than 1985
            HashMap<String, Condition> scanFilter = new HashMap<String, Condition>();
            Condition condition = new Condition()
                .withComparisonOperator(ComparisonOperator.GT.toString())
                .withAttributeValueList(new AttributeValue().withN("1985"));
            scanFilter.put("year", condition);
            ScanRequest scanRequest = new ScanRequest(tableName).withScanFilter(scanFilter);
            ScanResult scanResult = dynamoDB.scan(scanRequest);
            System.out.println("Result: " + scanResult);*/

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
 
    private static Map<String, AttributeValue> newItem( int Key, String STATION, String	STATION_NAME, String DATE, String TMAX, String TMIN, String TOBS, String WDMV, String AWND, String WDF2, String WDF5, String WDFG, String WSF2, String WSF5, String WSFG, String PGTM, String FMTM) {

    	Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();

        item.put("id", new AttributeValue().withN(Integer.toString(Key)));
        item.put("STATION", new AttributeValue(STATION));
        item.put("STATION_NAME", new AttributeValue(STATION_NAME));
        item.put("DATE", new AttributeValue(DATE));
        item.put("TMAX", new AttributeValue(TMAX));
        item.put("TMIN	", new AttributeValue(TMIN));
        item.put("TOBS", new AttributeValue(TOBS));
        item.put("WDMV", new AttributeValue(WDMV));
        item.put("AWND", new AttributeValue(AWND));
        item.put("WDF2", new AttributeValue(WDF2));
        item.put("WDF5", new AttributeValue(WDF5));
        item.put("WDFG", new AttributeValue(WDFG));
        item.put("WSF2", new AttributeValue(WSF2));
        item.put("WSF5", new AttributeValue(WSF5));
        item.put("WSFG", new AttributeValue(WSFG));
        item.put("PGTM", new AttributeValue(PGTM));
        item.put("FMTM", new AttributeValue(FMTM));

        return item;
    }

}
