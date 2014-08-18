AMAZON-WEB-SERVICES--S3--EC2--RDB--DynamoDB--Random-Queries-Program
===================================================================

• Program to copy the CSV or TSV file from local machine to AWS S3. 
• Using EC2 instance put that data into an AWS Relational DB and non- relational DB (NoSQL). 
• Execute 50 thousand random queries on that data and calculate the time.

===================================================================
1) Transfer data from local to Amazon S3 
2) Insert Data from S3 to Amazon RDB(MySql)
3) Insert Data from S3 to Amazon DynamoDB(NoSql)
4) For 2 and 3 ...
	->Perform Random queries on 100k data
	-> Perform Random queries on 0.1 to 1 % of the 	data
5) for 1, 2, 3 and 4 calculate time of execution

-> File weather-100K.csv contains 100k data. 

-> File S3TransferProgressSample.java is to transfer files from Local machine to AWS S3.

-> File AmazonDynamoDBSample.java and AmazonRDBSample is to insert data into AWS RDB and DynamoDB respectively.

-> File RandomQuery1OnRDB and RandomQuery1OnDynamoDB is to perform Random queries on 100k data of AWS RDB and DynamoDB respectively .

-> File RandomQuery2OnRDB and RandomQuery2OnDynamoDB is to perform Random queries on 0.1 to 1 % of the data of AWS RDB and DynamoDB respectively. 

-> Each file has Start Time and End Time to calculate execution time.
