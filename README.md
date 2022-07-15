# Website-Monitoring-Architecture-using-AWS

## Website Monitoring

Website Monitoring is an all-encompassing term for any activity that involves testing a website / web service for availability, performance, or function. It checks and verifies that the site is up and working and end-users can use the site as expected.


## Objective of the project

To create an architecture of real time website monitoring system that will provide functionality to test availability and performance of website by streaming and analysing logs, and trigger Lambda in case of any event or end-to-end testing which will be deployed using AWS services. The system also aims at storing copy of all the error messages in Aurora MySQL for aggregated views in the future.


## Real World Business Use Case

1. In some common shopping retail e-commerce website like  Amazon/Flipkart, sales for the product go up remarkably high maybe because of some limited time offer period or if the site is being misued by attackers doing phishing or fraudulent activity with order requests. In such cases, website monitoring system can provide a solution that can identify any such unusual activity in terms of maximum orders(which is set to 15) placed in every 15 seconds using AWS real-time streaming and processing systems. As an additional functionality, the system can be integrated with feature that can check if the orders are authentic and take action like increase the number of backend servers for processing or block the IP address from order placing, etc.

2. Media Streaming services such as Netflix uses such system to monitor the communications between all of its applications so it can detect and fix issues quickly, ensuring high service uptime and availability to its customers.


## Technologies used

* AWS Kinesis 
* AWS EC2 Instance
* AWS Lambda
* Amazon Aurora
* Amazon Dynamo DB
* Amazon SNS




## System Architecture (draw own diagram Lucid or something)

<explain sys arch later or something >



## Amazon Kinesis 
### Kinesis Data Analytics

Amazon Kinesis is an fully-managed data analytic AWS service that enables us to ingest, buffer, and process streaming data in real-time for machine learning, analytics, and other applications. It helps to process and analyze data as it arrives and respond instantly instead of having to wait until all your data is collected before the processing can begin.
It includes pre-built SQL functions for several advanced analytics including one for anomaly detection where we ccan simply make a call to this function from  SQL code for detecting anomalies in real-time.
This service is heavily used in end-to-end stream processing applications for log analytics, clickstream analytics, Internet of Things (IoT), ad tech, gaming, e-commerce platform, etc.


<make your own custom diagram>
<img width="798" alt="Screenshot 2022-07-15 at 9 14 25 AM" src="https://user-images.githubusercontent.com/25201417/179146007-8dba4247-b738-4b60-aac5-593fca6ae1d6.png">

AWS Kinesis works on first configuring  and collecting data ftrom various input data streams such as input devices, or Amazon Kinesis data stream or Amazon Kinesis Data Firehouse, and these streams can be queried using built-in integration for SQL analytics  and finally stored as output in Amazon s3 or Redshift, etc.

### Kinesis Data Streams(KDS)

Amazon Kinesis Data Streams (KDS) is a massively scalable and durable real-time data streaming service which can continuously capture GBs of data/second from hundreds of thousands of sources.The data collected is available in ms to enable real-time analytics use cases such as real-time dashboards, real-time anomaly detection, dynamic pricing, etc.
This works on first connecting external data source to KDS by Kinesis Agent or we can even use Firehose, Data Analytics as source data.The streamed data is divided into pre-configured shards to control the data throughput. This partitioned data can be further consumed and passed on to downstream system for analytics.



### Connect EC2 Instance and install Kinesis agent

< pdf SSH into EC2 instance and install Kinesis agent >

<Dataset online retail csv>

<LogGenerator.py>

<11 th video>

copy data , py, to EC2 instance

attach terminal ss

## Running and Processing Kinesis Analytics

In this section, once we have loaded and log stream the data into Kinesis , we will run and process the Kinesis Analytics on the stored data.

Steps to run and process the Kinesis Analytics:-

1. Create Kinesis Data Analytics Application from AWS console, and set Runtime as SQL.

2.  Next, connect the Kinesis Data Stream with the previously built data stream from LogGenerator.py script.

3.  The application , after loading the data stream , infers the schema by applying appropriate headers and fields , and returns a table. In the next step, we can also "Connect Reference Data" i.e. putting joins on our current table with the already existing data on S3(this data is mostly static and doesn't change much).

4. Then, we have to open SQL editor and start running the application .

For SQL , we can refer to this blog : https://aws.amazon.com/blogs/big-data/writing-sql-on-streaming-data-with-amazon-kinesis-analytics-part-1/#:~:text=Amazon%20Kinesis%20Analytics%20provides%20an,downstream%20to%20your%20configured%20destinations.

```
CREATE OR REPLACE STREAM "ALARM_STREAM" (order_count INTEGER);

CREATE OR REPLACE PUMP "STREAM_PUMP" AS
    INSERT INTO "ALARM_STREAM"
        FROM (
              SELECT STREAM COUNT(*) OVER FIFTEEN_SECOND_SLIDING_WINDOW AS order_count
              FROM "SOURCE_SQL_STREAM_001";
              WINDOW FIFTEEN_SECOND_SLIDING_WINDOW AS (RANGE INTERVAL '15' SECOND PRECEDING)
            )
        WHERE order_count >=30;
        
CREATE OR REPLACE STREAM TRIGGER_COUNT_STREAM(
  order_count INTEGER,
  trigger_count INTEGER);
  
CREATE OR REPLACE PUMP trigger_count_pump AS
    INSERT INTO "TRIGGER_COUNT_STREAM"
    SELECT STREAM order_count, trigger_count
        FROM (
              SELECT STREAM order_count , COUNT(*) OVER W1 AS trigger_count
              FROM "ALARM_STREAM";
              WINDOW W1 AS (RANGE INTERVAL '1' MINUTE PRECEDING)
            )
        WHERE trigger_count >=1;
        
```


Explanation of above SQL code:-

1. Firstly, we create a table name ALARM_STREAM with only one integer column i.e. order_count
2. Next, code snippet shows the source for the above created table where we have chose the count of rows as a 15 second window. Hence, we store the
   information of order_count every 15 seconds in the table.
3. In the next SQL command, we have created a second stream of the data, where we have trigger_count as input in addition to order_count. Here, we have      set a 1 minute interval to trigger alarm otherwise, if someone is trying to bring the site down, we will get alarms every second. Thus, in order to        avoid that, we set this time interval before we get alarm.

We also setup the Aurora MySQL (go to RDS in AWS) instance in order to hold all the alarm details over the time, so that we don't loose track of alarms that have been triggered in the past. This is useful to evalate our cloud instance resourcees , whethere to increase/ decrease for a given month or period.
For this project, we have chose "Capacity Type" as "Serverless" , and configured other related settings.After our DB is setup, we can connect AWS Secret Manager to our application.

<img width="760" alt="Screenshot 2022-07-15 at 12 44 53 PM" src="https://user-images.githubusercontent.com/25201417/179171949-a9301aec-54dd-4340-9de8-6a43df320acf.png">


After our DB is setup, we can run the following SQL command 
```
use logs; create table logs_history( alarm_creation_time datetime, Number_of_orders int )
select * from information_schema.tables where table_schema='logs';
select * from logs.logs_history; //should fetch 0 rows
```

## End to End Testing

We can setup the Dynamo DB with the basic config and run the Lambda function by initialising the lambda handler. The code for lambda_handler()
is given in py file and is shown below:

Note: We have to make sure that we give Read Access for Kinesis and Amazon Dynambo DB full access to Lambda by attaching policy.
```
#!/usr/bin/python
# -*- coding: utf-8 -*-
import base64
import json
import boto3
import decimal

def lambda_handler(event, context):

    item = None
    dynamo_db = boto3.resource('dynamodb')
    table = dynamo_db.Table('WebsiteOrders')

    decoded_record_data = [base64.b64decode(record['kinesis']['data'])
                           for record in event['Records']]

    deserialized_data = [json.loads(decoded_record)
                         for decoded_record in decoded_record_data]

    print event

    with table.batch_writer(overwrite_by_pkeys=['CustomerID', 'OrderID'
                            ]) as batch_writer:

        for item in deserialized_data:

            invoice = item['InvoiceNo']
            customer = int(item['Customer'])
            orderDate = item['InvoiceDate']
            quantity = item['Quantity']
            description = item['Description']
            unitPrice = item['UnitPrice']
            country = item['Country'].rstrip()
            stockCode = item['StockCode']

            # Construct a unique sort key for this line item

            orderID = invoice + '-' + stockCode

            batch_writer.put_item(Item={
                'CustomerID': decimal.Decimal(customer),
                'OrderID': orderID,
                'OrderDate': orderDate,
                'Quantity': decimal.Decimal(quantity),
                'UnitPrice': decimal.Decimal(unitPrice),
                'Description': description,
                'Country': country,
                })

```


 














