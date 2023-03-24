

## Week 6 Homework 


### Question 1: 

**Please select the statements that are correct**

- **Kafka Node is responsible to store topics**
- **Zookeeper is removed form Kafka cluster starting from version 4.0**
- **Retention configuration ensures the messages not get lost over specific period of time.**
- **Group-Id ensures the messages are distributed to associated consumers**


### Question 2: 

**Please select the Kafka concepts that support reliability and availability**

- **Topic Replication**
- **Topic Paritioning**
- Consumer Group Id
- Ack All



### Question 3: 

**Please select the Kafka concepts that support scaling**  

- **Topic Replication**
- **Topic Paritioning**
- **Consumer Group Id**
- Ack All


### Question 4: 

**Please select the attributes that are good candidates for partitioning key. 
Consider cardinality of the field you have selected and scaling aspects of your application**  

- payment_type
- **vendor_id**
- passenger_count
- total_amount
- **tpep_pickup_datetime**
- **tpep_dropoff_datetime**


### Question 5: 

**Which configurations below should be provided for Kafka Consumer but not needed for Kafka Producer**

- **Deserializer Configuration**
- Topics Subscription
- Bootstrap Server
- **Group-Id**
- Offset
- Cluster Key and Cluster-Secret

### Additional task: 

Please implement a streaming application, for finding out popularity of PUlocationID across green and fhv trip datasets.
Please use the datasets [fhv_tripdata_2019-01.csv.gz](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv) 
and [green_tripdata_2019-01.csv.gz](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/green)
