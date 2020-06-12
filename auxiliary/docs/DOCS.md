# System architecture
## Components:
* Producer
* Kafka
* Spark Streaming
* Consumer
##
1. Producer - python script, which receives data from stream and sends it to Kafka topic "events". 
Stream link : http://stream.meetup.com/2/rsvps

2. Kafka - multibroker cluster with following topics: US-meetups, US-cities-every-minute, Programming-meetups

3. Spark Streaming - PySpark application, deployed on the same instances with Kafka. For processing is use PySpark SQL Structured Streaming.
Firstly, parse the json data into dataframes. Some details:
    1) filtering for US is done during preprocessing (exactly after parsing)
    2) just format and send to appropriate Kafka topic
    3) here is used window function with timestamp of 1 minute
    4) format as in 1, but added filtering by topics (used arrays_overlap function)

4. Consumer - python script which is responsible for gathering entire results and send the report to S3 bucket. 