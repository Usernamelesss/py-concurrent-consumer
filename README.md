# Python Concurrent Consumer
This is a demo I wrote to setup a Kafka concurrent consumers in Python (using the kafka-python). 

The idea is to have one or more Kafka Consumer running on the same service. 
Each consumer can be started in parallel (multiprocessing) or in concurrency (multithreading), 
depending on you needing. 
Each Consumer is designed to subscribed to a specific topic. 


