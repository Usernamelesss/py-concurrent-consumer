from confluent_kafka import Producer

p = Producer({'bootstrap.servers': 'localhost:9093'})

p.produce(topic='T1', value='some test message', key='KEY', partition=1)
p.flush()
