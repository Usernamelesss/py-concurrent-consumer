from environs import Env

env = Env()

KAFKA_BOOTSTRAP = env('KAFKA_BOOTSTRAP', 'localhost:9093')
