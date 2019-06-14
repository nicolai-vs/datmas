from time import sleep
from json import dumps
from json import loads
from kafka import KafkaProducer
import socket

UDP_IP = "127.0.0.1"
UDP_PORT = 9999
BUFFER_SIZE = 1024

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind((UDP_IP, UDP_PORT))

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))


while True:
    data, addr = sock.recvfrom(BUFFER_SIZE)
    data = loads(data)
    producer.send('udp-input', value=data)