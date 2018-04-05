#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pykafka import KafkaClient
from .error import KafkaConnectErr


class KafkaProductor(object):

    def __init__(self, addr, topic, _queue):
        self.kafka = addr
        self.topic = topic
        self.session = None
        self.queue = _queue
        self._kafka_connect()

        if isinstance(self.topic, bytes) is not True:
            self.topic = str.encode(str(self.topic))

    def _kafka_connect(self):

        try:
            self.client = KafkaClient(hosts=self.kafka)
        except:
            raise KafkaConnectErr(self.kafka)

    @property
    def start(self):
        self.session = self.client.topics[self.topic]

        with self.session.get_producer(delivery_reports=True) as producer:
            count = 0

            while True:
                count += 1

                # producer.produce will only access bytes, translate the str to bytes first
                producer.produce(self.queue.get().encode())
                if count % 10 == 0:
                    while True:
                        try:
                            msg, exc = producer.get_delivery_report(block=False)
                            if exc is not None:
                                print('Failed to deliver msg {}: {}'.format(msg.partition_key, repr(exc)))
                        except:
                            break