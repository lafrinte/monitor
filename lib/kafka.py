#!/usr/bin/env python
# -*- coding: utf-8 -*-

from gevent import spawn
from gevent import sleep
from gevent.pool import Group
from pykafka import KafkaClient
from .error import KafkaConnectErr


class KafkaProductor(object):

    def __init__(self, queues):
        self.queues = queues
        self.kafka = None
        self.topic = None
        self.sessions = None
        self.group = None
        self.rdkafka = None
        self._init_parse()

    def is_rdkafka(self):
        try:
            from pykafka import rdkafka
            self.rdkafka = True
        except:
            self.rdkafka = False

    def _init_parse(self):
        self.sessions = [self.is_connection(data['kafka'], data['topic'], data['queue']) for data in self.queues]
        self.is_rdkafka()

    def encode_topic(self, topic):
        if isinstance(topic, bytes) is not True:
            topic = str.encode(str(topic))
        return topic

    def switch(self):
        sleep(0)

    def is_connection(self, host, topic, queue):

        try:
            client = KafkaClient(hosts=host)
            session = client.topics[self.encode_topic(topic)]
        except:
            raise KafkaConnectErr(host)
        return (session, queue)

    def start_a_producer(self, session):

        _session, _queue = session[0], session[1]
        with _session.get_producer(delivery_reports=True, use_rdkafka=self.rdkafka) as producer:
            count = 0

            while True:
                try:
                    msg = _queue.get_nowait()
                except:
                    self.switch()
                    continue

                count += 1
                # producer.produce will only access bytes, translate the str to bytes first
                producer.produce(msg.encode())
                if count % 1000 == 0:
                    while True:
                        try:
                            msg, exc = producer.get_delivery_report(block=False)
                            self.switch()
                            if exc is not None:
                                print('Failed to deliver msg {}: {}'.format(msg.partition_key, repr(exc)))
                        except:
                            break

    @property
    def start(self):
        self.group = Group()

        for session in self.sessions:
            self.group.add(spawn(self.start_a_producer, session))
        self.group.join()