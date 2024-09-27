import asyncio
import json
import signal
import sys
from json.decoder import JSONDecodeError
from time import sleep

from confluent_kafka import Consumer
from confluent_kafka import KafkaError
from confluent_kafka import KafkaException
from confluent_kafka import Producer


class MTDS:
    # TODO 单例模式
    def __init__(
        self,
        bootstrap_servers,
        group_id=None,
        loop=None,
        app=None,
        model=None,
        parse_message=True,
    ):
        self._loop = loop or asyncio.get_event_loop()
        # self._loop = asyncio.get_running_loop()
        self._bootstrap_servers = None
        self.group_id = group_id
        self.app = app
        self.model = model
        self._consumer = None
        self._producer = None
        self.subscribed_topics = set()
        self.dest_topics = set()
        self._shutdown_flag = False
        self.__message_handler = None
        self.__parse_message = parse_message

        # TODO 存在bug
        if isinstance(bootstrap_servers, list) and len(bootstrap_servers) == 1:
            self._bootstrap_servers = bootstrap_servers[0]
        elif isinstance(bootstrap_servers, str):
            self._bootstrap_servers = bootstrap_servers
        else:
            raise ValueError("bootstrap_servers must be a valid string or list")

        if not (bool(app) ^ bool(model)):
            raise ValueError("app and model can only be set one of them at the same time")

        for name, value in {"group_id": group_id, "app": app, "model": model}.items():
            if value:
                self._validate_non_empty_string(value, name)

        self.group_id = self.group_id if self.group_id else (self.app if self.app else self.model)

        self._register_signal()

        # self._init_topics()

        # self.print_skd_info()

        self._connect_kafka()

    # TODO
    def print_skd_info(self):
        print(f"bootstrap_servers: {self._bootstrap_servers}")
        print(f"group_id: {self.group_id}")
        print(f"app: {self.app}")
        print(f"model: {self.model}")
        print(f"subscribed_topics: {self.subscribed_topics}")
        print(f"dest_topics: {self.dest_topics}")

    @staticmethod
    def _validate_non_empty_string(value, name):
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{name} must be a valid non-empty string")

    def set_message_handler(self, handler):
        if not callable(handler):
            raise ValueError("message_handler must be a callable function")
        self.__message_handler = handler

    # def _init_topics(self):
    #     if self.model is not None:
    #         self.subscribed_topics.add(self.model)
    #         self.dest_topics.add(f"{self.model}_result")

    def _connect_kafka(self):
        print(f"Trying to connect to Kafka:{self._bootstrap_servers}")

        def error_cb(error_msg):
            print(f"---------{error_msg=}")

            if error_msg.code() is KafkaError._ALL_BROKERS_DOWN:
                print("All Kafka brokers are down")

        try:
            self._consumer = Consumer(
                {
                    "bootstrap.servers": self._bootstrap_servers,
                    "group.id": self.group_id,
                    "auto.offset.reset": "earliest",  # 如果消费者组没有偏移量，从最早的消息开始消费
                    "allow.auto.create.topics": True,
                    "enable.auto.commit": False,  # 关闭自动提交
                    # 'auto.create.topics.enable': True,
                    # 'error_cb': error_cb,
                }
            )

            self._consumer.list_topics(timeout=5)

            self._producer = Producer(
                {
                    "bootstrap.servers": self._bootstrap_servers,
                    # 'on_delivery': connect_callback,
                    "error_cb": error_cb,
                }
            )

            print(f"Connected to Kafka:{self._bootstrap_servers}")
        except KafkaException as e:
            print(f"Connect to Kafka error: {e}")
            sys.exit(1)

    # def get_topics(self):
    #     return self._topics

    # def set_topics(self, topics):
    #     # TODO 加日志
    #     self._topics = topics
    #     return self._topics

    # def add_subscribed_topics(self, topics=None):
    #     # TODO 加日志
    #     # if not isinstance(topics, list) or not all(isinstance(topic, str) and topic.strip() for topic in topics):

    #     if not isinstance(topics, list):
    #         raise ValueError('topics must be a UnNone list')

    #     for topic in topics:
    #         # 入股topic 不是string或者为空字符串，抛出异常
    #         if not isinstance(topic, str) or topic == '':
    #             raise ValueError('topic must be a string and not empty')

    #     for key in topics:
    #         if key in self._topics and isinstance(topics[key], list):
    #             self._topics[key].extend(topics[key])
    #         else:
    #             raise ValueError('topics is not valid')

    #     return self._topics

    def _delivery_callback(self, err, msg):
        if err:
            print(f"Message failed delivery: {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}] {msg.offset()},{msg.value()}")

    def sync_send_message(self, message, topic=None):
        # TODO 这里检查逻辑太丑了
        if self._producer is None:
            raise RuntimeError("Kafka producer is not initialized")

        if (topic is not None) and (not isinstance(topic, str) or topic == ""):
            raise ValueError("topic must be a valid string")

        # APP类型sdk必须指定消息发往的topic
        if self.app and not topic:
            raise ValueError("App type sdk instance must specify a valid topic when sending a message")

        # TODO 目前不确定是否要向多个topic发送相同消息的逻辑，暂时先不实现
        if len(list(self.dest_topics)) > 1:
            raise NotImplementedError("Sending message to multiple topics is not implemented yet")

        to_topic = topic if topic else list(self.dest_topics)[0]

        try:
            self._producer.produce(
                to_topic,
                message if isinstance(message, bytes) else json.dumps(message, ensure_ascii=False).encode("utf-8"),
                callback=self._delivery_callback,
            )

            self._producer.flush(timeout=5)

        except BufferError:
            print(f"Local producer queue is full ({len(self._producer)} messages awaiting delivery): try again")
        except Exception as e:
            print(f"Producer error: {e}")

    async def async_send_message(self, message, topic=None):
        # await asyncio.sleep(5)
        # TODO 检测message是否是dict
        # 将message限制为dict类型不太合理
        # TODO 这里检查逻辑太丑了
        if self._producer is None:
            raise RuntimeError("Kafka producer is not initialized")

        if (topic is not None) and (not isinstance(topic, str) or topic == ""):
            raise ValueError("topic must be a valid string")

        # APP类型sdk必须指定消息发往的topic
        if self.app and not topic:
            raise ValueError("App type sdk instance must specify a valid topic when sending a message")

        # TODO 目前不确定是否要向多个topic发送相同消息的逻辑，暂时先不实现
        if len(list(self.dest_topics)) > 1:
            raise NotImplementedError("Sending message to multiple topics is not implemented yet")

        to_topic = topic if topic else list(self.dest_topics)[0]

        result = self._loop.create_future()

        def ack(err, msg):
            if err:
                self._loop.call_soon_threadsafe(result.set_exception, KafkaException(err))
            else:
                self._loop.call_soon_threadsafe(result.set_result, msg)

            # if not isinstance(message, bytes):
            #     raw_msg = str(message).encode('utf-8')

        raw_msg = message if isinstance(message, bytes) else json.dumps(message, ensure_ascii=False).encode("utf-8")

        self._producer.produce(to_topic, raw_msg, on_delivery=ack)
        return result

        # try:
        #     self._producer.produce(to_topic, json.dumps(message).encode('utf-8'), callback=self._delivery_callback)

        #     self._producer.flush(timeout=5)

        # except BufferError:
        #     print(f'Local producer queue is full ({len(self._producer)} messages awaiting delivery): try again')
        # except Exception as e:
        #     print(f'Producer error: {e}')

    def send_done_message(self, message):
        pass

    def send_fail_message(self, message):
        pass

    def commit(self):
        if self._consumer is None:
            raise RuntimeError("Kafka consumer is not initialized")
        self._consumer.commit()

    def subscribe(self, topics):
        if isinstance(topics, str):
            if not topics.strip():
                raise ValueError(f"Topics {topics} is not valid")
        elif isinstance(topics, list):
            if not topics:
                raise ValueError("Topics must be a unempty list")

            if not all(isinstance(topic, str) for topic in topics):
                raise ValueError("topics must be a valid list")
        else:
            # 既不是字符串也不是列表时，抛出异常
            raise ValueError("Topics must be a valid string or list")

        if self._consumer is None:
            raise RuntimeError("Kafka consumer is not initialized")

        print(f"consuming topics: {topics}")
        self._consumer.subscribe(topics)

    def consume_message(self):
        # TODO 加日志 打印所有topics
        if self._consumer is None:
            raise RuntimeError("Kafka consumer is not initialized")

        while not self._shutdown_flag:
            try:
                msg = self._consumer.poll(timeout=1.0)

                if msg is None:
                    continue

                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                    continue

                print(f'Received message: {msg.value().decode("utf-8")}')

                if not callable(self.__message_handler):
                    raise ValueError("message_handler must be a callable function")

                msg_content = json.loads(msg.value().decode("utf-8")) if self.__parse_message else msg.value()

                self.__message_handler(self, msg.topic(), msg.partition(), msg.offset(), msg_content)

            except JSONDecodeError as e:
                # TODO 目前没有更好处理方式
                # 如果不commit,遇到错误消息就没有颁发处理了
                print(f"Parse message JSONDecodeError: {e}, message: {msg.value()}")
                if self.__parse_message:
                    self._consumer.commit()
            except Exception as e:
                print(f"Consumer error: {str(e)}, message: {msg.value()}")

                # TODO 这里不好，如果不sleep,一连串错误消息会导致系统卡死；如果sleep,可能会影响业务响应时间
                sleep(1)

    def _shutdown(self):
        self._shutdown_flag = True

        sleep(2)

        if self._consumer is not None:
            self._consumer.unsubscribe()
            self._consumer.close()

        if self._producer is not None:
            self._producer.flush(timeout=10)

        if self._loop is not None:
            self._loop.stop()

        sys.exit(0)

    def _register_signal(self):
        def signal_handler(signum, frame):
            print(f"Exit signal:{signal.Signals(signum).name} received, will shutting down gracefully")
            self._shutdown()

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    async def async_consume_message(self, topics=None):
        """
        Asynchronously consume messages by using run_in_executor to avoid blocking.
        """
        if self._consumer is None:
            raise RuntimeError("Kafka consumer is not initialized")

        while not self._shutdown_flag:
            # print('consuming......')
            if self._consumer is None:
                raise RuntimeError("Kafka consumer is not initialized")

            try:
                msg = await self._loop.run_in_executor(None, self._consumer.poll, 0.1)
                if msg is None:
                    continue

                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                    continue

                msg_content = json.loads(msg.value().decode("utf-8")) if self.__parse_message else msg.value()
                return (msg.topic(), msg.partition(), msg.offset(), msg_content)
            except JSONDecodeError as e:
                print(f"Parse message JSONDecodeError: {e}, message: {msg.value()}")
                if self.__parse_message:
                    self._consumer.commit()
            except Exception as e:
                print(f"Consumer error: {str(e)}")
                # print(f'message: {msg.value()}')
                # TODO 这里不好，如果不sleep,一连串错误消息会导致系统卡死；如果sleep,可能会影响业务响应时间
                await asyncio.sleep(0.1)

        print("exited async_consume_message")

    async def get_response(self, task_id, condition):
        pass
