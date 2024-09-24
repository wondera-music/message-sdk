from mtds import MTDS


# 消息回调函数
def message_handler(sdk, topic, partition, offset, message):
    print(f"message_handler running : {topic=}, {partition=}, {offset=},{message=}")
    sdk.commit()

    # 你的处理逻辑
    # 开始处理时发送开始消息
    # 出错误发送错误消息
    # 成功发送成功消息

    msg = {}

    sdk.send_message(msg, "lyrics_alignment_result")


if __name__ == "__main__":
    # 创建sdk实例
    sdk = MTDS(
        # kafka服务器地址
        bootstrap_servers=["127.0.0.1:9092"],
        # model name, 歌词生成就填lyrc_generate
        model="lyrics_alignment",
    )
    # 订阅消息
    sdk.subscribe("lyrics_alignment")

    sdk.set_message_handler(message_handler)
    sdk.consume_message()
