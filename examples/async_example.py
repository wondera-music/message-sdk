import asyncio

from mtds import MTDS


async def main():
    # 创建sdk实例
    sdk = MTDS(
        # kafka服务器地址
        bootstrap_servers=["127.0.0.1:9092"],
        # model name, 歌词生成就填lyrc_generate
        model="lyrics_alignment",
    )
    # 订阅消息
    sdk.subscribe("lyrics_alignment")

    while True:
        try:
            topic, partition, offset, message = await sdk.async_consume_message()

            sdk.commit()
            print(f"Consumed message:{topic=}, {message=}")

            # 你的处理逻辑
            # 开始处理时发送开始消息
            # 出错误发送错误消息
            # 成功发送成功消息

            msg = {
                # 消息体参考文档
            }

            # 发送消息到lyrc_generate_result topic
            await sdk.async_send_message(msg, "lyrics_alignment_result")
        except Exception:
            # 异常处理
            pass


asyncio.run(main())
