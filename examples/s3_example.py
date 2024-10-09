from mtds import S3Client

# 注意： 创建S3Client后会执行list_buckets()来检查是否认证成功，
# 不要每次上传文件都创建S3Client 实例，会有性能问题
s3 = S3Client()

cdn_url = "https://d1n8x1oristvw.cloudfront.net"

# file_path = "/home/nemo/workspace/work/mtds/test.wav"
# s3_key = "streaming/lyrics_adjust/ea3c92f0e7464f2f9cbd297062c4ca4b/ea3c92f0e7464f2f9cbd297062c4ca4b.wav"


# 没有extra_args参数示例
# resp = s3.upload_to_s3(file_path, s3_key)

# 有extra_args参数示例
# 文件通过浏览器下载时，文件名自动设置为“晴天_周杰伦_acc.wav”
# resp = s3.upload_to_s3(
#     file_path,
#     s3_key,
#     extra_args={"ContentDisposition": 'attachment;filename="晴天_周杰伦_acc.wav"'},
# )

# print(f"url: {cdn_url}/{s3_key}")


# s3_key = "streaming/lyrics_adjust/ea3c92f0e7464f2f9cbd297062c4ca4b/ea3c92f0e7464f2f9cbd297062c4c222.lyric"

# resp = s3.put_object(
#     "测试数据 test aaa".encode(),
#     s3_key,
# )
# print(f"url: {cdn_url}/{s3_key}")


s3_key = "streaming/lyrics_adjust/ea3c92f0e7464f2f9cbd297062c4ca4b/ea3c92f0e7464f2f9cbd297062c4c224.lyric"

resp = s3.put_object(
    "测试数据 test aaa22".encode(),
    s3_key,
    ContentDisposition='attachment;filename="test.lyric"',
)
print(f"url: {cdn_url}/{s3_key}")
