from enum import Enum


class TaskType(Enum):
    GENERATE_SONG = 'GENERATE_SONG'  # 生成歌曲
    SEPARATE_SONG = 'SEPARATE_SONG'  # 分离歌曲
    SPE = 'SPE'  # 分离模型
    SVC = 'SVC'  # svc模型
    SVC_SO_VITS = 'SVC_SO_VITS'  # svc模型
