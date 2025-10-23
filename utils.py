import json
import os

PROGRESS_FILE = "progress"


def load_progress(fieldPart):
    filePath = PROGRESS_FILE + str(fieldPart) + ".json"
    """加载爬取进度"""
    try:
        with open(filePath, "r", encoding="utf-8") as f:
            # print('文件打开成功')
            data = json.load(f)
            return data["last_field"], data["last_char"]
    except (FileNotFoundError, json.JSONDecodeError, KeyError):
        # print('文件打开失败')
        return None, None  # 首次运行时无进度


def save_progress(field, char, fieldPart):
    filePath = PROGRESS_FILE + str(fieldPart) + ".json"

    """保存当前进度"""
    with open(filePath, "w", encoding="utf-8") as f:
        json.dump({"last_field": field, "last_char": char}, f, ensure_ascii=False)


def clean_progress(fieldPart):
    filePath = PROGRESS_FILE + str(fieldPart) + ".json"
    """清理进度文件"""
    if os.path.exists(filePath):
        os.remove(filePath)
