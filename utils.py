import json
import os
import os
import json
import sys
PROGRESS_FILE = "progress"

# 获取项目根目录（根据实际项目结构可能需要调整路径层级）
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# 将项目根目录添加到模块搜索路径
if project_root not in sys.path:
    sys.path.insert(0, project_root)


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

# 拆分字典
def split_dict_equal(original_dict, num_parts):
    """
    将字典均等拆分成指定数量的子字典
    """
    items = list(original_dict.items())
    total_items = len(items)

    # 计算每个子字典应该包含的大致项目数
    chunk_size = total_items // num_parts
    remainder = total_items % num_parts

    result = []
    start = 0

    for i in range(num_parts):
        # 处理余数，让前面的字典多一个元素
        end = start + chunk_size + (1 if i < remainder else 0)
        result.append(dict(items[start:end]))
        start = end

    return result


class JsonFileSplitter:
    def __init__(self, base_filename: str,
                 max_bytes: int = 70 * 1024 * 1024,
                 encoding: str = 'utf-8'):
        """
        base_filename: 不带后缀和编号的基础名，例如 "data" -> data_1.json
        max_bytes: 每个文件最大字节数，默认 70MB (70 * 1024 * 1024)
        """
        self.base = base_filename
        self.max_bytes = int(max_bytes)
        self.encoding = encoding

        # 判断当前目录是否已经有符合命名规则的文件，如果有就接着往最后一个文件写入
        self.index = self._find_last_index()

        self.fh = None
        self.current_bytes = 0   # 已写入的字节数（不包括未写入的缓冲）
        self.is_first_in_file = True
        self._open_file()

    def _filename_for_index(self, idx):
        return f"{self.base}_{idx}.json"

    def _find_last_index(self):
        # 在当前目录查找所有符合 base_N.json 的文件，返回最后一个文件的编号（不超出 max_bytes 的情况下）
        base = self.base + "_"
        max_idx = 0
        for name in os.listdir('.'):
            if name.startswith(base) and name.endswith('.json'):
                mid = name[len(base):-5]  # 剥掉 base_ 和 .json
                if mid.isdigit():
                    idx = int(mid)
                    file_size = os.path.getsize(self._filename_for_index(idx))
                    # 如果当前文件小于 max_bytes，继续写入
                    if file_size < self.max_bytes:
                        return idx  # 返回当前文件编号
                    if idx > max_idx:
                        max_idx = idx
        return max_idx + 1  # 如果没有符合条件的文件，则新建一个编号

    def _open_file(self):
        # 关闭旧的（如果有），然后打开新文件并写入数组开头
        if self.fh:
            self._close_file_internal()

        filename = self._filename_for_index(self.index)

        # 如果文件存在且文件大小未超过限制
        if os.path.exists(filename) and os.path.getsize(filename) < self.max_bytes:
            # 以读写模式打开文本文件
            self.fh = open(filename, 'r+', encoding=self.encoding)

            # 读取文件内容到末尾前一部分
            self.fh.seek(0, os.SEEK_END)
            if self.fh.tell() > 0:  # 文件不为空
                # 先读取最后一个字符
                self.fh.seek(self.fh.tell() - 1)
                last_char = self.fh.read(1)
                if last_char == ']':
                    print("检测到末尾的]符号，去掉它")
                    # 截断最后一个字符
                    self.fh.seek(self.fh.tell() - 1)
                    self.fh.truncate()

            # 重新定位到文件末尾，准备写入
            self.fh.seek(0, os.SEEK_END)
            self.is_first_in_file = False


        else:
            # 如果文件不存在或已超出最大字节限制，使用 'w' 模式创建新文件
            self.fh = open(filename, 'w', encoding=self.encoding)
            self.fh.write('[\n')  # 写入 JSON 数组的开头

        self.fh.flush()
        self.current_bytes = os.path.getsize(filename)
        self.is_first_in_file = False

    def _close_file_internal(self):
        # 在数组末尾写入换行加 ]，关闭句柄
        if not self.fh:
            return
        self.fh.write('\n]\n')
        self.fh.flush()
        filename = self.fh.name
        self.fh.close()
        self.current_bytes = os.path.getsize(filename)
        self.fh = None

    def add(self, obj):
        """
        添加一个 JSON-可序列化的对象（例如 dict/list/str/number）
        会在写入前检查，如果写入后会超过 max_bytes，就先开启新文件。
        """
        if self.fh is None:
            self._open_file()

        # 将对象序列化成字符串（最小化占用）
        json_text = json.dumps(obj, ensure_ascii=False)  # 保留unicode，不转义

        # 需要写入的实际字节（考虑分隔符和换行）
        sep = '' if self.is_first_in_file else ',\n'

        # 将分隔符和 json_text 转换为字节
        sep_bytes = sep
        json_text_bytes = json_text

        bytes_to_add = len(sep_bytes) + len(json_text_bytes) + len('\n]')

        # 如果当前文件已有内容并且写入会超过限制，则关闭当前文件并开新文件
        if self.current_bytes + bytes_to_add > self.max_bytes:
            # 如果当前文件还是空文件（只有开头的 "[\n"），并且对象本身就超过限制，
            # 我们仍然要写入到新的文件中（单个对象超过阈值），在这里打印警告。
            if self.is_first_in_file and len(json_text_bytes) + len('[\n') + len(
                    '\n]') > self.max_bytes:
                print(
                    f"Warning: single object size {len(json_text_bytes)} bytes exceeds max_bytes {self.max_bytes}. It will be stored in its own file.")
                # 继续写入到当前（新开）文件 — 因为我们刚 open_new_file() 时 current was small.
            else:
                # 关闭当前文件并开启新的
                self._close_file_internal()
                self.index += 1
                self._open_file()
                # reset sep and recompute bytes_to_add for empty new file
                sep_bytes = b''  # 新文件的分隔符
                bytes_to_add = len(sep_bytes) + len(json_text_bytes) + len('\n]'.encode(self.encoding))

        # 写入 sep + json_text（都转换为字节）
        if not self.is_first_in_file:
            self.fh.write(sep_bytes)

        self.fh.write(json_text_bytes)
        self.fh.flush()

        # 更新标志与字节计数
        self.is_first_in_file = False
        self.current_bytes += len(sep_bytes) + len(json_text_bytes)

    def close(self):
        """显式关闭当前文件并写入结尾符号（必须调用以保证 JSON 合法）"""
        if self.fh:
            self._close_file_internal()
            self.fh = None

    # 为了确保进程结束时自动写入结尾，可支持上下文管理器
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

if __name__ == '__main__':
    # 示例使用
    big_dict = {f'key_{i}': f'value_{i}' for i in range(100)}
    parts = split_dict_equal(big_dict, 7)

    for i, part in enumerate(parts):
        print(f"第{i + 1}部分: {len(part)}个元素， {part}")
