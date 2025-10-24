import json
import os
import os
import json
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
                 start_index: int = None,
                 encoding: str = 'utf-8'):
        """
        base_filename: 不带后缀和编号的基础名，例如 "data" -> data_1.json
        max_bytes: 每个文件最大字节数，默认 70MB (70 * 1024 * 1024)
        start_index: 如果传 None，会在目录中查找已有的最大编号并从下一个编号开始
        """
        self.base = base_filename
        self.max_bytes = int(max_bytes)
        self.encoding = encoding

        # 决定从哪个编号开始
        if start_index is None:
            self.index = self._find_next_index()
        else:
            self.index = int(start_index)

        self.fh = None
        self.current_bytes = 0   # 已写入的字节数（不包括未写入的缓冲）
        self.is_first_in_file = True
        self._open_new_file()

    def _filename_for_index(self, idx):
        return f"{self.base}_{idx}.json"

    def _find_next_index(self):
        # 在当前目录查找已存在的 base_N.json，取得最大的 N，然后 +1
        base = self.base + "_"
        maxidx = 0
        for name in os.listdir('.'):
            if name.startswith(base) and name.endswith('.json'):
                mid = name[len(base):-5]  # 剥掉 base_ 和 .json
                if mid.isdigit():
                    i = int(mid)
                    if i > maxidx:
                        maxidx = i
        return maxidx + 1

    def _open_new_file(self):
        # 关闭旧的（如果有），然后打开新文件并写入数组开头
        if self.fh:
            self._close_file_internal()

        filename = self._filename_for_index(self.index)
        self.fh = open(filename, 'w', encoding=self.encoding)
        # 写入 JSON 数组的开头
        self.fh.write('[\n')
        self.fh.flush()
        # 计算当前字节数（基于写入的内容）
        # 这里直接测量文件实际大小以防之前有残留（更稳健）
        self.current_bytes = os.path.getsize(filename)
        self.is_first_in_file = True
        # print(f"Opened new file: {filename}")

    def _close_file_internal(self):
        # 在数组末尾写入换行加 ]，关闭句柄
        if not self.fh:
            return
        self.fh.write('\n]\n')
        self.fh.flush()
        filename = self.fh.name
        self.fh.close()
        # 更新 bytes（确保精确）
        self.current_bytes = os.path.getsize(filename)
        # print(f"Closed file: {filename}, size={self.current_bytes} bytes")
        self.fh = None

    def add(self, obj):
        """
        添加一个 JSON-可序列化的对象（例如 dict/list/str/number）
        会在写入前检查，如果写入后会超过 max_bytes，就先开启新文件。
        """
        if self.fh is None:
            self._open_new_file()

        # 将对象序列化成字符串（最小化占用）
        json_text = json.dumps(obj, ensure_ascii=False)  # 保留unicode，不转义
        # 需要写入的实际字节（考虑分隔符和换行）
        sep = '' if self.is_first_in_file else ',\n'
        bytes_to_add = len(sep.encode(self.encoding)) + len(json_text.encode(self.encoding)) + len('\n]'.encode(self.encoding))

        # 如果当前文件已有内容并且写入会超过限制，则关闭当前文件并开新文件
        if self.current_bytes + bytes_to_add > self.max_bytes:
            # 如果当前文件还是空文件（只有开头的 "[\n"），并且对象本身就超过限制，
            # 我们仍然要写入到新的文件中（单个对象超过阈值），在这里打印警告。
            if self.is_first_in_file and len(json_text.encode(self.encoding)) + len('[\n'.encode(self.encoding)) + len('\n]'.encode(self.encoding)) > self.max_bytes:
                print(f"Warning: single object size {len(json_text.encode(self.encoding))} bytes exceeds max_bytes {self.max_bytes}. It will be stored in its own file.")
                # 继续写入到当前（新开）文件 — 因为我们刚 open_new_file() 时 current was small.
            else:
                # 关闭当前文件并开启新的
                self._close_file_internal()
                self.index += 1
                self._open_new_file()
                # reset sep and recompute bytes_to_add for empty new file
                sep = ''
                bytes_to_add = len(sep.encode(self.encoding)) + len(json_text.encode(self.encoding)) + len('\n]'.encode(self.encoding))

        # 写入 sep + json_text
        if not self.is_first_in_file:
            self.fh.write(sep)
        self.fh.write(json_text)
        self.fh.flush()

        # 更新标志与字节计数
        self.is_first_in_file = False
        self.current_bytes += len(sep.encode(self.encoding)) + len(json_text.encode(self.encoding))

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
