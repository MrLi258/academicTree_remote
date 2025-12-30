import datetime
import json
import os
import json
import sys
import shutil
from traceback import print_tb

import mysql

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


class OldJsonFileSplitter:
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
        # # 在数组末尾写入换行加 ]，关闭句柄
        # if not self.fh:
        #     return
        # self.fh.write('\n]\n')
        # self.fh.flush()
        # filename = self.fh.name
        # self.fh.close()
        # self.current_bytes = os.path.getsize(filename)
        # self.fh = None

        # 在数组末尾写入换行加 ]，关闭句柄
        if not self.fh:
            return
        try:
            # 确保文件指针在末尾再写尾符
            try:
                self.fh.seek(0, os.SEEK_END)
            except Exception:
                pass
            # 避免重复写入 ']'：可以先检查文件末尾是否已经有 ']'（简单且快速）
            try:
                # 在文本模式下，读取最后一段来判断
                pos = self.fh.tell()
                # 读取一定长度的尾部文本（例如 16 字节）来判断是否已有结尾括号
                tail_len = min(1024, pos)
                if tail_len > 0:
                    # 回到末尾前 tail_len 字符位置
                    self.fh.seek(pos - tail_len)
                    tail = self.fh.read(tail_len)
                    # 如果尾部已经包含 ']' 则不再次写入
                    if tail.rstrip().endswith(']'):
                        # 已经有结尾，直接关闭
                        self.fh.flush()
                        self.fh.close()
                        self.current_bytes = os.path.getsize(self.fh.name)
                        self.fh = None
                        return
                    # 否则回到末尾准备写入
                    self.fh.seek(0, os.SEEK_END)
            except Exception:
                # 如果检查失败，也尽量写上结尾
                pass

            self.fh.write('\n]\n')
            self.fh.flush()
        finally:
            try:
                filename = self.fh.name
            except Exception:
                filename = None
            try:
                if self.fh:
                    self.fh.close()
            except Exception:
                pass
            self.fh = None
            if filename:
                try:
                    self.current_bytes = os.path.getsize(filename)
                except Exception:
                    self.current_bytes = 0



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

class JsonFileSplitter:
    """
    每行写入一个 JSON（NDJSON / JSONL），并按大小分割文件：
    文件名格式: {base}_{index}.jsonl  （你可以改成 .json）
    """
    def __init__(self, base_filename: str,
                 max_bytes: int = 70 * 1024 * 1024,
                 start_index: int = None,
                 encoding: str = 'utf-8'):
        self.base = base_filename
        self.max_bytes = int(max_bytes)
        self.encoding = encoding

        if start_index is None:
            self.index = self._find_last_index()
        else:
            self.index = int(start_index)

        self.fh = None
        self.current_bytes = 0
        self._open_file_for_append()

    def _filename_for_index(self, idx):
        return f"{self.base}_{idx}.jsonl"

    def _find_last_index(self):
        base_prefix = self.base + "_"
        max_idx = 0
        for name in os.listdir('.'):
            if name.startswith(base_prefix) and name.endswith('.jsonl'):
                mid = name[len(base_prefix):-6]  # remove prefix and .jsonl
                if mid.isdigit():
                    i = int(mid)
                    if i > max_idx:
                        max_idx = i
        return max_idx + 1

    def _open_file_for_append(self):
        """打开当前 index 的文件用于追加；如果已超出大小限制则新建文件。"""
        # 确保关闭旧句柄
        if self.fh:
            self._close_file_internal()

        filename = self._filename_for_index(self.index)
        if os.path.exists(filename):
            size = os.path.getsize(filename)
            if size < self.max_bytes:
                # 继续在此文件追加
                self.fh = open(filename, 'a', encoding=self.encoding, newline='\n')
                self.current_bytes = size
                return
            # 否则按下面逻辑新建文件（index 不变，_open below will create new with same index? we increment)
        # 新建文件（保证不会覆盖已有文件）
        # 如果目标名存在且已经满了，我们应该递增 index
        while os.path.exists(filename) and os.path.getsize(filename) >= self.max_bytes:
            self.index += 1
            filename = self._filename_for_index(self.index)
        # 打开新文件（append 模式也可以）
        self.fh = open(filename, 'a', encoding=self.encoding, newline='\n')
        self.current_bytes = os.path.getsize(filename)

    def _close_file_internal(self):
        if not self.fh:
            return
        try:
            self.fh.flush()
            # 尝试强制刷盘（可选）
            try:
                os.fsync(self.fh.fileno())
            except Exception:
                pass
            self.fh.close()
        finally:
            try:
                self.current_bytes = os.path.getsize(self.fh.name)
            except Exception:
                self.current_bytes = 0
            self.fh = None

    def add(self, obj):
        """
        将 obj 写成一行 JSON 并追加到当前文件，必要时切分新文件。
        """
        if self.fh is None:
            self._open_file_for_append()

        line = json.dumps(obj, ensure_ascii=False)
        line_with_nl = line + '\n'
        b = line_with_nl.encode(self.encoding)
        bytes_to_add = len(b)

        # 如果写入会超出限制，先切分文件（新开一个编号）
        if self.current_bytes + bytes_to_add > self.max_bytes:
            # 新文件编号递增
            self._close_file_internal()
            self.index += 1
            self._open_file_for_append()

        # 写入文本（文本模式不需要手动 encode）
        self.fh.write(line_with_nl)
        self.fh.flush()
        print(f'写入一个数据')
        try:
            os.fsync(self.fh.fileno())
        except Exception:
            pass
        self.current_bytes += bytes_to_add

    def close(self):
        """幂等关闭"""
        if self.fh:
            self._close_file_internal()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

# 合并之前不完全的数据
def merge_dicts(final_folder_path, supplement_folder_path):
    # 补充数据读取
    # supplement_folder_path = './temporaryAuthorJsons'
    supplement_dict = {}
    for filename in os.listdir(supplement_folder_path):
        if filename.endswith(".json"):
            file_path = os.path.join(supplement_folder_path, filename)

            shutil.copy(file_path, file_path + '.bak')  # 修改文件之前先备份文件，以免被覆盖使数据消失
            # 读取并解析JSON文件
            with open(file_path, 'r', encoding='utf-8') as file:
                data = json.load(file)

                # 假设data中是一个列表，我们要遍历列表中的每个元素
                for entry in data:
                    key, value = next(iter(entry.items()))
                    supplement_dict[key] = value


    # 用补充数据的父子合作节点信息替换目标文件中的对应信息
    # final_folder_path = './completeAuthorInfo'
    for filename in os.listdir(final_folder_path):
        if filename.endswith(".json"):
            file_path = os.path.join(final_folder_path, filename)

            shutil.copy(file_path, file_path + '.bak')  # 修改文件之前先备份文件，以免被覆盖使数据消失

            # 读取并解析JSON文件
            with open(file_path, 'r', encoding='utf-8') as file:
                data = json.load(file)

                # 假设data中是一个列表，我们要遍历列表中的每个元素
                for entry in data:
                    id = entry['id']
                    if bool(supplement_dict.get(id)):  # 把原始数据集的父子合作信息替换为补充数据的信息
                        if len(supplement_dict[id]['parentsIdList']) != 0 and entry.get('parentsIdList'):
                            if len(supplement_dict[id]['parentsIdList']) >= len(entry['parentsIdList']):
                                entry['parentsIdList'] = supplement_dict[id]['parentsIdList']
                        else:
                            entry['parentsIdList'] = supplement_dict[id]['parentsIdList']


                        if len(supplement_dict[id]['childrenIdList']) != 0 and entry.get('childrenIdList'):
                            if len(supplement_dict[id]['childrenIdList']) >= len(entry['childrenIdList']):
                                entry['childrenIdList'] = supplement_dict[id]['childrenIdList']
                        else:
                            entry['childrenIdList'] = supplement_dict[id]['childrenIdList']

                        entry['collaboratorsIdList'] = supplement_dict[id]['collaboratorsIdList']
            with open(file_path, 'w', encoding='utf-8') as file:
                json.dump(data, file, ensure_ascii=False, indent=4)


# 检查本轮需要爬取的作者数量
def calculate_nums_of_imminent_authors_in_this_round():
    completed_authors_set = set()
    imminent_author_set = set()
    imminent_dir_path = './completeAuthorInfo/temporaryAuthorJsons'  # './completeAuthorInfo/temporaryAuthorJsons'
    completed_dir_path = './completeAuthorInfo/completeAuthorJsons_backup'
    for filename in os.listdir(completed_dir_path):
        if filename.endswith(".json"):
            file_path = os.path.join(completed_dir_path, filename)

            # shutil.copy(file_path, file_path + '.bak')  # 修改文件之前先备份文件，以免被覆盖使数据消失
            # 读取并解析JSON文件
            with open(file_path, 'r', encoding='utf-8') as file:
                for line in file:
                    entry = json.loads(line)
                    completed_authors_set.add(entry['id'])


                                              #
    for filename in os.listdir(imminent_dir_path):
        if filename.endswith(".json"):
            file_path = os.path.join(imminent_dir_path, filename)

            # shutil.copy(file_path, file_path + '.bak')  # 修改文件之前先备份文件，以免被覆盖使数据消失
            # 读取并解析JSON文件
            with open(file_path, 'r', encoding='utf-8') as file:
                for line in file:
                    entry = json.loads(line)
                    if entry.get('parentsIdList'):
                        for parent in entry['parentsIdList']:
                            if parent[0] not in completed_authors_set:
                                imminent_author_set.add(parent[0])
                    if entry.get('collaboratorsIdList'):
                        for collaborator in entry['collaboratorsIdList']:
                            if collaborator[0] not in imminent_author_set:
                                imminent_author_set.add(collaborator[0])
                    if entry.get('childrenIdList'):
                        for child in entry['childrenIdList']:
                            if child[0] not in imminent_author_set:
                                imminent_author_set.add(child[0])

    print(len(imminent_author_set))

# 检查本轮已经爬取的作者数量
def calculate_nums_of_completed_authors_in_this_round():
    completed_this_round_authors_set = set()
    completed_this_round_dir_path = './completeAuthorInfo/newAuthorJsons/split'

    nums = 0
    for filename in os.listdir(completed_this_round_dir_path):
        if filename.endswith(".jsonl"):
            file_path = os.path.join(completed_this_round_dir_path, filename)

            # shutil.copy(file_path, file_path + '.bak')  # 修改文件之前先备份文件，以免被覆盖使数据消失
            # 读取并解析JSON文件
            with open(file_path, 'r', encoding='utf-8') as file:
                for line in file:
                    entry = json.loads(line)
                    nums += 1
                    completed_this_round_authors_set.add(entry['id'])

    print(len(completed_this_round_authors_set))
    print(f'未去重的数量是{nums}')
    print(datetime.datetime.now())

# 把已有json文件转化为jsonl文件
def convert_json2jsonl(base_dir):
    for filename in os.listdir(base_dir):
        if filename.endswith(".json"):
            file_path = os.path.join(base_dir, filename)
            shutil.copy(file_path, file_path + '.bak')  # 修改文件之前先备份文件，以免被覆盖使数据消失
            # 读取并解析JSON文件
            with open(file_path, 'r', encoding='utf-8') as rfile:
                with open(file_path.replace('.json', '.jsonl'), 'w', encoding='utf-8', newline='') as wfile:

                    data = json.load(rfile)
                    # 假设data中是一个列表，我们要遍历列表中的每个元素
                    for entry in data:
                        line = json.dumps(entry, ensure_ascii=False)
                        wfile.write(line+'\n')

# 尝试读取jsonl文件
def try_read_jsonl_files(base_dir):
    for filename in os.listdir(base_dir):
        if filename.endswith(".jsonl"):
            file_path = os.path.join(base_dir, filename)
            with open(file_path, 'r', encoding='utf-8') as rfile:
                for line in rfile:
                    # print(line[400:500])
                    entry = json.loads(line)
                    print(entry, type(entry))


def connect_to_mysql():
    return mysql.connector.connect(
        host="localhost",  # MySQL服务器地址
        user="root",  # MySQL用户名
        password="258456396ljt",  # MySQL密码
        database="academictree"  # 数据库名
    )

# 把所有指定字段从对应的表格中加载到内存中的dict中
def load_cols_from_table_into_memory(id_or_idWithField:bool, field_name_list, table_name='author_pool'):
    conn = connect_to_mysql()
    cursor = conn.cursor()
    try:
        key_col = 'id' if id_or_idWithField else 'field_pid'
        # 构建要查询的列，保证 key_col 在最前面
        cols = [key_col] + list(field_name_list)
        select_clause = ", ".join(cols)
        cursor.execute(f"SELECT {select_clause} FROM {table_name}")
        rows = cursor.fetchall()

        id_to_values = {}
        for row in rows:
            key = row[0]
            other = row[1:]
            if len(other) == 0:
                value = None
            elif len(other) == 1:
                # 只查询了一个附加字段，直接返回该值
                value = other[0]
            else:
                # 多个字段，按查询顺序返回元组
                value = tuple(other)
            id_to_values[key] = value

        return id_to_values
    finally:
        try:
            cursor.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass

if __name__ == '__main__':
    # merge_dicts('./completeAuthorInfo/completeAuthorJsons', './completeAuthorInfo/temporaryAuthorJsons')
    # calculate_nums_of_completed_authors_in_this_round()
    # convert_json2jsonl('./completeAuthorInfo/newAuthorJsons/split')
    # try_read_jsonl_files('./completeAuthorInfo/completeAuthorJsons')
    calculate_nums_of_completed_authors_in_this_round()
    ...

