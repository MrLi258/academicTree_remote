import json
import os

import mysql.connector
import time
import random

# 连接MySQL数据库
def connect_to_mysql():
    return mysql.connector.connect(
        host="localhost",  # MySQL服务器地址
        user="root",  # MySQL用户名
        password="258456396ljt",  # MySQL密码
        database="academictree"  # 数据库名
    )

# 方案一：把所有ID加载到内存中的set
def load_ids_and_urls_into_memory(cursor):
    cursor.execute("SELECT author_id, author_url FROM author_pool")  # 查询 id 和 url
    data = cursor.fetchall()
    id_to_url = {id: url for id, url in data}  # 将 id 和 url 存储到字典中
    return id_to_url

def check_in_memory(id_to_url, new_id):
    # 判断新ID是否存在，并返回url（如果存在）
    return new_id in id_to_url, id_to_url.get(new_id)

# 方案二：每次都查询数据库
def check_in_db(cursor, new_id):
    query = "SELECT EXISTS(SELECT 1 FROM author_pool WHERE author_id = %s)"
    cursor.execute(query, (new_id,))
    result = cursor.fetchone()
    return bool(result[0])

# 主程序：对比执行时间
def compare_methods():
    # 连接到数据库
    conn = connect_to_mysql()
    cursor = conn.cursor()

    # 获取数据库中的所有ID并加载到内存中
    print("加载所有ID到内存中...")
    id_set = load_ids_and_urls_into_memory(cursor)
    print(f"已加载 {len(id_set)} 条ID到内存中。")

    # 模拟1000次新的ID
    new_ids = [random.randint(1, 1000000) for _ in range(1000)]  # 模拟1000个新的ID

    # 方案一：内存查找
    start_time = time.time()
    for new_id in new_ids:
        check_in_memory(id_set, new_id)
    memory_time = time.time() - start_time
    print(f"方案一：内存查找耗时 {memory_time:.4f} 秒")

    # 方案二：数据库查找
    start_time = time.time()
    for new_id in new_ids:
        check_in_db(cursor, new_id)
    db_time = time.time() - start_time
    print(f"方案二：数据库查找耗时 {db_time:.4f} 秒")

    # 关闭连接
    cursor.close()
    conn.close()


def get_parents_and_children_set(folder_path):
    total_parents_and_children_set = set()
    for filename in os.listdir(folder_path):
        if filename.endswith(".json"):
            file_path = os.path.join(folder_path, filename)

            # 读取并解析JSON文件
            with open(file_path, 'r', encoding='utf-8') as file:
                data = json.load(file)

                # 假设data中是一个列表，我们要遍历列表中的每个元素
                for entry in data:
                    if 'parentsIdList' in entry:
                        if entry['parentsIdList']:
                            for meta in entry['parentsIdList']:
                                total_parents_and_children_set.add(meta)
                    if 'childrenIdList' in entry:
                        if entry['childrenIdList']:
                            for meta in entry['childrenIdList']:
                                total_parents_and_children_set.add(meta)
    print(f'目前总共需要添加的所有父子节点为数量为：{len(total_parents_and_children_set)} <UNK>')
    return total_parents_and_children_set



# 执行对比
if __name__ == "__main__":
    # conn = connect_to_mysql()
    # cursor = conn.cursor()
    # id_to_url = load_ids_and_urls_into_memory(cursor)
    # print(check_in_memory(id_to_url ,'5067'))

    get_parents_and_children_set('./completeAuthorJsons/')

