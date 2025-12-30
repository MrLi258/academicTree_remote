import json
import os
from multiprocessing import get_context, Process, Queue, Manager
from _2_get_and_save_new_pcc_info_from_old_info import process_writer
import mysql.connector

from completeAuthorInfo._1_load_completed_ids_and_urls_into_memory import load_completed_ids_and_urls_into_memory,load_completed_idWithFields_into_memory


def move_files_from_new2temp():
    # 获取数据库中已有的数据，避免主键冲突
    # id2url = {}
    idWithField2url = load_completed_idWithFields_into_memory()
    print(len(idWithField2url.items()))
    # 使用 Manager 创建共享的队列
    with Manager() as manager:
        data_queue = manager.Queue()  # 通过 Manager 创建的 Queue 是可以跨进程共享的
        # 启动主进程的写入线程
        base_name = f'./temporaryAuthorJsons/temporaryAuthorInfo'  # 写入的文件夹以及文件名
        writer_process = Process(target=process_writer, args=(data_queue, 1, base_name))
        writer_process.start()

        for filename in os.listdir('./newAuthorJsons/split'):
            if filename.endswith(".jsonl"):
                file_path = os.path.join('./newAuthorJsons/split', filename)
                # 读取并解析JSON文件
                with open(file_path, 'r', encoding='utf-8') as file:
                    for line in file:
                        entry = json.loads(line)
                        if 'idWithField'  in entry and 'personal_url' in entry:
                            idWithField = entry['idWithField']
                            # print(f'id={id}')
                            if idWithField not in idWithField2url:  # 把本轮重复爬取的信息去除，下一轮不再爬取
                                # 把该作者所有的父子合作节点添加进将要爬取的作者中
                                data_queue.put(entry)  # 往输送管道内传输数据，传输管道送到writer_process进程内把数据写入到base_name的文件夹内
                                idWithField2url[idWithField] = entry['personal_url']  # 动态去重

        # 数据全部传输完之后通知writer_process进程
        data_queue.put('DONE')
        writer_process.join()


# 连接MySQL数据库
def connect_to_mysql():
    return mysql.connector.connect(
        host="localhost",  # MySQL服务器地址
        user="root",  # MySQL用户名
        password="258456396ljt",  # MySQL密码
        database="academictree"  # 数据库名
    )


# 将数据插入数据库
def insert_into_author_pool_with_field(cursor, idWithField, url):
    query = "INSERT INTO author_pool_with_field (field_pid, author_url) VALUES (%s, %s)"
    cursor.execute(query, (idWithField, url))




def upload_temporary_info2MySQL():
    connection = connect_to_mysql()
    cursor = connection.cursor()
    folder_path = './temporaryAuthorJsons'

    # 获取数据库中已有的数据，避免主键冲突
    idWithField2url = load_completed_idWithFields_into_memory()
    for filename in os.listdir(folder_path):
        if filename.endswith(".jsonl"):
            file_path = os.path.join(folder_path, filename)

            # 读取并解析JSON文件
            with open(file_path, 'r', encoding='utf-8') as file:
                for line in file:
                    entry = json.loads(line)
                    if 'idWithField' in entry and 'personal_url' in entry:
                        idWithField = entry['idWithField']
                        personal_url = entry['personal_url']
                        if idWithField not in idWithField2url:
                            # 将id和url插入到数据库
                            insert_into_author_pool_with_field(cursor, idWithField, personal_url)
                            idWithField2url[idWithField] = personal_url
    connection.commit()
    print(f'向数据库中插入{cursor.rowcount}条数据')

    cursor.close()
    connection.close()

if __name__ == '__main__':
    move_files_from_new2temp()
    upload_temporary_info2MySQL()