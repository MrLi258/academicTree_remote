import os
import json
import mysql.connector


# 连接MySQL数据库
def connect_to_mysql():
    return mysql.connector.connect(
        host="localhost",  # MySQL服务器地址
        user="root",  # MySQL用户名
        password="258456396ljt",  # MySQL密码
        database="academictree"  # 数据库名
    )


#
def check_author_exists(cursor, author_id):

    # 查询语句（使用参数化查询）
    query = "SELECT EXISTS(SELECT 1 FROM author_pool WHERE id = %s)"
    cursor.execute(query, (author_id,))

    # 获取结果
    exists = cursor.fetchone()[0]  # 返回1或0

    # 返回布尔值
    return bool(exists)

