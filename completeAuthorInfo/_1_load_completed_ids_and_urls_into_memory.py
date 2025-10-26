# 连接MySQL数据库
import mysql


def connect_to_mysql():
    return mysql.connector.connect(
        host="localhost",  # MySQL服务器地址
        user="root",  # MySQL用户名
        password="258456396ljt",  # MySQL密码
        database="academictree"  # 数据库名
    )

# 把所有ID加载到内存中的dict中
def load_completed_ids_and_urls_into_memory():
    conn = connect_to_mysql()
    cursor = conn.cursor()
    cursor.execute("SELECT author_id, author_url FROM author_pool")  # 查询 id 和 url
    data = cursor.fetchall()
    id_to_url = {id: url for id, url in data}  # 将 id 和 url 存储到字典中
    return id_to_url