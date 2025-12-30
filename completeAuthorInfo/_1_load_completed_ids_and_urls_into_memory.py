# 连接MySQL数据库
import mysql.connector


def connect_to_mysql():
    return mysql.connector.connect(
        host="localhost",  # MySQL服务器地址
        user="root",  # MySQL用户名
        password="258456396ljt",  # MySQL密码
        database="academictree"  # 数据库名
    )

# 把所有ID加载到内存中的dict中
def load_completed_ids_and_urls_into_memory(tale_name='author_pool'):
    conn = connect_to_mysql()
    cursor = conn.cursor()
    cursor.execute("SELECT author_id, author_url FROM author_pool")  # 查询 id 和 url
    data = cursor.fetchall()
    id_to_url = {id: url for id, url in data}  # 将 id 和 url 存储到字典中
    return id_to_url

def load_completed_idWithFields_into_memory():
    conn = connect_to_mysql()
    cursor = conn.cursor()
    cursor.execute("SELECT field_pid, author_url FROM author_pool_with_field")  # 查询 id
    data = cursor.fetchall()
    id_to_url = {id: url for id, url in data} # 将 id 存储到集合中
    return id_to_url

if __name__ == '__main__':
    print(len(load_completed_ids_and_urls_into_memory().items()))