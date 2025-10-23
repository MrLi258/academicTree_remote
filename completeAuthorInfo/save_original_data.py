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


# 将数据插入数据库
def insert_into_author_pool(cursor, id, url):
    query = "INSERT INTO author_pool (author_id, author_url) VALUES (%s, %s)"
    cursor.execute(query, (id, url))


# 处理文件夹中的所有JSON文件
def process_json_files(folder_path, cursor):
    for filename in os.listdir(folder_path):
        if filename.endswith(".json"):
            file_path = os.path.join(folder_path, filename)

            # 读取并解析JSON文件
            with open(file_path, 'r', encoding='utf-8') as file:
                data = json.load(file)

                # 假设data中是一个列表，我们要遍历列表中的每个元素
                for entry in data:
                    if 'id' in entry and 'baseHref' in entry:
                        id = entry['id']
                        base_href = entry['baseHref']

                        # 生成新的URL
                        url = f"{base_href}peopleinfo.php?pid={id}"

                        # 将id和url插入到数据库
                        insert_into_author_pool(cursor, id, url)


# 主程序
def main():
    # 文件夹路径
    folder_path = './completeAuthorJsons'

    # 连接MySQL数据库
    connection = connect_to_mysql()
    cursor = connection.cursor()

    try:
        # 处理文件夹中的所有JSON文件
        process_json_files(folder_path, cursor)

        # 提交事务
        connection.commit()
        print("数据成功插入到author_pool表格中。")
    except Exception as e:
        print(f"发生错误: {e}")
        connection.rollback()
    finally:
        # 关闭数据库连接
        cursor.close()
        connection.close()


if __name__ == "__main__":
    main()
