'''
    在数据库中创建两张表格，
    一张以pid作为主键
    一张以field_pid作为主键
    判断现有数据的相对于两张表格各自的新的父子合作节点的数量是否一致

'''
import json
import os

from completeAuthorInfo._1_load_completed_ids_and_urls_into_memory import connect_to_mysql, \
    load_completed_ids_and_urls_into_memory


# 将数据插入数据库
def insert_into_author_pool_with_field(cursor, id, url):
    query = "INSERT INTO author_pool_with_field (field_pid, author_url) VALUES (%s, %s)"
    cursor.execute(query, (id, url))

# 把所有ID加载到内存中的dict中
def load_completed_field_ids_and_urls_into_memory():
    conn = connect_to_mysql()
    cursor = conn.cursor()
    cursor.execute("SELECT field_pid, author_url FROM author_pool_with_field")  # 查询 id 和 url
    data = cursor.fetchall()
    id_to_url = {id: url for id, url in data}  # 将 id 和 url 存储到字典中
    return id_to_url


def upload_date_info2table_author_pool_with_field():
    connection = connect_to_mysql()
    cursor = connection.cursor()
    folder_path = './completeAuthorJsons'

    # 获取数据库中已有的数据，避免主键冲突
    id2url = load_completed_field_ids_and_urls_into_memory()
    # print(len(id2url.items()))
    for filename in os.listdir(folder_path):
        if filename.endswith(".jsonl"):
            file_path = os.path.join(folder_path, filename)

            # 读取并解析JSON文件
            with open(file_path, 'r', encoding='utf-8') as file:
                for line in file:
                    entry = json.loads(line)
                    if 'id' in entry:
                        id = entry['id']
                        try:
                            url = entry['personal_url']
                        except Exception as e:
                            url = entry['baseHref']
                            url = url + 'peopleinfo.php?pid=' + str(id)
                        field = url.split('/')[-2]
                        field_pid = '_'.join([field, id])

                        if field_pid not in id2url:
                            print('_'.join([field, id]), url)
                            # 将id和url插入到数据库
                            insert_into_author_pool_with_field(cursor,field_pid , url)
                            id2url[field_pid] = url
                            # print(len(id2url.items()))
                            ...
    connection.commit()
    print(f'向数据库中插入{cursor.rowcount}条数据')

    cursor.close()
    connection.close()

def upload_date_info2table_author_pool_test():
    connection = connect_to_mysql()
    cursor = connection.cursor()
    folder_path = './completeAuthorJsons'

    # 获取数据库中已有的数据，避免主键冲突
    id2url = load_completed_field_ids_and_urls_into_memory()
    # print(len(id2url.items()))
    for filename in os.listdir(folder_path):
        if filename.endswith(".jsonl"):
            file_path = os.path.join(folder_path, filename)

            # 读取并解析JSON文件
            with open(file_path, 'r', encoding='utf-8') as file:
                for line in file:
                    entry = json.loads(line)
                    if 'id' in entry:
                        id = entry['id']
                        try:
                            url = entry['personal_url']
                        except Exception as e:
                            url = entry['baseHref']
                            url = url + 'peopleinfo.php?pid=' + str(id)


                        if id not in id2url:
                            # 将id和url插入到数据库
                            query = "INSERT INTO author_pool_test (author_id, author_url) VALUES (%s, %s)"
                            cursor.execute(query, (id, url))
                            id2url[id] = url
                            # print(len(id2url.items()))
                            ...
    connection.commit()
    print(f'向数据库中插入{cursor.rowcount}条数据')

    cursor.close()
    connection.close()

def test_valid_uniqueness_of_pid():
    # 记录所有已经完成的作者ID，避免重复爬取(这个去重主要是在某一次迭代处于未完成状态重新启动时防止该次迭代过程中之前爬取的数据重复，也就是提升爬取速率，不重复爬取)
    completedTemporaryAuthors_with_field = dict()
    for filename in os.listdir('./newAuthorJsons/split'):
        if filename.endswith(".jsonl"):
            file_path = os.path.join('./newAuthorJsons/split', filename)
            # 读取并解析JSON文件
            with open(file_path, 'r', encoding='utf-8') as file:
                for line in file:
                    entry = json.loads(line)
                    # 假设data中是一个列表，我们要遍历列表中的每个元素
                    if 'id' in entry:
                        author_id = entry['id']
                        try:
                            url = entry['personal_url']
                        except Exception as e:
                            url = entry['baseHref']
                            url = url + 'peopleinfo.php?pid=' + str(author_id)
                        field = url.split('/')[-2]
                        field_pid = '_'.join([field, author_id])
                        completedTemporaryAuthors_with_field[field_pid] = url

    completed_id_to_url_with_field = load_completed_field_ids_and_urls_into_memory()
    completed_id_to_url_with_field.update(completedTemporaryAuthors_with_field)  # 把数据库和文件夹中以爬取的作者汇总用于后续去重
    print(f'现有作者数量：{len(completed_id_to_url_with_field.items())}<UNK>')


    # 读取将要爬取的new_authors
    newAuthors_with_field = dict()
    for filename in os.listdir('./completeAuthorJsons'):
        if filename.endswith(".jsonl"):
            file_path = os.path.join('./completeAuthorJsons', filename)
            # 读取并解析JSON文件
            with open(file_path, 'r', encoding='utf-8') as file:
                for line in file:
                    entry = json.loads(line)
                    # 把该作者所有的父子合作节点添加进将要爬取的作者中
                    if parentsIdList := entry.get('parentsIdList'):
                        for parent in parentsIdList:
                            try:
                                field = parent[1].split('/')[-2]
                                field_pid = '_'.join([field, parent[0]])
                                newAuthors_with_field[field_pid] = parent[1]
                            except Exception as e:
                                ...

                    if childrenIdList := entry.get('childrenIdList'):
                        for child in childrenIdList:
                            try:
                                field = child[1].split('/')[-2]
                                field_pid = '_'.join([field, child[0]])
                                newAuthors_with_field[field_pid] = child[1]
                            except Exception as e:
                                ...
                    if collaboratorsIdList := entry.get('collaboratorsIdList'):
                        for collaborator in collaboratorsIdList:
                            try:
                                field = collaborator[1].split('/')[-2]
                                field_pid = '_'.join([field, collaborator[0]])
                                newAuthors_with_field[field_pid] = collaborator[1]
                            except Exception as e:
                                ...
    newAuthors_with_field = {k: v for k, v in newAuthors_with_field.items() if k not in completed_id_to_url_with_field}
    print(f'带领域的新人数量是：{len(newAuthors_with_field.items())}<UNK>')


    # 查找不同领域同一id的作者
    different_field_seam_pid = dict()
    for newAuthor_with_field in newAuthors_with_field:
        new_field = newAuthor_with_field.split('_')[0]
        new_id = newAuthor_with_field.split('_')[1]
        different_field_seam_pid[new_id] = []
        different_field_seam_pid[new_id].append(newAuthor_with_field)
        print(newAuthors_with_field[newAuthor_with_field])
        for completedAuthor_with_field in completed_id_to_url_with_field:
            field = completedAuthor_with_field.split('_')[0]
            id = completedAuthor_with_field.split('_')[1]
            if id == new_id and field != new_field:
                different_field_seam_pid[id].append(completedAuthor_with_field)
                print(f'找到一个相同id不同领域的作者{completed_id_to_url_with_field[completedAuthor_with_field]}')

    for id in different_field_seam_pid:
        if len(different_field_seam_pid[id]) > 1:
            print(different_field_seam_pid[id])
    # ------------------------------------------------------------------------------------------------------------------------------------------------------------
    # 记录所有已经完成的作者ID，避免重复爬取(这个去重主要是在某一次迭代处于未完成状态重新启动时防止该次迭代过程中之前爬取的数据重复，也就是提升爬取速率，不重复爬取)
    completedTemporaryAuthors = dict()
    for filename in os.listdir('./newAuthorJsons/split'):
        if filename.endswith(".jsonl"):
            file_path = os.path.join('./newAuthorJsons/split', filename)
            # 读取并解析JSON文件
            with open(file_path, 'r', encoding='utf-8') as file:
                for line in file:
                    entry = json.loads(line)
                    # 假设data中是一个列表，我们要遍历列表中的每个元素
                    if 'id' in entry and 'personal_url' in entry:
                        author_id = entry['id']
                        author_url = entry['personal_url']
                        completedTemporaryAuthors[author_id] = author_url

    completed_id_to_url = load_completed_ids_and_urls_into_memory()
    completed_id_to_url.update(completedTemporaryAuthors)  # 把数据库和文件夹中以爬取的作者汇总用于后续去重
    print(f'现有作者数量：{len(completed_id_to_url.items())}<UNK>')


    # 读取将要爬取的new_authors
    newAuthors = dict()
    for filename in os.listdir('./completeAuthorJsons'):
        if filename.endswith(".jsonl"):
            file_path = os.path.join('./completeAuthorJsons', filename)
            # 读取并解析JSON文件
            with open(file_path, 'r', encoding='utf-8') as file:
                for line in file:
                    entry = json.loads(line)
                    # 把该作者所有的父子合作节点添加进将要爬取的作者中
                    if parentsIdList := entry.get('parentsIdList'):
                        for parent in parentsIdList:
                            newAuthors[parent[0]] = parent[1]
                    if childrenIdList := entry.get('childrenIdList'):
                        for child in childrenIdList:
                            newAuthors[child[0]] = child[1]
                    if collaboratorsIdList := entry.get('collaboratorsIdList'):
                        for collaborator in collaboratorsIdList:
                            newAuthors[collaborator[0]] = collaborator[1]
    newAuthors = {k:v for k,v in newAuthors.items() if k not in completed_id_to_url}
    print(f'新人数量是：{len(newAuthors.items())}<UNK>')
if __name__ == '__main__':
    # upload_date_info2table_author_pool_with_field()
    # upload_date_info2table_author_pool_test()
    test_valid_uniqueness_of_pid()