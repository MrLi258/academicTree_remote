import atexit
import os
import json
import random
import shutil
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor
from multiprocessing import get_context, Process, Queue, Manager

import pyautogui
from DrissionPage import ChromiumPage
import mysql.connector
from DrissionPage._configs.chromium_options import ChromiumOptions

from utils import JsonFileSplitter
from utils import split_dict_equal
MAX_THREAD = 2
MAX_PROCESSORS = 8
# 连接MySQL数据库
def connect_to_mysql():
    return mysql.connector.connect(
        host="localhost",  # MySQL服务器地址
        user="root",  # MySQL用户名
        password="258456396ljt",  # MySQL密码
        database="academictree"  # 数据库名
    )


# 获取所有已有作者的所有父子节点集合  (该集合需要在修补完之前的所有数据后才能使用，也就是指把父子列表中的单id数据变为（id，url）数据
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
    return total_parents_and_children_set



#
def check_author_exists(cursor, author_id):
    # 查询语句（使用参数化查询）
    query = "SELECT EXISTS(SELECT 1 FROM author_pool WHERE id = %s)"
    cursor.execute(query, (author_id,))

    # 获取结果
    exists = cursor.fetchone()[0]  # 返回1或0

    # 返回布尔值
    return bool(exists)

#
def getParentsAndChildrenInfo(browser, lock_new_tab, author_url):

    # 参数: {'locator': 'css:td:nth-child(1) a', 'index': 1, 'timeout': 10}
    with lock_new_tab:
        tab = browser.new_tab('about:blank')
    tab.get(author_url)  # 这句代码放不放在with上下文管理器中非常重要，直接影响到tab是否能并发，如果方法with内，多线程直接并发不了了就
    # 初始化一个空字典,记录新增的用户信息
    data_dict = {}
    try:
        while (True):
            html = tab.html
            if "Please complete the security check to proceed" in html:
                print('检测到有人机检验')
                time.sleep(random.uniform(1, 3))
            else:
                break
        # time.sleep(100)
        # 获取parentsIdList
        parentsRow = tab.eles("css:.leftcol table:nth-child(1) tr")
        parentsIdList = []
        if parentsRow is not None:
            for tr in parentsRow:
                parentUrl = tr.ele('css:td:nth-child(1) a').attr("href")
                parentId = parentUrl.split("=")[-1]
                # print(f'找到一个父节点id：{parentId}')
                parentsIdList.append((parentUrl, parentId))
        data_dict["parentsIdList"] = parentsIdList
        # 获取childrenIdList
        childrenRow = tab.eles("css:.leftcol table:nth-child(2)  tr")
        childrenIdList = []
        if childrenRow is not None:
            for tr in childrenRow:
                childUrl = tr.ele('css:td:nth-child(1) a').attr("href")
                childId = childUrl.split("=")[-1]
                # print(f'找到一个子节点id：{childId}')
                childrenIdList.append((childUrl, childId))
        data_dict["childrenIdList"] = childrenIdList
    except Exception as e:
        print(e)
        print('我已暂停当前线程')
        time.sleep(10000)
    finally:
        time.sleep(random.uniform(2,4))
        tab.close()
        # print(f'返回一个{data_dict}')
        return data_dict

# 给定个人主页的链接爬取完善 个人信息
def getCompleteAuthorInfo(browser, lock_new_tab, author_url):
    with lock_new_tab:
        tab = browser.new_tab('about:blank')
        tab.get(author_url)
    while (True):
        html = tab.html
        if "Please complete the security check to proceed" in html:
            print('检测到有人机检验')
            time.sleep(2)
        else:
            break
    # input(f'暂停检查网站，输入任意字符继续')
    # 初始化一个空字典,记录新增的用户信息
    data_dict = {}

    # 获取parentsIdList
    parentsRow = tab.eles("css:.leftcol table:nth-child(1) tr")
    parentsIdList = []
    if parentsRow is not None:
        for tr in parentsRow:
            parentUrl = tr.ele('css:td:nth-child(1) a').attr("href")
            parentId = parentUrl.split("=")[-1]
            parentsIdList.append(parentUrl, parentId)
    data_dict["parentsIdList"] = parentsIdList
    # 获取childrenIdList
    childrenRow = tab.eles("css:.leftcol table:nth-child(2)  tr")
    childrenIdList = []
    if childrenRow is not None:
        for tr in childrenRow:
            childUrl = tr.ele('css:td:nth-child(1) a').attr("href")
            childId = childUrl.split("=")[-1]
            childrenIdList.append(childUrl, childId)
    data_dict["childrenIdList"] = childrenIdList
    print(f'第一步获取父子节点陈成功{data_dict}')
    # 获取个人基本信息
    try:
        personInf_tag = tab.ele('css:.personinfo')
        whole_name = personInf_tag.ele('css:h1').text.strip()
        whole_info = personInf_tag.text.replace(whole_name, '').strip().replace('\n', '')
        # print('whole_info:', whole_info)
        h5_eles = personInf_tag.eles('css:h5')
        start_idx = 0
        end_idx = 0
        if len(h5_eles) > 2:
            for index, h5_ele in enumerate(h5_eles[:-1]):
                start_idx = whole_info.find(h5_ele.text)
                end_idx = whole_info.find(h5_eles[index + 1].text)
                if start_idx != -1 and end_idx != -1:
                    data_dict[h5_ele.text] = whole_info[start_idx + len(h5_ele.text):end_idx]
                    # print(f"{h5_ele.text}:{whole_info[start_idx + len(h5_ele.text):end_idx]}")
            data_dict[h5_eles[-1].text] = whole_info[end_idx + len(h5_eles[-1].text):len(whole_info)]
            # print(f"{h5_eles[-1].text}:{whole_info[end_idx + len(h5_eles[-1].text):len(whole_info)]}")
        elif len(h5_eles) == 2:
            start_idx = whole_info.find(h5_eles[0].text)
            end_idx = whole_info.find(h5_eles[1].text)
            data_dict[h5_eles[0].text] = whole_info[start_idx + len(h5_eles[0].text):end_idx]
            data_dict[h5_eles[-1].text] = whole_info[end_idx + len(h5_eles[-1].text):len(whole_info)]
        elif len(h5_eles) == 1:
            data_dict[h5_eles[0].text] = whole_info[len(h5_eles[-1].text):len(whole_info)]
    except Exception as e:
        print(f'在基础信息提取部分出问题的url是：{author_url}')
        print(e)
        tab.close()
        return (-1, data_dict)
    print(f'第二步获取个人信息陈成功{data_dict}')


    # 获取文章信息
    publication_url = author_url.replace('peopleinfo', 'publications')
    publicationEles = tab.eles('css:.rightcol .container tbody .clickable-row')
    # print(f'该作者有{len(publicationEles)}篇文章')
    if len(publicationEles) > 0:
        # 如果有论文，则跳转到到publication详情页爬取
        tab.get(publication_url)
    else:
        tab.close()
        return (200, data_dict)

    publications = []
    tr_list = tab.eles('css:.table-body tbody tr')
    if len(tr_list) > 0:
        for tr in tr_list[2:-1]:
            temp_dict = {}
            year = tr.ele('css:td:nth-child(1)').text.strip()
            name = tr.ele('css:td:nth-child(2)').text.strip()
            score = tr.ele('css:td:nth-child(3)').text.strip()
            temp_dict['year'] = year
            temp_dict['name'] = name
            temp_dict['score'] = score
            publications.append(temp_dict)
        data_dict['publications'] = publications
        print(f'第三步获取个人信息成功{data_dict}')
    tab.close()
    return (200, data_dict)

# 把所有ID加载到内存中的dict中
def load_ids_and_urls_into_memory(cursor):
    cursor.execute("SELECT author_id, author_url FROM author_pool")  # 查询 id 和 url
    data = cursor.fetchall()
    id_to_url = {id: url for id, url in data}  # 将 id 和 url 存储到字典中
    return id_to_url

def check_in_memory(id_to_url, new_id):
    # 判断新ID是否存在，并返回url（如果存在）
    return new_id in id_to_url

# 获取新父子节点的单进程
def crawler_authors_by_exist_parents_and_children_singleProcess(batch, proc_idx, id_to_url, data_queue):
    co = ChromiumOptions()
    # co.set_argument('--headless=new')
    # 临时目录，一进程一份
    profile_dir = os.path.join(tempfile.gettempdir(),
                               f"dp_profile_{os.getpid()}_{proc_idx}")
    print('profile_dir', profile_dir)
    os.makedirs(profile_dir, exist_ok=True)
    co.set_argument(f'--user-data-dir={profile_dir}')
    co.set_local_port(9300 + proc_idx)  #
    browser = ChromiumPage(co)

    # 根据屏幕大小以及进程数量设置浏览器窗口的大小
    screen_width, screen_height = pyautogui.size()
    width = int(screen_width / (MAX_PROCESSORS ** 0.5))
    height = int(screen_height / (MAX_PROCESSORS ** 0.5))
    browser.set.window.size(width, height)


    # 这里先行访问一个主页是为了手动过一次人机验证
    temp_tab = browser.new_tab('')
    temp_tab.get('http://academictree.org/biomech/peopleinfo.php?pid=238154')
    time.sleep(20)
    temp_tab.close()

    all_url = list(batch.values())
    home_tab = browser.new_tab('about:blank')
    home_tab.get(all_url[0])
    time.sleep(10)  # 等待10秒手动过人机校验
    home_tab.close()

    lock_new_tab = threading.Lock()  # 保护 br.new_tab()
    try:
        with ThreadPoolExecutor(max_workers=MAX_THREAD) as ex:
            futs = [ex.submit(getParentsAndChildrenInfo, browser, lock_new_tab, url) for url in all_url]
            for fut in as_completed(futs):
                dic = fut.result()
                if dic:
                    if (parentsIdList:=dic.get('parentsIdList', [])) or (childrenIdList:=dic.get('childrenIdList', [])):
                        print(f'接收到一个非空数据：{dic}')
                        # new_data = []
                        # 添加新的父子节点
                        for parentUrl, parentId in parentsIdList:
                            # flag = check_in_memory(id_to_url, parentId)
                            # if not flag:
                                parentNode = {parentId: parentUrl}
                                print(f'向队列中添加数据：{parentNode}')
                                data_queue.put(parentNode)

                                # new_data.append(parentNode)
                                # print(f'找到一个新的父作者{parentNode}')
                        for childUrl, childrenId in childrenIdList:
                            # flag = check_in_memory(id_to_url, childrenId)
                            # if not flag:
                                childrenNode = {childrenId:childUrl}
                                print(f'向队列中添加数据：{childrenNode}')
                                # new_data.append(childrenNode)
                                # print(f'找到一个新的子作者{childrenNode}')
                        # if new_data:
                        #     print(f'向队列中添加数据：{new_data}')
                        #     data_queue.put(new_data)
    finally:
        browser.quit()
        # 注册退出时自动删除
        atexit.register(lambda: shutil.rmtree(profile_dir, ignore_errors=True))

    # 发送完成信号给写入进程
    data_queue.put('DONE')  # 发送“DONE”标识该进程已完成

# 一个专门写入数据的线程，避免多线程同时写入数据时容易造成的错误
def process_writer(data_queue, num_workers):
    splitter = JsonFileSplitter("./temporaryAuthorJsons/temporary_id_url", max_bytes=70 * 1024 * 1024)
    done_count = 0  # 用来追踪已完成的进程数量
    while done_count < num_workers:
        # 从队列中获取数据
        data = data_queue.get()

        if data == 'DONE':
            # 收到 "DONE" 表示某个进程完成任务
            done_count += 1
        else:
            if data:
                print(f'写入一个数据')
                splitter.add(data)

        # 如果队列为空，可以稍作等待再继续检查
        if data_queue.empty():
            time.sleep(1)  # 调整等待时间，以减少 CPU 使用

    splitter.close()  # 写入完成后，关闭文件

def multProcessRun():
    ctx = get_context('spawn')
    conn = connect_to_mysql()
    cursor = conn.cursor()
    id_to_url = load_ids_and_urls_into_memory(cursor)
    batchs = [part for part in split_dict_equal(id_to_url, MAX_PROCESSORS)]

    # 使用 Manager 创建共享的队列
    with Manager() as manager:
        data_queue = manager.Queue()  # 通过 Manager 创建的 Queue 是可以跨进程共享的

        # 启动主进程的写入线程
        writer_process = Process(target=process_writer, args=(data_queue, MAX_PROCESSORS))
        writer_process.start()


        with ProcessPoolExecutor(max_workers=MAX_PROCESSORS, mp_context=ctx) as executor:
            futures = [executor.submit(crawler_authors_by_exist_parents_and_children_singleProcess, batchs[i], i, id_to_url, data_queue) for i in range(MAX_PROCESSORS)]
            # futures.append(executor.submit(crawler_authors_by_exist_parents_and_children_singleProcess, batchs[0], 0, id_to_url, data_queue))
            # futures.append(executor.submit(crawler_authors_by_exist_parents_and_children_singleProcess, batchs[1], 1, id_to_url, data_queue))
            # futures.append(executor.submit(crawler_authors_by_exist_parents_and_children_singleProcess, batchs[2], 2, id_to_url, data_queue))
            # futures.append(executor.submit(crawler_authors_by_exist_parents_and_children_singleProcess, batchs[3], 3, id_to_url, data_queue))
            # futures.append(executor.submit(crawler_authors_by_exist_parents_and_children_singleProcess, batchs[4], 4, id_to_url, data_queue))
            # futures.append(executor.submit(crawler_authors_by_exist_parents_and_children_singleProcess, batchs[5], 5, id_to_url, data_queue))
            # futures.append(executor.submit(crawler_authors_by_exist_parents_and_children_singleProcess, batchs[6], 6, id_to_url, data_queue))
            # futures.append(executor.submit(crawler_authors_by_exist_parents_and_children_singleProcess, batchs[7], 7, id_to_url, data_queue))
            # futures.append(executor.submit(crawler_authors_by_exist_parents_and_children_singleProcess, batchs[8], 8, id_to_url, data_queue))
            # 可选：等待并处理结果/异常
            for fut in as_completed(futures):
                try:
                    res = fut.result()

                except Exception as e:
                    print("worker failed:", e)

        writer_process.join()  # 等待写入线程完成


if __name__ == '__main__':
    multProcessRun()




