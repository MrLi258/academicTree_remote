import atexit
import json
import os
import random
import shutil
import sys
import tempfile
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor
from multiprocessing import get_context, Process, Queue, Manager

import pyautogui
from DrissionPage import ChromiumPage
import mysql.connector
from DrissionPage._configs.chromium_options import ChromiumOptions
from utils import JsonFileSplitter, split_dict_equal
MAX_THREAD = 2
MAX_PROCESSORS = 9
from _1_load_completed_ids_and_urls_into_memory import load_completed_ids_and_urls_into_memory

# 读取配置信息
with open('./config.json', 'r') as f:
    config = json.load(f)
    MAX_THREAD = config['MAX_THREAD']
    MAX_PROCESSORS = config['MAX_PROCESSORS']
    PART_NUMS = config['PART_NUMS']
    THIS_COMPUTER_IDX = config['THIS_COMPUTER_IDX']


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
            parentsIdList.append(parentId, parentUrl)
    data_dict["parentsIdList"] = parentsIdList
    # 获取childrenIdList
    childrenRow = tab.eles("css:.leftcol table:nth-child(2)  tr")
    childrenIdList = []
    if childrenRow is not None:
        for tr in childrenRow:
            childUrl = tr.ele('css:td:nth-child(1) a').attr("href")
            childId = childUrl.split("=")[-1]
            childrenIdList.append(childId, childUrl)
    data_dict["childrenIdList"] = childrenIdList
    print(f'第一步获取父子节点陈成功{data_dict}')
    # 获取CollaboratorsIdList
    collaboratorsRow = tab.eles("css:.leftcol .boxfloat:nth-child(3) table tbody tr")
    collaboratorsIdList = []
    if collaboratorsRow is not None:
        for tr in collaboratorsRow:
            try:
                collaboratorsUrl = tr.ele('css:td:nth-child(1) a').attr("href")
                collaboratorsId = collaboratorsUrl.split("=")[-1]
                # print(f'找到一个合作节点id：{collaboratorsId}')
                collaboratorsIdList.append((collaboratorsId, collaboratorsUrl))
            except:
                ...
    data_dict["collaboratorsIdList"] = collaboratorsIdList
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


# 获取新父子合作节点信息的单进程
def crawler_authors_by_exist_parents_and_children_singleProcess(batch, proc_idx, data_queue):
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

    all_url = list(batch.values())

    # 这里先行访问一个主页是为了手动过一次人机验证
    # temp_tab = browser.new_tab('')
    # temp_tab.get(all_url[random.randint(0, len(all_url) - 1)])  #
    # print('检测到有人机检验, 尝试模拟点击过验证')
    # ele = temp_tab.ele('css:.cf-turnstile ')
    # simulate_security_check(temp_tab, ele)
    # time.sleep(10)
    # temp_tab.close()

    lock_new_tab = threading.Lock()  # 保护 br.new_tab()
    try:
        with ThreadPoolExecutor(max_workers=MAX_THREAD) as ex:
            futs = [ex.submit(getCompleteAuthorInfo, browser, lock_new_tab, url) for url in all_url]
            for fut in as_completed(futs):
                state, dic = fut.result()
                if state == 200:
                    print(f'向数据管道输送一条数据：{dic}')
                    data_queue.put(dic)
    finally:
        browser.quit()
        # 注册退出时自动删除
        atexit.register(lambda: shutil.rmtree(profile_dir, ignore_errors=True))

    # 发送完成信号给写入进程
    data_queue.put('DONE')  # 发送“DONE”标识该进程已完成

# 一个专门写入数据的线程，避免多线程同时写入数据时容易造成的错误
def process_writer(data_queue, num_workers, base_name):
    # base_name = f'./newAuthorJsons/split/newAuthorInfo_computer{THIS_COMPUTER_IDX}'
    splitter = JsonFileSplitter(base_name, max_bytes=70 * 1024 * 1024)
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
    # 记录所有已经完成的作者ID，避免重复爬取(这个去重主要是在某一次迭代处于未完成状态重新启动时防止该次迭代过程中之前爬取的数据重复，也就是提升爬取速率，不重复爬取)
    completedTemporaryAuthors = dict()
    for filename in os.listdir('./newAuthorJsons'):
        if filename.endswith(".json"):
            file_path = os.path.join('./newAuthorJsons', filename)
            # 读取并解析JSON文件
            with open(file_path, 'r', encoding='utf-8') as file:
                data = json.load(file)

                # 假设data中是一个列表，我们要遍历列表中的每个元素
                for entry in data:
                    if 'id' in entry and 'url' in entry:
                        author_id = entry['id']
                        author_url = entry['url']
                        completedTemporaryAuthors[author_id] = author_url

    completed_id_to_url = load_completed_ids_and_urls_into_memory()
    completed_id_to_url.update(completedTemporaryAuthors)  # 把数据库和文件夹中以爬取的作者汇总用于后续去重

    # 读取将要爬取的new_authors
    newAuthors = dict()
    for filename in os.listdir('./temporaryAuthorJsons'):
        if filename.endswith(".json"):
            file_path = os.path.join('./temporaryAuthorJsons', filename)
            # 读取并解析JSON文件
            with open(file_path, 'r', encoding='utf-8') as file:
                data = json.load(file)
                # 把该作者所有的父子合作节点添加进将要爬取的作者中
                for entry in data:
                    if parentsIdList:=entry.get('parentsIdList'):
                        for parent in parentsIdList:
                            newAuthors[parent[0]] = parent[1]
                    if childrenIdList:=entry.get('childrenIdList'):
                        for child in childrenIdList:
                            newAuthors[child[0]] = child[1]
                    if collaboratorsIdList:=entry.get('collaboratorsIdList'):
                        for collaborator in collaboratorsIdList:
                            newAuthors[collaborator[0]] = collaborator[1]

    # 因为分了好几台机器，所以我们自己这台机器只用爬取所有newAuthors对应的部分即可
    myNewAuthors = split_dict_equal(newAuthors, PART_NUMS)[THIS_COMPUTER_IDX]

    # 把本台机器所需要爬取的作者按照进程数分批，同时与已有作者去重
    batchs = [{k:v for k,v in part.items() if k not in completed_id_to_url} for part in split_dict_equal(myNewAuthors, MAX_PROCESSORS)]

    # 使用 Manager 创建共享的队列
    with Manager() as manager:
        data_queue = manager.Queue()  # 通过 Manager 创建的 Queue 是可以跨进程共享的

        # 启动主进程的写入线程
        base_name = f'./newAuthorJsons/split/newAuthorInfo_computer{THIS_COMPUTER_IDX}'  # 写入的文件夹以及文件名
        writer_process = Process(target=process_writer, args=(data_queue, MAX_PROCESSORS, base_name))
        writer_process.start()


        with ProcessPoolExecutor(max_workers=MAX_PROCESSORS, mp_context=ctx) as executor:
            futures = [executor.submit(crawler_authors_by_exist_parents_and_children_singleProcess, batchs[i], i, data_queue) for i in range(MAX_PROCESSORS)]
            # 可选：等待并处理结果/异常
            for fut in as_completed(futures):
                try:
                    res = fut.result()
                except Exception as e:
                    print("worker failed:", e)
                    traceback.print_exc()


        writer_process.join()  # 等待写入线程完成

