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

import DrissionPage
import pyautogui
from DrissionPage import ChromiumPage, errors
import mysql.connector
from DrissionPage._configs.chromium_options import ChromiumOptions
from utils import JsonFileSplitter, split_dict_equal

from _1_load_completed_ids_and_urls_into_memory import load_completed_ids_and_urls_into_memory

# 读取配置信息
with open('./config.json', 'r') as f:
    config = json.load(f)
    MAX_THREAD = config['MAX_THREAD']
    MAX_PROCESSORS = config['MAX_PROCESSORS']
    PART_NUMS = config['PART_NUMS']
    THIS_COMPUTER_IDX = config['THIS_COMPUTER_IDX']

print(f'MAX_PROCESSORS = {MAX_PROCESSORS}')

# 一个过人机检验的点击函数
def simulate_security_check(page, element):
    # 帮助点击前可视化定位点击坐标的函数
    def add_click_visualization(page, x, y):
        """在页面上添加点击位置可视化标记"""
        js_code = f"""
            // 移除旧的标记
            const oldMarker = document.getElementById('click-visualization-marker');
            if (oldMarker) oldMarker.remove();

            // 创建新的标记
            const marker = document.createElement('div');
            marker.id = 'click-visualization-marker';
            marker.style.position = 'fixed';
            marker.style.left = '{x - 15}px';
            marker.style.top = '{y - 15}px';
            marker.style.width = '30px';
            marker.style.height = '30px';
            marker.style.border = '3px solid red';
            marker.style.borderRadius = '50%';
            marker.style.backgroundColor = 'rgba(255, 0, 0, 0.2)';
            marker.style.zIndex = '999999';
            marker.style.pointerEvents = 'none';
            marker.style.boxShadow = '0 0 10px rgba(255,0,0,0.5)';

            // 添加动画效果
            marker.style.animation = 'pulse 1.5s infinite';

            // 添加动画样式
            const style = document.createElement('style');
            style.textContent = `
                @keyframes pulse {{
                    0% {{ transform: scale(1); opacity: 1; }}
                    50% {{ transform: scale(1.2); opacity: 0.7; }}
                    100% {{ transform: scale(1); opacity: 1; }}
                }}
            `;
            document.head.appendChild(style);

            document.body.appendChild(marker);

            // 5秒后自动移除标记
            setTimeout(() => {{
                marker.remove();
                style.remove();
            }}, 3000);
            """
        page.run_js(js_code)
        print('添加元素后停顿几秒')
        time.sleep(5)
    # 使用JavaScript的getBoundingClientRect方法获取元素坐标
    js_code = """
    var rect = arguments[0].getBoundingClientRect();
    return {x: rect.left, y: rect.top};
    """

    # 起点坐标
    result = page.run_js(js_code, element)
    window_x = result['x']
    window_y = result['y']


    #  终点坐标
    click_x = window_x + random.uniform(25, 30)
    click_y = window_y +random.uniform(25, 30)
    print(f'我即将点击的坐标是{click_x}：{click_y}')

    start_x = click_x + 100
    start_y = click_y + 100

    # add_click_visualization(page, click_x, click_y)

    # 方法二：使用drissionpage自带的鼠标移动模拟点击法
    # page.actions.move_to((window_x, window_y))

    while True:
        random_x_step = random.uniform(1, 4)
        random_y_step = random.uniform(1, 4)
        if start_x > click_x:
            start_x -= random_x_step
        if start_y > click_y:
            start_y -= random_y_step
        if start_x < click_x and start_y < click_y:  # 表示这是最后一步，直接跳到终点坐标执行点击动作
            page.actions.move_to((start_x, start_y))
            page.actions.click(times=1)
            break  # 点击完成推出循环

        page.actions.move_to((start_x, start_y))  # 否则一步一步的移动


# 获取主页列表所有作者信息
def get_home_list_info(browser, lock_new_tab, home_url, completedAuthors):
    with lock_new_tab:
        tab = browser.new_tab('about:blank')

    tab.get(home_url)
    time.sleep(1)
    flag = False
    while True:
        html = tab.html
        if "Please complete the security check to proceed" in html:
            flag = True
            print('检测到有人机检验, 尝试模拟点击过验证')
            ele = tab.ele('css:.cf-turnstile ')
            simulate_security_check(tab, ele)
            time.sleep(random.uniform(1, 3))
        else:
            if flag:
                print('成功完成人机检验')
            break

    time.sleep(2)
    print(f'正在访问搜索主页{home_url}')
    tbody = tab.ele('css:tbody')
    authorList = []
    tr_list = tbody.eles('css:tr')
    if len(tr_list) > 3:
        for tr in tbody.eles('css:tr')[4:-1]:
            author_info = tr.ele('css:td:nth-child(1 ) a:nth-child(2)')
            if author_info:
                author_url = author_info.attr('href')
                id = author_url.split('/')[-1].replace('peopleinfo.php?pid=', '')
                if id not in completedAuthors:
                    # print(f'找到一个新的作者{author_url}<UNK>')
                    authorList.append(author_url)

    tab.close()
    return authorList
    ...

# 给定个人主页的链接爬取完善 个人信息
def getCompleteAuthorInfo(browser, lock_new_tab, author_url):
    with lock_new_tab:
        tab = browser.new_tab('about:blank')


    id = author_url.split('?')[-1].replace('pid=', '')
    data_dict = {'id': id}
    try:
        tab.get(author_url)
    except DrissionPage.errors.IncorrectURLError as e:
        try:
            tab.get('http://' + author_url)
        except DrissionPage.errors.IncorrectURLError as e:
            tab.close()
            return (-1,data_dict)

    try:
        flag = False
        while (True):
            html = tab.html
            if "Please complete the security check to proceed" in html:
                flag = True
                print('检测到有人机检验, 尝试模拟点击过验证')
                ele = tab.ele('css:.cf-turnstile ')
                simulate_security_check(tab, ele)
                time.sleep(random.uniform(1, 3))
            else:
                if flag:
                    print('成功完成人机检验')
                break
        # input(f'暂停检查网站，输入任意字符继续')
        # 初始化一个空字典,记录新增的用户信息

        data_dict['personal_url'] = author_url

        # print(f'即将开始查找父节点')
        parentsRow = tab.eles("css:.leftcol .boxfloat:nth-child(1) table tbody tr")
        parentsIdList = []
        if parentsRow is not None:
            for tr in parentsRow:
                try:
                    parentUrl = tr.ele('css:td:nth-child(1) a').attr("href")
                    parentId = parentUrl.split("=")[-1]
                    # print(f'找到一个父节点id：{parentId}')
                    parentsIdList.append((parentId, parentUrl))
                except:
                    ...
        data_dict["parentsIdList"] = parentsIdList

        # print(f'即将开始查找子节点')
        # 获取childrenIdList
        childrenRow = tab.eles("css:.leftcol .boxfloat:nth-child(2) table tbody tr")
        childrenIdList = []
        if childrenRow is not None:
            for tr in childrenRow:
                try:
                    childUrl = tr.ele('css:td:nth-child(1) a').attr("href")
                    childId = childUrl.split("=")[-1]
                    # print(f'找到一个子节点id：{childId}')
                    childrenIdList.append((childId, childUrl))
                except:
                    ...
        data_dict["childrenIdList"] = childrenIdList

        # print(f'即将开始查找合作节点')
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
        # print(f'第一步获取pcc节点成功{data_dict}')
        # 获取个人基本信息
        try:
            personInf_tag = tab.ele('css:.personinfo')
            whole_name = personInf_tag.ele('css:h1').text.strip()
            data_dict["whole_name"] = whole_name
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
            traceback.print_exc()
            tab.close()
            return (-1, data_dict)
        # print(f'第二步获取个人信息陈成功{data_dict}')

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
            # print(f'第三步获取个人信息成功{data_dict}')
        tab.close()
        return (200, data_dict)
    except DrissionPage.errors.PageDisconnectedError as e:
        try:
            tab.close()
            return (-1, data_dict)
        except Exception as e:
            ...

# 获取新节点信息的单进程
def crawler_authors_by_exist_parents_and_children_singleProcess(baseHrefs, completedAuthors, proc_idx, data_queue):
    co = ChromiumOptions()
    # co.set_argument('--headless=new')
    # 临时目录，一进程一份
    profile_dir = os.path.join(tempfile.gettempdir(),
                               f"dp_profile_DrissionPage_{proc_idx}")
    print('profile_dir', profile_dir)
    os.makedirs(profile_dir, exist_ok=True)
    co.set_argument(f'--user-data-dir={profile_dir}')
    co.set_timeouts(30)
    co.set_local_port(9300 + proc_idx)  #
    browser = ChromiumPage(co)

    # 根据屏幕大小以及进程数量设置浏览器窗口的大小
    screen_width, screen_height = pyautogui.size()
    width = int(screen_width / (MAX_PROCESSORS ** 0.5))
    height = int(screen_height / (MAX_PROCESSORS ** 0.5))
    browser.set.window.size(width, height)

    lock_new_tab = threading.Lock()  # 保护 br.new_tab()

    # 按照每个领域a-z搜索
    search_list = 'abcdefghijklmnopqrstuvwxyz'

    try:
        for baseHref in baseHrefs:
            for searchName in search_list:
                url = (
                        baseHref
                        + "/peoplelist.php?searchname="
                        + searchName
                        + "&searchalltrees=1&allfields=1"
                )

                all_url = get_home_list_info(browser, lock_new_tab, url, completedAuthors)

                print(f'网页{baseHref}\t字母{searchName}需要爬取{len(all_url)}个作者信息')
                print(all_url)

                if not all_url:
                    continue

                with ThreadPoolExecutor(max_workers=MAX_THREAD) as ex:
                    futs = [ex.submit(getCompleteAuthorInfo, browser, lock_new_tab, url) for url in all_url]
                    for fut in as_completed(futs):
                        state, dic = fut.result()
                        if state == 200:
                            # print(f'向数据管道输送一条数据：{dic}')
                            data_queue.put(dic)

    finally:
        browser.quit()
        # 注册退出时自动删除
        atexit.register(lambda: shutil.rmtree(profile_dir, ignore_errors=True))

    # 发送完成信号给写入进程
    data_queue.put('DONE')  # 发送“DONE”标识该进程已完成
    print(f'进程{proc_idx}结束')

# 一个专门写入数据的线程，避免多线程同时写入数据时容易造成的错误
def process_writer(data_queue, num_workers, base_name):
    base_dir ='/'.join(base_name.split('/')[:-1])
    print(f'base_dir={base_dir}')
    if not os.path.exists(base_dir):
        os.makedirs(base_dir)
    splitter = JsonFileSplitter(base_name, max_bytes=70 * 1024 * 1024)
    buffer = []
    batch_size = 400
    done_count = 0  # 用来追踪已完成的进程数量
    try:
        while done_count < num_workers:
            # 从队列中获取数据
            data = data_queue.get()

            if data == 'DONE':
                # 收到 "DONE" 表示某个进程完成任务
                done_count += 1
            else:
                if data:
                    buffer.append(data)
                    # 批够数量就写一次
                    if len(buffer) >= batch_size:
                        for obj in buffer:
                            splitter.add(obj)
                        buffer.clear()
                    # splitter.add(data)

            # 如果队列为空，可以稍作等待再继续检查
            # if data_queue.empty():
            #     time.sleep(1)  # 调整等待时间，以减少 CPU 使用
    finally:
        if buffer:
            for obj in buffer:
                splitter.add(obj)
        splitter.close()  # 写入完成后，关闭文件

def multProcessRun():
    ctx = get_context('spawn')
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

    with open("../fieldsHref.txt", mode="r", encoding="utf8") as rfile:
        href_list = rfile.readlines()
        href_list = [item.replace("\n", "") for item in href_list]

    batchs = [href_list[start:start + MAX_PROCESSORS] for start in range(0, len(href_list), (len(href_list) // MAX_PROCESSORS) + 1)]
    print(batchs)
    # 使用 Manager 创建共享的队列
    with Manager() as manager:
        data_queue = manager.Queue()  # 通过 Manager 创建的 Queue 是可以跨进程共享的

        # 启动主进程的写入线程
        base_name = f'./newAuthorJsons/split/newAuthorInfo_computer{THIS_COMPUTER_IDX}'  # 写入的文件夹以及文件名
        writer_process = Process(target=process_writer, args=(data_queue, MAX_PROCESSORS, base_name))
        writer_process.start()


        with ProcessPoolExecutor(max_workers=MAX_PROCESSORS, mp_context=ctx) as executor:
            futures = [executor.submit(crawler_authors_by_exist_parents_and_children_singleProcess, batchs[i], completed_id_to_url, i, data_queue) for i in range(MAX_PROCESSORS)]
            # 可选：等待并处理结果/异常
            for fut in as_completed(futures):
                try:
                    res = fut.result()
                except Exception as e:
                    print("worker failed:", e)
                    traceback.print_exc()


        writer_process.join()  # 等待写入线程完成

if __name__ == '__main__':
    multProcessRun()

