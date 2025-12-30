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


# 一个过人机检验的点击函数
# def simulate_security_check(page, element):
#     # 帮助点击前可视化定位点击坐标的函数
#     def add_click_visualization(page, x, y):
#         """在页面上添加点击位置可视化标记"""
#         js_code = f"""
#             // 移除旧的标记
#             const oldMarker = document.getElementById('click-visualization-marker');
#             if (oldMarker) oldMarker.remove();
#
#             // 创建新的标记
#             const marker = document.createElement('div');
#             marker.id = 'click-visualization-marker';
#             marker.style.position = 'fixed';
#             marker.style.left = '{x - 15}px';
#             marker.style.top = '{y - 15}px';
#             marker.style.width = '30px';
#             marker.style.height = '30px';
#             marker.style.border = '3px solid red';
#             marker.style.borderRadius = '50%';
#             marker.style.backgroundColor = 'rgba(255, 0, 0, 0.2)';
#             marker.style.zIndex = '999999';
#             marker.style.pointerEvents = 'none';
#             marker.style.boxShadow = '0 0 10px rgba(255,0,0,0.5)';
#
#             // 添加动画效果
#             marker.style.animation = 'pulse 1.5s infinite';
#
#             // 添加动画样式
#             const style = document.createElement('style');
#             style.textContent = `
#                 @keyframes pulse {{
#                     0% {{ transform: scale(1); opacity: 1; }}
#                     50% {{ transform: scale(1.2); opacity: 0.7; }}
#                     100% {{ transform: scale(1); opacity: 1; }}
#                 }}
#             `;
#             document.head.appendChild(style);
#
#             document.body.appendChild(marker);
#
#             // 5秒后自动移除标记
#             setTimeout(() => {{
#                 marker.remove();
#                 style.remove();
#             }}, 3000);
#             """
#         page.run_js(js_code)
#         print('添加元素后停顿几秒')
#         time.sleep(5)
#     # 使用JavaScript的getBoundingClientRect方法获取元素坐标
#     js_code = """
#     var rect = arguments[0].getBoundingClientRect();
#     return {x: rect.left, y: rect.top};
#     """
#
#     # 起点坐标
#     result = page.run_js(js_code, element)
#     window_x = result['x']
#     window_y = result['y']
#
#
#     #  终点坐标
#     click_x = window_x + random.uniform(25, 30)
#     click_y = window_y +random.uniform(25, 30)
#     print(f'我即将点击的坐标是{click_x}：{click_y}')
#     # add_click_visualization(page, click_x, click_y)
#
#     # 方法二：使用drissionpage自带的鼠标移动模拟点击法
#     # page.actions.move_to((window_x, window_y))
#
#     while True:
#         random_x_step = random.uniform(1, 2)
#         random_y_step = random.uniform(1, 2)
#         window_x += random_x_step
#         window_y += random_y_step
#         if window_x > click_x or window_y > click_y:  # 表示这是最后一步，直接跳到终点坐标执行点击动作
#             page.actions.move_to((window_x, window_y))
#             page.actions.click(times=1)
#             break  # 点击完成推出循环
#         else:  # 否则一步一步的移动
#             page.actions.move_to((window_x, window_y))



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

# 给定个人主页的链接爬取完善 个人信息
def getAuthorName(browser, lock_new_tab, author_info):
    with lock_new_tab:
        tab = browser.new_tab('about:blank')


    try:
        tab.get(author_info['personal_url'])
    except DrissionPage.errors.IncorrectURLError as e:
        try:
            print(f'出错的personal_url={author_info["personal_url"]}')
            tab.get('http://' + author_info['personal_url'])
        except DrissionPage.errors.IncorrectURLError as e:
            print(f'访问个人主页失败{author_info["personal_url"]}<直接返回>')
            return (-1,author_info)

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

        # 获取个人基本信息
        try:
            personInf_tag = tab.ele('css:.personinfo')
            whole_name = personInf_tag.ele('css:h1').text.strip()
            author_info["whole_name"] = whole_name
        except Exception as e:
            print(f'在基础信息提取部分出问题的url是：{author_info["personal_url"]}<<UNK>>')
            traceback.print_exc()
            tab.close()
            return (-1, author_info)

        time.sleep(1)
        tab.close()
        return (200, author_info)
    except DrissionPage.errors.PageDisconnectedError as e:
        try:
            tab.close()
            return (-1, author_info)
        except Exception as e:
            ...

# 获取新父子合作节点信息的单进程
def crawler_author_name_singleProcess(batch, proc_idx, data_queue):
    co = ChromiumOptions()
    # co.set_argument('--headless=new')
    # 临时目录，一进程一份
    profile_dir = os.path.join(tempfile.gettempdir(),
                               f"dp_profile_DrissionPage_{proc_idx}")
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

    all_author_info = list(batch.values())

    print(f'进程{proc_idx}需要爬取{len(all_author_info)}个作者信息')

    if not all_author_info:
        print(f'进程{proc_idx}：传入 batch 为空，跳过该进程的爬取。')
        # 发送完成标志，保持和其他进程一致（确保写线程能正确退出）
        data_queue.put('DONE')
        browser.quit()
        # 清理 profile_dir 的注册也保留
        atexit.register(lambda: shutil.rmtree(profile_dir, ignore_errors=True))
        return

    # 这里先行访问一个主页是为了手动过一次人机验证
    temp_tab = browser.new_tab('about:blank')
    # print(all_author_info[random.randint(0, len(all_author_info) - 1)])
    temp_tab.get(all_author_info[random.randint(0, len(all_author_info) - 1)]['personal_url'])  #
    print('检测到有人机检验, 尝试模拟点击过验证')
    # ele = temp_tab.ele('css:.cf-turnstile ')
    # simulate_security_check(temp_tab, ele)
    time.sleep(60)
    temp_tab.close()

    lock_new_tab = threading.Lock()  # 保护 br.new_tab()
    try:
        with ThreadPoolExecutor(max_workers=MAX_THREAD) as ex:
            futs = [ex.submit(getAuthorName, browser, lock_new_tab, v) for v in all_author_info]
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
    completedFinalAuthors = set()
    for filename in os.listdir('./finalAuthorJsons/replenishAuthorJsons'):
        if filename.endswith(".jsonl"):
            file_path = os.path.join('./finalAuthorJsons/replenishAuthorJsons', filename)
            # 读取并解析JSON文件
            with open(file_path, 'r', encoding='utf-8') as file:
                for line in file:
                    entry = json.loads(line)
                    # 假设data中是一个列表，我们要遍历列表中的每个元素
                    if 'id' in entry:
                        author_id = entry['id']
                        completedFinalAuthors.add(author_id)

    print(f'现有作者数量：{len(completedFinalAuthors)}<UNK>')


    # 使用 Manager 创建共享的队列
    with Manager() as manager:
        data_queue = manager.Queue()  # 通过 Manager 创建的 Queue 是可以跨进程共享的

        # 启动主进程的写入线程
        base_name = f'./finalAuthorJsons/replenishAuthorJsons/completeAuthorInfo'  # 写入的文件夹以及文件名
        writer_process = Process(target=process_writer, args=(data_queue, MAX_PROCESSORS, base_name))
        writer_process.start()

        # 读取将要爬取的new_authors
        newAuthors = dict()
        for filename in os.listdir('./completeAuthorJsons'):
            if filename.endswith(".jsonl"):
                file_path = os.path.join('./completeAuthorJsons', filename)
                # 读取并解析JSON文件
                with open(file_path, 'r', encoding='utf-8') as file:
                    line_flag = True
                    for line in file:
                        entry = json.loads(line)
                        if entry.get('whole_name'):
                            ...
                        elif entry.get('name'):
                            entry['whole_name'] = entry['name']
                        else:  # 如果既没有name 也没有whole_name说明这条数据是缺失数据
                            line_flag = False

                        if entry.get('baseHref'):  # 把baseHref对应的personal_url也补全
                            entry['personal_url'] = entry['baseHref'] + f"peopleinfo.php?pid={entry['id']}"

                        if (not line_flag) and (entry['id'] not in completedFinalAuthors):  # 如果缺乏名字，并且没有补过，就添加入即将爬取名字的字典内
                            newAuthors[entry['id']] = entry
                        elif (line_flag) and (entry['id'] not in completedFinalAuthors):
                            data_queue.put(entry)

        print(f'这轮需要补充名字的作者数量是：{len(newAuthors)}<UNK>')
        # 因为分了好几台机器，所以我们自己这台机器只用爬取所有newAuthors对应的部分即可
        newAuthors = {k: v for k, v in newAuthors.items() if k not in completedFinalAuthors}
        # 把本台机器所需要爬取的作者按照进程数分批，同时与已有作者去重
        batchs = split_dict_equal(newAuthors, MAX_PROCESSORS)


        with ProcessPoolExecutor(max_workers=MAX_PROCESSORS, mp_context=ctx) as executor:
            futures = [executor.submit(crawler_author_name_singleProcess, batchs[i], i, data_queue) for i in range(MAX_PROCESSORS)]
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

