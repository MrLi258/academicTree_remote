import csv
import json
import os
import tempfile
import threading
import time
import atexit
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor
from itertools import islice
import random
from multiprocessing import get_context
from DrissionPage._configs.chromium_options import ChromiumOptions
from DrissionPage import ChromiumPage
from jsonFileRepair import replaceFileInplace
MAX_THREAD = 2
def getExistAuthorInfo():
    filePath = './completeAuthorInfo.json'
    idSet = set()
    if not os.path.exists(filePath) or os.path.getsize(filePath) == 0:
        # 文件不存在或为空，返回默认值
        return idSet
    with open(filePath, 'r', encoding='utf-8') as f:
        authorInfos = json.load(f)
        for authorinfo in authorInfos:
            idSet.add(authorinfo['id'])
    return idSet

def create_not200_csv_if_not_exists():
    # 示例使用
    file_path = './not200Author.csv'
    headers = ['name', 'url', 'state_code']
    # 判断文件是否存在
    if not os.path.exists(file_path):
        # 不存在则创建文件并写入表头
        with open(file_path, mode='w', encoding='utf-8', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(headers)
        print(f"文件 '{file_path}' 已创建，并写入表头。")
    else:
        print(f"文件 '{file_path}' 已存在，无需创建。")

# 数据生成器
def urlGenerator():
    href_ = 'peopleinfo.php?pid='
    with open("../deduplicated_authors.json", mode="r", encoding="utf8") as rfile:
        json_data = json.load(rfile)
        print(type(json_data))

    existAuthorInfos = getExistAuthorInfo()

    for data in json_data[:]:
        href = data["baseHref"] + "peopleinfo.php?pid=" + data["id"]
        if data["id"] in existAuthorInfos:
            print(f"{data['id']} <已经爬取过了>")
            continue
        yield (data, href)

# 每次从生成器中产生批量大小的数据
def batch_generator(iterable, n):
    it = iter(iterable)
    while True:
        batch = list(islice(it, n))
        if not batch:
            break
        yield batch

def getAuthorInfo(browser, lock_new_tab, meta, url):
    with lock_new_tab:
        tab = browser.new_tab('about:blank')
    tab.get(url)
    # input(f'暂停检查网站，输入任意字符继续')
    # 初始化一个空字典,记录新增的用户信息
    data_dict = {}

    try:
        while(True):
            html = tab.html
            if "Please complete the security check to proceed" in html:
                print('检测到有人机检验')
                time.sleep(2)
            else:
                break
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
        print(f'在基础信息提取部分出问题的url是：{url}')
        print(e)
        tab.close()
        return (-1, meta)
    meta.update(data_dict)


    publication_url = url.replace('peopleinfo', 'publications')
    publicationEles = tab.eles('css:.rightcol .container tbody .clickable-row')
    # print(f'该作者有{len(publicationEles)}篇文章')
    if len(publicationEles) > 0:
        # 如果有论文，则跳转到到publication详情页爬取
        tab.get(publication_url)
    else:
        tab.close()

        return (200, meta)

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
    tab.close()
    meta.update(data_dict)
    return (200, meta)

def getCompleteAuthorInfo_singleProcess(batch, proc_idx):
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

    infoList = list()
    # 这里先行访问一个主页是为了手动过一次人机验证
    temp_tab = browser.new_tab('')
    temp_tab.get('http://academictree.org/biomech/peopleinfo.php?pid=238154')
    # input('请在完成人机验证之后输入任意字符继续程序')
    time.sleep(20)
    temp_tab.close()

    lock_new_tab = threading.Lock()  # 保护 br.new_tab()
    try:
        with ThreadPoolExecutor(max_workers=MAX_THREAD) as ex:
            futs = [ex.submit(getAuthorInfo, browser, lock_new_tab, meta, url) for meta, url in batch]
            # print(f'总共有{len(futs)}个任务等待返回')
            for fut in as_completed(futs):
                stat_code, meta = fut.result()
                if stat_code == 200:
                    print(meta)
                    infoList.append(meta)

                if len(infoList) >= 50:
                    with open('./completeAuthorInfo.json', mode='a', encoding="utf8") as wfile:
                        json.dump(infoList, wfile)
                        print(f'{len(infoList)}条数据写成功')
                    infoList.clear()
                    # break
            else:
                with open('./completeAuthorInfo.json', mode='a', encoding="utf8") as wfile:
                    json.dump(infoList, wfile)
                    print(f'{len(infoList)}条数据写成功')
                infoList.clear()
            time.sleep(random.uniform(3, 6))
            return True
    finally:
        try:
            browser.quit()
            # 注册退出时自动删除
            atexit.register(lambda: shutil.rmtree(profile_dir, ignore_errors=True))
            return True
        except:
            return False
            pass

    ...

def multProcessRun():
    ctx = get_context('spawn')

    batchs = [[] for _ in range(MAX_PROCESSORS)]
    for idx, meta in enumerate(urlGenerator()):
        batchs[idx % MAX_PROCESSORS].append(meta)

    with ProcessPoolExecutor(max_workers=MAX_PROCESSORS, mp_context=ctx) as executor:
        futures = []
        futures.append(executor.submit(getCompleteAuthorInfo_singleProcess, batchs[0], 0))
        # futures.append(executor.submit(getCompleteAuthorInfo_singleProcess, batchs[1], 1))
        # futures.append(executor.submit(getCompleteAuthorInfo_singleProcess, batchs[2], 2))
        # futures.append(executor.submit(getCompleteAuthorInfo_singleProcess, batchs[3], 3))
        # futures.append(executor.submit(getCompleteAuthorInfo_singleProcess, batchs[4], 4))
        # futures.append(executor.submit(getCompleteAuthorInfo_singleProcess, batchs[5], 5))
        # futures.append(executor.submit(getCompleteAuthorInfo_singleProcess, batchs[6], 6))
        # futures.append(executor.submit(getCompleteAuthorInfo_singleProcess, batchs[7], 7))
        # executor.submit(getCompleteAuthorInfo_singleProcess, browsers[2], batchs[2])
        # 可选：等待并处理结果/异常
        for fut in as_completed(futures):
            try:
                res = fut.result()

            except Exception as e:
                print("worker failed:", e)







def test_getInfo():
    meta = {"parentsIdList": ["556961"], "childrenIdList": [], "id": "559624", "name": "Virginia J. Ochoa-Winemiller", "baseHref": "http://academictree.org/anthropology/"}
    browser = ChromiumPage()
    lock_new_tab = threading.Lock()  # 保护 br.new_tab()
    url = meta["baseHref"] + "peopleinfo.php?pid=" + meta["id"]
    print(getAuthorInfo(browser=browser, lock_new_tab=lock_new_tab, meta=meta, url=url))

def testNum():
    with open('./completeAuthorInfo.json', mode='r', encoding="utf8") as rfile:
            json_list = json.load(rfile)
            print(len(json_list))
if __name__ == "__main__":
    # create_not200_csv_if_not_exists()
    multProcessRun()
    # test_getInfo()
    
    replaceFileInplace('./completeAuthorInfo.json')
    testNum()
    # getAuthorInfo("Florence M. Brunel", "https://academictree.org/chemistry/peopleinfo.php?pid=305745")