import atexit
import os
import shutil
import sys
import tempfile
import threading
from concurrent.futures import as_completed
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import get_context

from DrissionPage._configs.chromium_options import ChromiumOptions
import time
import json

from DrissionPage import ChromiumPage

from jsonFileRepair import replaceFileInplace
from utils import save_progress
from config import driver_path

MAX_THREAD = 2




def getAuthorInfo(browser, lock_new_tab, meta):
    with lock_new_tab:
        tab = browser.new_tab('about:blank')
        tab.get(meta['person_url'])
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
    data_dict.update(meta)

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
        print(f'在基础信息提取部分出问题的url是：{meta["person_url"]}')
        print(e)
        tab.close()
        return (-1, data_dict)
    print(f'第二步获取个人信息陈成功{data_dict}')


    # 获取文章信息
    publication_url = meta["person_url"].replace('peopleinfo', 'publications')
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

def getInfoListAndSaveCompleteAuthorInfo(proc_idx, base_url, searchName):
    browser = ChromiumPage()
    url = (
        base_url
        + "/peoplelist.php?searchname="
        + searchName
        + "&searchalltrees=1&allfields=1"
    )
    field = base_url.split('/')[-2]
    print(url)
    infoList_tab = browser.new_tab()
    infoList_tab.get(url)
    time.sleep(15)
    # # 只要ip没被封就记录
    # save_progress(self.field, self.searchName, self.fieldPart)

    tableRows = infoList_tab.eles("css:tbody tr")
    print(
        f"领域{base_url}内包含{searchName}的作者有{len(tableRows) - 3} 个"
    )
    meta_list = []
    for row in tableRows[3:-2]:
        infoHref = row.ele("css:td:nth-child(1) a:nth-child(2)").attr("href")
        id = infoHref.split("=")[-1]
        name = row.ele("css:td:nth-child(1) a:nth-child(2)").text.strip()
        meta_list.append({"person_url":infoHref, "id": id, "name": name})
    browser.quit()

    getCompleteAuthorInfo(meta_list, proc_idx, field, searchName)

def getCompleteAuthorInfo(meta_list, proc_idx, field, searchName):
    infoList = []
    def meta_generator():
        for meta in meta_list:
            yield meta

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
    lock_new_tab = threading.Lock()
    ctx = get_context('spawn')
    try:
        with ThreadPoolExecutor(max_workers=MAX_THREAD) as ex:
            futs = [ex.submit(getAuthorInfo, browser, lock_new_tab, meta) for meta in meta_generator()]  # 线程列表
            for fut in as_completed(futs):
                stat_code, data_dict = fut.result()
                if stat_code == 200:
                    print(data_dict)
                    infoList.append(data_dict)
                # 每五条消息保存一次
                if len(infoList) >= 10:
                    saveInfo2(infoList, field, searchName)

                    infoList.clear()
                    exit(1)
    finally:
        try:
            browser.quit()
            # 注册退出时自动删除
            atexit.register(lambda: shutil.rmtree(profile_dir, ignore_errors=True))
            return True
        except:
            return False

def saveInfo2(infoList, field, searchName):
    if infoList is not None:
        base_path = "./completeInfo/" + f"{field}/"
        if os.path.exists(base_path) == False:
            os.makedirs(base_path)
        try:
            fileName = base_path + searchName + ".json"
            with open(
                fileName,
                mode="a",
                encoding="utf-8",
            ) as f1:
                json.dump(infoList, f1, ensure_ascii=False, indent=4)
                print(
                    "成功在",
                    base_path  + searchName + ".json",
                    f"文件中保存{len(infoList)}条数据",
                )
            # json数据格式修复
            replaceFileInplace(fileName)
        except Exception as e:
            print(e)
            print("info数据写入失败")
        finally:
            pass
    else:
        print("无内容")

if __name__ == '__main__':
    getInfoListAndSaveCompleteAuthorInfo(1, 'http://neurotree.org/neurotree/', 'v')