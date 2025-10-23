import atexit
import os
import shutil
import sys
import tempfile
import threading
from concurrent.futures import as_completed
from concurrent.futures import ThreadPoolExecutor

from DrissionPage._configs.chromium_options import ChromiumOptions
from selenium import webdriver
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.chrome.service import Service
import time
import json
from bs4 import BeautifulSoup
from DrissionPage import ChromiumPage

from jsonFileRepair import replaceFileInplace
from utils import save_progress
from config import driver_path

# 获取当前日期
current_date = time.strftime("%Y_%m_%d")


class treeApi:
    """
    输入
    proxies
    代理
    driver_path
    webdriver
    路径
    if_debug
    是否显示界面
    user_file_path
    用户文件保存
    需要绝对路径
    fieldPart: 属于哪个主机，用于保存不同主机的爬取进度
    """

    def __init__(
        self,
        base_url,
        searchName,
        proc_idx,
        proxies=None,
        MAX_THREAD=4
    ):
        self.base_url = base_url
        self.field = self.base_url.split('/')[-1]
        self.searchName = searchName
        self.proxies = proxies
        if self.proxies != None:
            os.environ["http_proxy"] = proxies["http_proxy"]  # "http://127.0.0.1:10809"
            os.environ["https_proxy"] = proxies[
                "https_proxy"
            ]  # "http://127.0.0.1:10809"
        self.proc_idx = proc_idx
        self._create_driver()  #
        self.MAX_THREAD = MAX_THREAD

    def _create_driver(self):
        co = ChromiumOptions()
        # co.set_argument('--headless=new')
        # 临时目录，一进程一份
        self.profile_dir = os.path.join(tempfile.gettempdir(),
                                   f"dp_profile_{os.getpid()}_{self.proc_idx}")
        print('profile_dir', self.profile_dir)
        os.makedirs(self.profile_dir, exist_ok=True)
        co.set_argument(f'--user-data-dir={self.profile_dir}')
        co.set_local_port(9300 + self.proc_idx)  #
        self.browser = ChromiumPage(co)

    def __del__(self):
        if hasattr(self, "driver") and self.driver:
            try:
                self.browser.quit()  # 改用 quit() 确保完全退出
                # 注册退出时自动删除
                atexit.register(lambda: shutil.rmtree(self.profile_dir, ignore_errors=True))
            except Exception as e:
                print(f"关闭driver时出错: {e}")

    def getAuthorInfo(self, lock_new_tab, meta):
        with lock_new_tab:
            tab = self.browser.new_tab('about:blank')
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

    def getInfoListAndSaveCompleteAuthorInfo(self):
        url = (
            self.base_url
            + "/peoplelist.php?searchname="
            + self.searchName
            + "&searchalltrees=1&allfields=1"
        )
        print(url)
        infoList_tab = self.browser.new_tab()
        infoList_tab.get(url)
        time.sleep(15)
        # input('解锁网站')

        # # 只要ip没被封就记录
        # save_progress(self.field, self.searchName, self.fieldPart)

        # try:
        tableRows = infoList_tab.eles("css:tbody tr")
        infoList = []
        print(
            f"领域{self.base_url}内包含{self.searchName}的作者有{len(tableRows) - 3} 个"
        )
        meta_list = []
        for row in tableRows[3:-2]:
            # try:
            #     print(row.ele("css:td:nth-child(0)>a:nth-child(1)").text)
            #     print(row.ele("css:td:nth-child(1)>a:nth-child(2)").text)
                infoHref = row.ele("css:td:nth-child(1) a:nth-child(2)").attr("href")
                # print(infoHref)
                id = infoHref.split("=")[-1]
                name = row.ele("css:td:nth-child(1) a:nth-child(2)").text.strip()
                meta_list.append({"person_url":infoHref, "id": id, "name": name})
            # except Exception as e:
            #     print("getInforList_:", e)
            #     continue
        # 严格并发控制：Semaphore 控制同时进入 getAuthorInfo 的数量
        semaphore = threading.Semaphore(self.MAX_THREAD)
        lock_new_tab = threading.Lock()

        def _worker(meta):
            """wrapper: 在进入关键区域前获取 semaphore"""
            acquired = semaphore.acquire(timeout=300)  # 可设置超时，防止死锁
            if not acquired:
                return (500, {"error": "semaphore timeout", "meta": meta})
            try:
                # 可选调试信息：当前线程数
                print("THREAD START", meta["id"], "active_threads:", threading.active_count())
                return self.getAuthorInfo(lock_new_tab, meta)
            finally:
                print("THREAD END", meta["id"], "active_threads:", threading.active_count())
                semaphore.release()

        with ThreadPoolExecutor(max_workers=self.MAX_THREAD) as ex:
            futs = [ex.submit(_worker, lock_new_tab, meta) for meta in meta_list]  # 线程列表
            for fut in as_completed(futs):
                stat_code, data_dict = fut.result()
                if stat_code == 200:
                    print(data_dict)
                    infoList.append(data_dict)
                # 每五条消息保存一次
                if len(infoList) >= 10:
                    self.saveInfo2(infoList)

                    infoList.clear()
                    exit(1)

        # except Exception as e:
        #     print(self.searchName, "getInfoList:", e)
        # todo 将本次拿到的id(selectedList)保存起来最后去重用
        # print(selectedStr)

    def saveInfo(self, infoList, searchName):
        if infoList is not None:
            bid_path = "./info/"
            if os.path.exists(bid_path) == False:
                os.makedirs(bid_path)
            try:
                with open(
                    bid_path + current_date + "_" + searchName + ".json",
                    "w",
                    encoding="utf-8",
                ) as f2:
                    json.dump(infoList, f2, ensure_ascii=False, indent=4)

            except Exception as e:
                print(e)
                print("info数据写入失败")
            finally:
                pass
        else:
            print("无内容")

    def saveInfo2(self, infoList):
        if infoList is not None:
            base_path = "./completeInfo/" + f"{self.field}/"
            if os.path.exists(base_path) == False:
                os.makedirs(base_path)
            try:
                fileName = base_path + self.searchName + ".json"
                with open(
                    fileName,
                    mode="a",
                    encoding="utf-8",
                ) as f1:
                    json.dump(infoList, f1, ensure_ascii=False, indent=4)
                    print(
                        "成功在",
                        base_path  + self.searchName + ".json",
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
    crawler = treeApi('http://neurotree.org/neurotree/', 'v', 1)
    crawler.getInfoListAndSaveCompleteAuthorInfo()