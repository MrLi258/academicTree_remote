import os
import sys

from selenium import webdriver
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.chrome.service import Service
import time
import json
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
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
        field,
        searchName,
        fieldPart,
        proxies=None,
        driver_path=driver_path,
        if_debug=True,
        user_file_path="D:\\Default",
    ):
        self.field = field
        self.searchName = searchName
        self.proxies = proxies
        self.fieldPart = fieldPart
        if self.proxies != None:
            os.environ["http_proxy"] = proxies["http_proxy"]  # "http://127.0.0.1:10809"
            os.environ["https_proxy"] = proxies[
                "https_proxy"
            ]  # "http://127.0.0.1:10809"

        self.driver_path = driver_path
        self._create_driver(if_debug=if_debug, user_file_path=user_file_path)
        self.last_call = 0.0
        self.count = 0

    def _create_driver(self, if_debug=True, user_file_path="D:\\Default"):
        options = webdriver.ChromeOptions()

        # 禁用自动化特征
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_argument("--disable-web-security")

        # 随机化浏览器指纹
        options.add_argument("--window-size=1366,768")
        options.add_argument("--lang=zh-CN")

        if not if_debug:
            options.add_argument("--headless")
        options.add_argument("--user-data-dir=" + user_file_path)
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")  # 防止内存不足问题

        service = Service(executable_path=self.driver_path)

        driver = webdriver.Chrome(service=service, options=options)
        driver.set_page_load_timeout(3000)  # 设置为300秒
        self.driver = driver
        self.driver.maximize_window()

    def __del__(self):
        if hasattr(self, "driver") and self.driver:
            try:
                self.driver.quit()  # 改用 quit() 确保完全退出
            except Exception as e:
                print(f"关闭driver时出错: {e}")

    def _is_ip_blocked(self):
        """通过页面特征检测是否被封禁"""
        try:
            # 特征1：检测特定封禁关键词
            if "blocked" in self.driver.page_source.lower():
                return True

            if "not found" in self.driver.page_source.lower():
                return True

            if "无法访问此网站" in self.driver.page_source:
                return True

            # 特征2：验证码元素存在
            if len(self.driver.find_elements(By.ID, "captcha")) > 0:
                return True

            # 特征3：异常标题
            if "Access Denied" in self.driver.title:
                return True

            return False
        except WebDriverException:
            return False  # 防止页面未加载完成时报错

    def getFileds(self):
        url = "https://academictree.org/"
        self.driver.get(url)
        time.sleep(2)
        field_list = []
        try:
            soup = BeautifulSoup(self.driver.page_source, "html.parser")
            hrefRow = soup.select("td>a")
            print(len(hrefRow))
            for row in hrefRow:
                href = row.get("href")
                if href:
                    field_list.append(href)
        except Exception as e:
            print(e)
        if bool(field_list):
            with open(
                "../academicTree_new/fieldsHref.txt",
                mode="w",
                encoding="utf8",
                newline="\n",
            ) as wfile:
                for field in field_list:
                    wfile.write(field)
                    wfile.write("\n")

        ...

    def getInfoList(self, baseHref):
        url = (
            baseHref
            + "/peoplelist.php?searchname="
            + self.searchName
            + "&searchalltrees=1&allfields=1"
        )
        print(url)
        # https://academictree.org/publicpolicy/peoplelist.php?searchname=h&searchalltrees=1&pidconn=-1&allfields=1
        # https://academictree.org/publicpolicy/peoplelist.php?searchname=h&searchalltrees=1&allfields=1
        self.driver.get(url)
        # input('解锁网站')
        time.sleep(5)
        # IP检测
        if self._is_ip_blocked():
            print("[!] IP封禁检测（列表页）")
            sys.exit(1)  # 触发重启

        # 只要ip没被封就记录
        save_progress(self.field, self.searchName, self.fieldPart)

        try:
            soup = BeautifulSoup(self.driver.page_source, "html.parser")
            tableRow = soup.select("tbody")[0].select("tr")
            saveList = []
            selectedStr = ""

            print(
                f"领域{baseHref}内包含{self.searchName}的作者有{len(tableRow) - 3} 个"
            )
            for i in range(3, len(tableRow)):
                try:
                    items = tableRow[i]
                    infoHref = items.select("td")[0].select("a")[1].get("href")
                    # print(infoHref)
                    info = self.getInfo(infoHref)
                    # info['parentsIdList'] = infoTemp['parentsIdList']
                    # info['childrenIdList'] = infoTemp['childrenIdList']
                    info["id"] = infoHref.split("=")[1]
                    info["name"] = items.select("td")[0].select("a")[0].text.strip()
                    # info['html'] = soup.prettify()
                    saveList.append(info)
                    # 每五条消息保存一次
                    if len(saveList) >= 1:
                        self.saveInfo2(saveList)
                        saveList.clear()
                    selectedStr = selectedStr + info["id"] + "_"
                    print(info)
                except Exception as e:
                    print("getInforList_:", e)
                    continue
            with open("./selectedNode.txt", "a", encoding="utf-8") as f:
                f.write(selectedStr)

            return saveList

        except Exception as e:
            print(self.searchName, "getInfoList:", e)
        # todo 将本次拿到的id(selectedList)保存起来最后去重用
        # print(selectedStr)

    def getInfo(self, infoHref):
        head = f"https://academictree.org/{self.field}/"
        # print(head + infoHref)
        if "http://" in infoHref:
            print(f"{self.field}的{self.searchName}没有对应的科学家，记录已爬取并跳过")
            save_progress(self.field, self.searchName, self.fieldPart)
        self.driver.get(head + infoHref)
        time.sleep(20)
        # IP检测
        if self._is_ip_blocked():
            print("[!] IP封禁检测（列表页）")
            sys.exit(1)  # 触发重启
        # input('请你输入任意字符继续程序')
        try:
            soup = BeautifulSoup(self.driver.page_source, "html.parser")
            info = {}
            # 获取parentsIdList
            parentsRow = soup.select("table")[1].select("tr")
            # print('parentsRow:', len(parentsRow))
            parentsIdList = []
            if parentsRow is not None:
                for i in range(len(parentsRow)):
                    items = parentsRow[i]
                    # print(items)
                    item = items.select("td")[0]
                    if item.find_all(True):
                        parentId = item.select("a")[0].get("href").split("=")[1]
                        parentsIdList.append(parentId)
            info["parentsIdList"] = parentsIdList
            # 获取childrenIdList
            childrenRow = soup.select("table")[2].select("tr")
            # print('childrenRow:',len(childrenRow))
            childrenIdList = []
            if childrenRow is not None:
                for i in range(len(childrenRow)):
                    items = childrenRow[i]
                    item = items.select("td")[0]
                    if item.find_all(True):
                        childId = item.select("a")[0].get("href").split("=")[1]
                        childrenIdList.append(childId)
            info["childrenIdList"] = childrenIdList
        except Exception as e:
            print("getInfo:", e)
        # print(info)
        return info

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
            bid_path = "./info/" + f"{self.field}/"
            if os.path.exists(bid_path) == False:
                os.makedirs(bid_path)
            try:
                fileName = bid_path + current_date + "_" + self.searchName + ".json"
                with open(
                    fileName,
                    mode="a",
                    encoding="utf-8",
                ) as f2:
                    json.dump(infoList, f2, ensure_ascii=False, indent=4)
                    print(
                        "成功在",
                        bid_path + current_date + "_" + self.searchName + ".json",
                        "文件中保存1条数据",
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
