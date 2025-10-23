import csv
import json
import os
import time

import requests
from bs4 import BeautifulSoup
from lxml import html
from requests import RequestException
from jsonFileRepair import replaceFileInplace

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

def fetch_url(url, timeout=20, max_retries=3):
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()
            return response
        except RequestException as e:
            print(f"第{attempt+1}次请求失败：{url}，原因：{e}")
            time.sleep(2)  # 重试前稍作停顿

    # 若全部重试失败，返回None
    print(f"访问超时{url}")
    # with open("failed_urls.txt", "a") as f:
    #     f.write(f"{url}\n")
    return None

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

def infoExtractor(data_dict, str_data):
    # 将字符串按行分割
    lines = str_data.strip().split("\n")

    # 遍历每一行并解析键值对
    for line in lines:
        # 如果该行包含冒号，则处理为键值对
        if ":" in line:
            # 使用 split(':', 1) 只对第一个冒号进行分割
            key_value_pairs = line.split(":", 1)

            # 处理第一个键值对
            key = key_value_pairs[0].strip()
            value = key_value_pairs[1].strip()
            # 检查是否还有额外的冒号，比如 "Google: "Florence Brunel"Mean distance:"
            if "Mean distance:" in value:
                # print(f'key:{key}')
                data_dict[key] = value.replace("Mean distance:", "").strip().strip('"')


                key_value_pairs2 = line.split("Mean distance:", 1)
                # 提取 "Florence Brunel" 这部分，并将其拆分成适当的键值对
                data_dict["Mean distance"] = key_value_pairs2[1].strip().strip('"')
            else:
                data_dict[key] = value
        else:
            # 检查字典是否为空，防止出现 IndexError
            if data_dict:
                # 如果字典不为空，获取字典中最后一个键
                last_key = list(data_dict.keys())[-1]  # 获取字典中最后一个键
                data_dict[last_key] += " " + line.strip()  # 将内容附加到最后一个键的值上
            else:
                # 如果字典为空，给出错误处理或跳过这行
                print("字典为空，无法更新值")
                continue  # 可以选择跳过或做其他处理

    # 输出字典
    # print(data_dict)

def getAuthorInfo(name, url):
    response = fetch_url(url)
    # print(response.text)
    if bool(response):
        if response.status_code != 200:
            print(f'{url}这个网页无法正常访问')
            with open('./not200Author.csv', mode='a', encoding="utf8", newline='') as wfile:
                wirter = csv.writer(wfile)
                wirter.writerow([name, url, response.status_code])
            return (response.status_code, None)
    else:
        with open('./not200Author.csv', mode='a', encoding="utf8", newline='') as wfile:
            wirter = csv.writer(wfile)
            wirter.writerow([name, url, -1])
        return (-1, None)
    # 创建 BeautifulSoup 对象
    soup = BeautifulSoup(response.text, 'html.parser')

    # 初始化一个空字典
    data_dict = {}


    personInf_tag = soup.find('div', attrs={'class': 'personinfo'})
    if personInf_tag:
        allPersonInfo = personInf_tag.text
        # print(f'name: {name}')
        allPersonInfo = allPersonInfo.replace(name, "",1).replace(', Ph.D', '',1).strip()
    personInf_table_tag = soup.select('.personinfo > table')

    if personInf_table_tag:
        for table in personInf_table_tag:
            # print("table: ",table.text)
            allPersonInfo = allPersonInfo.replace(table.text, '')
            infoExtractor(data_dict, table.text)

        # print("allPersonInfo: ", allPersonInfo)
        infoExtractor(data_dict, allPersonInfo)
        # print(f'提取后的info：{data_dict}')
    # 提取发表论文信息
    data_dict['Publications'] = []
    publicationEles = soup.select('.rightcol .container tbody .clickable-row')
    if publicationEles:
        for publicationEle in publicationEles:
            publication = publicationEle.text
            print(publication)
            data_dict['Publications'].append(publication)
    return (response.status_code, data_dict)

def getCompleteAuthorInfo():
    infoList = list()
    for meta, url in urlGenerator():
        print(url)
        stat_code, dic = getAuthorInfo(meta['name'], url)
        if stat_code == 200:
            print(stat_code, dic)
            meta.update(dic)
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
            infoList.clear()
        print(f'{len(infoList)}条数据写成功')

    ...

if __name__ == "__main__":
    create_not200_csv_if_not_exists()
    getCompleteAuthorInfo()
    replaceFileInplace('./completeAuthorInfo.json')
    # getAuthorInfo("Florence M. Brunel", "https://academictree.org/chemistry/peopleinfo.php?pid=305745")