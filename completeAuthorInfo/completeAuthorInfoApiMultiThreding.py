import csv
import json
import os
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup
from requests import RequestException
from jsonFileRepair import replaceFileInplace

lock = threading.Lock()
output_lock = threading.Lock()

def getExistAuthorInfo():
    filePath = './completeAuthorInfo.json'
    idSet = set()
    if not os.path.exists(filePath) or os.path.getsize(filePath) == 0:
        return idSet
    with open(filePath, 'r', encoding='utf-8') as f:
        authorInfos = json.load(f)
        for authorinfo in authorInfos:
            idSet.add(authorinfo['id'])
    return idSet

def create_not200_csv_if_not_exists():
    file_path = './not200Author.csv'
    headers = ['name', 'url', 'state_code']
    if not os.path.exists(file_path):
        with open(file_path, mode='w', encoding='utf-8', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(headers)

def fetch_url(url, timeout=20, max_retries=3):
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()
            return response
        except RequestException as e:
            print(f"[重试 {attempt+1}] 请求失败：{url}，原因：{e}")
            time.sleep(2)
    return None

def urlGenerator():
    with open("../deduplicated_authors.json", mode="r", encoding="utf8") as rfile:
        json_data = json.load(rfile)
    existAuthorInfos = getExistAuthorInfo()
    for data in json_data:
        href = data["baseHref"] + "peopleinfo.php?pid=" + data["id"]
        if data["id"] in existAuthorInfos:
            print(f"{data['id']} <已经爬取过了>")
            continue
        yield (data, href)

def infoExtractor(data_dict, str_data):
    lines = str_data.strip().split("\n")
    for line in lines:
        if ":" in line:
            key_value_pairs = line.split(":", 1)
            key = key_value_pairs[0].strip()
            value = key_value_pairs[1].strip()
            if "Mean distance:" in value:
                data_dict[key] = value.replace("Mean distance:", "").strip().strip('"')
                data_dict["Mean distance"] = value.split("Mean distance:")[1].strip().strip('"')
            else:
                data_dict[key] = value
        else:
            if data_dict:
                last_key = list(data_dict.keys())[-1]
                data_dict[last_key] += " " + line.strip()

def getAuthorInfo(meta, url):
    name = meta['name']
    response = fetch_url(url)
    if not response:
        write_failed(name, url, -1)
        return None
    if response.status_code != 200:
        write_failed(name, url, response.status_code)
        return None

    soup = BeautifulSoup(response.text, 'html.parser')
    data_dict = {}
    personInf_tag = soup.find('div', attrs={'class': 'personinfo'})
    if personInf_tag:
        allPersonInfo = personInf_tag.text
        allPersonInfo = allPersonInfo.replace(name, "", 1).replace(', Ph.D', '', 1).strip()
    else:
        allPersonInfo = ""

    tables = soup.select('.personinfo > table')
    for table in tables:
        allPersonInfo = allPersonInfo.replace(table.text, '')
        infoExtractor(data_dict, table.text)
    infoExtractor(data_dict, allPersonInfo)
    meta.update(data_dict)
    return meta

def write_failed(name, url, code):
    with output_lock:
        with open('./not200Author.csv', mode='a', encoding="utf8", newline='') as wfile:
            writer = csv.writer(wfile)
            writer.writerow([name, url, code])

def getCompleteAuthorInfoMultiThread(max_threads=10):
    all_results = []
    batch_size = 50
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = []
        for meta, url in urlGenerator():
            futures.append(executor.submit(getAuthorInfo, meta, url))

        for future in as_completed(futures):
            result = future.result()
            if result:
                all_results.append(result)

            if len(all_results) >= batch_size:
                save_results(all_results)
                all_results.clear()

    if all_results:
        save_results(all_results)

def save_results(results):
    with output_lock:
        with open('./completeAuthorInfo.json', mode='a', encoding="utf8") as wfile:
            json.dump(results, wfile)
        print(f"✅ 写入 {len(results)} 条作者信息")

if __name__ == "__main__":
    create_not200_csv_if_not_exists()
    getCompleteAuthorInfoMultiThread(max_threads=10)
    replaceFileInplace('./completeAuthorInfo.json')
