import os
import json
import time

import mysql.connector


# 连接MySQL数据库
def connect_to_mysql():
    return mysql.connector.connect(
        host="localhost",  # MySQL服务器地址
        user="root",  # MySQL用户名
        password="258456396ljt",  # MySQL密码
        database="academictree"  # 数据库名
    )


#
def check_author_exists(cursor, author_id):

    # 查询语句（使用参数化查询）
    query = "SELECT EXISTS(SELECT 1 FROM author_pool WHERE id = %s)"
    cursor.execute(query, (author_id,))

    # 获取结果
    exists = cursor.fetchone()[0]  # 返回1或0

    # 返回布尔值
    return bool(exists)

# 给定个人主页的链接爬取完善 个人信息
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

