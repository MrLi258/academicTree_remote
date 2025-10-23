import os
import sys

from treeApi.treeApi import treeApi

from utils import load_progress, save_progress
from utils import clean_progress
from config import PROGRESS_FILE, IF_DEBUG, fieldPart
"""
获取各个领域的作者id以及其父子id
"""



def main(fieldPart):
    """
    fieldPart: 表示要爬取的领域部分，这里只分了两部分
    """
    # 根据参数确定爬取的领域区间
    mid_idx = 73 // 2
    if fieldPart == 1:
        start_idx = 0
        end_idx = mid_idx
    elif fieldPart == 2:
        start_idx = mid_idx
        end_idx = 74
    else:
        raise KeyError

    searchList = "abcdefghijklmnopqrstuvwxyz"

    # # 一次性函数，用于保存所有的领域链接
    # a.getFileds()

    # 记录爬取过的领域和字母避免重复爬取
    folder_path = "./info"
    exist_file_names = set()
    for file in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file)
        if not os.path.isfile(file_path):
            field = file
            for be_file in os.listdir(file_path):
                character = be_file.split("_")[3].replace(".json", "")
                exist_file_names.add(field + "_" + character)
                # print(field + '_' + character)

    with open("./fieldsHref.txt", mode="r", encoding="utf8") as rfile:
        href_list = rfile.readlines()
        href_list = [item.replace("\n", "") for item in href_list]

    href_list = href_list[start_idx:end_idx]

    filePath = PROGRESS_FILE + str(fieldPart) + ".json"
    # print(filePath, os.path.exists(filePath))
    if os.path.exists(filePath):
        last_field, last_char = load_progress(fieldPart)  # 加载上一次爬取进度
    else:
        # save_progress(href_list[0].split('/')[3], 'a', fieldPart)  # 文件夹不存在需初始化
        last_field, last_char = load_progress(fieldPart)

    # print(f'上一次爬取的领域是；{last_field}， {last_char}')

    for f_idx, base_href in enumerate(href_list):
        current_field = base_href.split("/")[3]
        print(current_field)

        # 断点续爬逻辑
        start_char = 0
        if last_field:  #
            if current_field != last_field:
                continue  # print('还没到断点处')
            elif (
                current_field == last_field and last_char == "z"
            ):  # 上一次是某一个领域的最后一个字母，这次需更新到新的领域的第一个字母
                print(
                    f"折回到第{f_idx + 1}个领域了， {href_list[f_idx + 1].split('/')[3]}"
                )
                current_field = href_list[f_idx + 1].split("/")[3]
                base_href = href_list[f_idx + 1]
                last_char = 0
                print(f"回到断点 {current_field}_{searchList[start_char]}处")
            elif (
                current_field == last_field and last_char != "z"
            ):  # 非最后一个字母，直接字母索引加一即可
                start_char = (searchList.index(last_char) + 1) if last_char else 0
                print(f"回到断点 {current_field}_{searchList[start_char]}处")
                last_field = None  # 只处理一次

        for char_idx in range(start_char, len(searchList)):
            current_char = searchList[char_idx]
            current_field_character = current_field + "_" + current_char
            if current_field_character in exist_file_names:
                print(f"{current_field_character}爬取过了，跳过")
                continue
            try:
                a = treeApi(
                    field=current_field,
                    searchName=current_char,
                    fieldPart=fieldPart,
                    if_debug=IF_DEBUG,
                )
                print(current_char)
                infoList = a.getInfoList(base_href)
                if bool(infoList):
                    a.saveInfo2(infoList)
                del a
            except SystemExit as e:
                if e.code == 1:
                    print("[主程序] 检测到封禁，触发重启")
                    sys.exit(1)  # 传递退出码给监控脚本
            finally:
                ...
    else:
        # 所有任务完成后清理进度
        clean_progress(fieldPart)


if __name__ == "__main__":
    main(fieldPart)
