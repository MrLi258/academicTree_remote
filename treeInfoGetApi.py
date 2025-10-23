import os
from treeApi.treeApi import treeApi

##########################################################################################

if __name__ == "__main__":

    searchList = "abcdefghijklmnopqrstuvwxyz"
    # # 一次性函数，用于保存所有的领域链接
    # a.getFileds()


    folder_path = './info'
    exist_file_names = set()
    for file in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file)
        if not os.path.isfile(file_path):
            field = file
            for be_file in os.listdir(file_path):
                character = be_file.split('_')[3].replace('.json', '')
                exist_file_names.add(field + '_' + character)
                print(field + '_' + character)

    with open('./fieldsHref.txt', mode='r', encoding='utf8') as rfile:
        href_list = rfile.readlines()
        href_list = [item.replace('\n', '') for item in href_list]
    for base_href in href_list[4:]:
        field = base_href.split('/')[3]
        print(field)
        for i in searchList:
            field_character = field + '_' + i
            if field_character in exist_file_names:
                print(f'{field_character}爬取过了，跳过')
                continue
            a = treeApi(field=field, searchName=i, if_debug=True)
            print(i)
            infoList = a.getInfoList(base_href)
            if bool(infoList):
                a.saveInfo2(infoList)
            del a

