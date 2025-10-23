import json
import re
from jsonFileRepair import replaceFileInplace
def testNum():
    with open('./completeAuthorInfo.json', mode='r', encoding="utf8") as rfile:
            json_list = json.load(rfile)
            print(len(json_list))
    # for dic in json_list:
    #      print(dic)

def test_num_of_parent_and_child():
     total_num = 0
     child_num = 0
     parent_num = 0
     complete_num = 0
     with open('./completeAuthorInfo.json', mode='r', encoding="utf8") as rfile:
        json_list = json.load(rfile)
        total_num = len(json_list)
        print(total_num)
        for dic in json_list:
            home_id = dic['id']
            parents = dic.get('parentsIdList')
            childs = dic.get('childrenIdList')
            parents_exist = False
            childs_exist = False
            if parents:
                parents_exist = True
                parents_dic = {parent_id:False for parent_id in parents}
            if childs:
                childs_exist = True
                childs_dic = {child_id:False for child_id in childs}
            parent_flag = False
            children_flag = False
            for dic in json_list:
                id = dic['id']
                if parents_exist:
                    if not parent_flag:
                      if any([id == parent for parent in parents]):
                        parents_dic[id] = True
                        parent_num += 1
                        print(f'{home_id}找到一个parent{id}')
                        if all([parents_dic[key] for key in parents]):
                            parent_flag = True
                if childs_exist:
                    if not children_flag:
                        if any([id == children for children in childs]):
                            childs_dic[id] = True
                            child_num += 1
                            print(f'{home_id}找到一个children{id}')
                            if all([childs_dic[key] for key in childs]):
                                children_flag = True
                if parent_flag and children_flag:
                    complete_num += 1
                    break
        print(f'最终发现总共有{complete_num}个作者有完整的child 和 parent, 其中child数量为{child_num}, parent数量为{parent_num}')

if __name__ == '__main__':
    # line = ', "baseHref": "https://academictree.org/arthistory/"},,,,,,,,,,{"parentsIdList":'
    # new_line = re.sub(r',+', ',', line)
    # print(new_line)
    replaceFileInplace('./completeAuthorInfo.json')
    testNum()
    test_num_of_parent_and_child()

    # with  open('./completeAuthorInfo.json', 'r', encoding='utf-8') as f:
    #     while True:
    #         line = f.readline()

    
    #         # print(line, type(line))
    #         print(line[98450200:98450363])
    #         line = json.loads(line)s
    #         print(line, type(line))
    #         break
    #         if not line:  # 到 EOF，返回空字符串，则终止循环
    #             break
    #         js = json.loads(line)