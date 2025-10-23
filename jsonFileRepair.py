import fileinput
import json
import os


def repairFunc(s_fileName, d_fileName):

    with open(s_fileName, mode='r', encoding='utf8', newline='\n') as rfile:
        with open(d_fileName, mode='w', encoding='utf8', newline='\n') as wfile:
            lines = rfile.readlines()
            for line in lines:
                if '][' in line:
                    line = ',\n'
                wfile.write(line)


def json_load_test(fileName):
    with open(fileName, mode='r', encoding='utf8') as rfile:
        dic = json.load(rfile)
        print(type(dic))
        print(dic)

def replaceFile_test(s_fileName,d_fileName):
    os.replace(s_fileName, d_fileName)

# 用这个方法即可，直接在原文件上进行替换修改
def replaceFileInplace(fileName):
    with fileinput.input(fileName, mode='r', inplace=True, encoding='utf8') as rfile:
        for line in rfile:
            if '][' in line:
                print(line.replace("][", ","), end="")
            else:
                print(line, end='')

def main():
    folder_path = './info'
    for file in os.listdir(folder_path):
        # print(file)
        file_path = os.path.join(folder_path, file)
        # print(os.path.isfile(file_path))
        if os.path.isfile(file_path):
            replaceFileInplace(file_path)
            ...
        else:
            for beFile in os.listdir(file_path):
                beFile_path = os.path.join(file_path, beFile)
                # print(beFile_path)
                if os.path.isfile(beFile_path):
                    replaceFileInplace(beFile_path)
                    ...

if __name__ == '__main__':
    json_load_test('./2025_04_21_e.json')
    # 修复之info文件夹下的所有相关文件
    # main()


