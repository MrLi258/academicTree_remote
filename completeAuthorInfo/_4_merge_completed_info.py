
import json
import os
from multiprocessing import get_context, Process, Queue, Manager
from _2_get_and_save_new_pcc_info_from_old_info import process_writer


def merge_completed_info():
    # 避免重复写入
    idWithField2url = {}
    for filename in os.listdir('./completeAuthorJsons'):  # ./completeAuthorJsons  finalAuthorJsons
        if filename.endswith(".jsonl"):
            file_path = os.path.join('./completeAuthorJsons', filename)
            # 读取并解析JSON文件
            with open(file_path, 'r', encoding='utf-8') as file:
                for line in file:
                    entry = json.loads(line)
                    if 'idWithField'  in entry:
                        idWithField = entry.get('idWithField')
                        if idWithField not in idWithField2url:  #
                            idWithField2url[idWithField] = entry  # 动态去重

    print(len(idWithField2url.items()))
    # 使用 Manager 创建共享的队列
    with Manager() as manager:
        data_queue = manager.Queue()  # 通过 Manager 创建的 Queue 是可以跨进程共享的
        # 启动主进程的写入线程
        base_name = f'./completeAuthorJsons/completeAuthorJson'  # 写入的文件夹以及文件名  #./completeAuthorJsons/completeAuthorInfo            ./finalAuthorJsons/finaleAuthorInfo
        writer_process = Process(target=process_writer, args=(data_queue, 1, base_name))
        writer_process.start()


        for filename in os.listdir('./temporaryAuthorJsons'):  # ./temporaryAuthorJsons           completeAuthorJsons
            if filename.endswith(".jsonl"):
                file_path = os.path.join('./temporaryAuthorJsons', filename)
                # 读取并解析JSON文件
                with open(file_path, 'r', encoding='utf-8') as file:
                    print(f'进入文件{file_path}')
                    for line in file:
                        entry = json.loads(line)
                        # 把该作者所有的父子合作节点添加进将要爬取的作者中
                        if entry['idWithField'] not in idWithField2url:
                            data_queue.put(entry)  # 往输送管道内传输数据，传输管道送到writer_process进程内把数据写入到base_name的文件夹内
                            idWithField2url[entry['idWithField']] = entry


        # 数据全部传输完之后通知writer_process进程
        data_queue.put('DONE')
        writer_process.join()





if __name__ == '__main__':
    merge_completed_info()
