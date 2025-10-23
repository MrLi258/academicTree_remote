import json
import os
from collections import defaultdict
from pathlib import Path

def getFields():
    with open("./fieldsHref.txt", mode="r", encoding="utf8") as rfile:
        href_list = rfile.readlines()
        href_list = [item.replace("\n", "") for item in href_list]

    return href_list

def deduplicate_authors():
    """
    对info文件夹下所有JSON文件中的作者信息进行去重，根据id字段去重
    """
    info_dir = Path("info")
    all_authors = {}  # 使用字典存储，key为id，value为作者信息
    processed_files = []
    total_files = 0
    href_list = getFields()
    # 遍历info文件夹下的所有子文件夹
    for subdir in info_dir.iterdir():
        if subdir.is_dir():
            print(f"处理文件夹: {subdir.name}")
            for href in href_list:
                if subdir.name in href:
                    baseHref = href
                    break


            # 遍历子文件夹中的所有JSON文件
            for json_file in subdir.glob("*.json"):
                print(json_file)
                total_files += 1
                try:
                    with open(json_file, 'r', encoding='utf-8') as f:
                        authors = json.load(f)
                    
                    # 处理每个作者
                    for author in authors:
                        if isinstance(author, dict) and 'id' in author:
                            author_id = author['id']
                            # 如果id不存在，则添加；如果已存在，保留第一个（或者可以选择保留最新的）
                            if author_id not in all_authors:
                                author["baseHref"] = baseHref
                                all_authors[author_id] = author
                    
                    processed_files.append(str(json_file))
                    print(f"  已处理: {json_file.name} (包含 {len(authors)} 个作者)")
                    
                except Exception as e:
                    print(f"  处理文件 {json_file} 时出错: {e}")
    
    # 将去重后的作者信息保存到新文件
    deduplicated_authors = list(all_authors.values())
    
    # 保存去重后的数据
    output_file = "deduplicated_authors.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(deduplicated_authors, f, ensure_ascii=False, indent=4)
    
    # 生成统计报告
    report_file = "deduplication_report.txt"
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write("作者信息去重报告\n")
        f.write("=" * 50 + "\n\n")
        f.write(f"处理的文件夹数量: {len([d for d in info_dir.iterdir() if d.is_dir()])}\n")
        f.write(f"处理的JSON文件数量: {total_files}\n")
        f.write(f"去重前总作者数量: 无法精确统计（需要重新读取所有文件）\n")
        f.write(f"去重后作者数量: {len(deduplicated_authors)}\n")
        f.write(f"输出文件: {output_file}\n\n")
        
        f.write("处理的文件列表:\n")
        for file_path in processed_files:
            f.write(f"  - {file_path}\n")
    
    print(f"\n去重完成!")
    print(f"去重后作者数量: {len(deduplicated_authors)}")
    print(f"结果已保存到: {output_file}")
    print(f"报告已保存到: {report_file}")
    
    return deduplicated_authors

def get_detailed_statistics():
    """
    获取详细的统计信息，包括去重前后的对比
    """
    info_dir = Path("info")
    all_authors_before = []
    all_authors_after = {}
    
    # 重新读取所有文件获取去重前的总数
    for subdir in info_dir.iterdir():
        if subdir.is_dir():
            for json_file in subdir.glob("*.json"):
                try:
                    with open(json_file, 'r', encoding='utf-8') as f:
                        authors = json.load(f)
                    all_authors_before.extend(authors)
                    
                    # 同时进行去重
                    for author in authors:
                        if isinstance(author, dict) and 'id' in author:
                            author_id = author['id']
                            if author_id not in all_authors_after:
                                all_authors_after[author_id] = author
                except Exception as e:
                    print(f"统计时处理文件 {json_file} 出错: {e}")
    
    print(f"去重前总作者数量: {len(all_authors_before)}")
    print(f"去重后作者数量: {len(all_authors_after)}")
    print(f"去重数量: {len(all_authors_before) - len(all_authors_after)}")
    
    return len(all_authors_before), len(all_authors_after)

if __name__ == "__main__":
    # print(getFields())
    print("开始对作者信息进行去重...")
    deduplicated_authors = deduplicate_authors()

    print("\n获取详细统计信息...")
    before_count, after_count = get_detailed_statistics()

    print(f"\n最终统计:")
    print(f"去重前: {before_count} 个作者")
    print(f"去重后: {after_count} 个作者")
    print(f"去重数量: {before_count - after_count} 个作者")