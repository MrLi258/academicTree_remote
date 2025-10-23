import json
import os
import math


def split_json_file(input_file, max_size_mb=80):
    """
    将大JSON文件按指定大小限制进行拆分

    参数:
        input_file (str): 输入的JSON文件路径
        max_size_mb (int): 每个拆分文件的最大大小(MB)，默认为80MB
    """

    # 将MB转换为字节
    max_size_bytes = max_size_mb * 1024 * 1024

    # 检查输入文件是否存在
    if not os.path.exists(input_file):
        print(f"错误: 文件 '{input_file}' 不存在")
        return

    # 获取原文件信息
    file_size = os.path.getsize(input_file)
    file_name = os.path.basename(input_file)
    file_dir = os.path.dirname(input_file) or '.'
    file_base_name = os.path.splitext(file_name)[0]

    print(f"原始文件: {file_name}")
    print(f"文件大小: {file_size / (1024 * 1024):.2f} MB")
    print(f"文件路径: {file_dir}")

    # 计算预计的拆分文件数量
    estimated_parts = math.ceil(file_size / max_size_bytes)
    print(f"预计将拆分为 {estimated_parts} 个文件")

    try:
        # 读取原始JSON文件
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # 检查JSON数据类型并相应处理
        if isinstance(data, list):
            split_list_data(data, max_size_bytes, file_dir, file_base_name)
        elif isinstance(data, dict):
            split_dict_data(data, max_size_bytes, file_dir, file_base_name)
        else:
            print(f"不支持的JSON数据类型: {type(data)}")

    except json.JSONDecodeError as e:
        print(f"JSON解析错误: {e}")
    except Exception as e:
        print(f"处理文件时发生错误: {e}")


def split_list_data(data_list, max_size_bytes, output_dir, file_base_name):
    """拆分列表类型的JSON数据"""
    current_size = 0
    current_chunk = []
    chunk_number = 1
    total_items = len(data_list)

    print(f"\n开始拆分包含 {total_items} 个项目的列表...")

    for i, item in enumerate(data_list):
        # 估算当前项目的大小
        item_size = len(json.dumps(item, ensure_ascii=False).encode('utf-8'))

        # 如果添加当前项目会超过大小限制，且当前块不为空，则保存当前块
        if current_size + item_size > max_size_bytes and current_chunk:
            save_chunk(current_chunk, output_dir, file_base_name, chunk_number)
            chunk_number += 1
            current_chunk = []
            current_size = 0

        # 添加项目到当前块
        current_chunk.append(item)
        current_size += item_size

        # 显示进度
        if (i + 1) % 1000 == 0 or (i + 1) == total_items:
            progress = (i + 1) / total_items * 100
            print(f"进度: {i + 1}/{total_items} ({progress:.1f}%)...")

    # 保存最后一个块
    if current_chunk:
        save_chunk(current_chunk, output_dir, file_base_name, chunk_number)

    print(f"\n拆分完成！共生成 {chunk_number} 个文件")


def split_dict_data(data_dict, max_size_bytes, output_dir, file_base_name):
    """拆分字典类型的JSON数据"""
    items = list(data_dict.items())
    current_size = 0
    current_chunk = {}
    chunk_number = 1
    total_items = len(items)

    print(f"\n开始拆分包含 {total_items} 个键值对的字典...")

    for i, (key, value) in enumerate(items):
        # 估算当前键值对的大小
        item_size = len(json.dumps({key: value}, ensure_ascii=False).encode('utf-8'))

        # 如果添加当前键值对会超过大小限制，且当前块不为空，则保存当前块
        if current_size + item_size > max_size_bytes and current_chunk:
            save_chunk(current_chunk, output_dir, file_base_name, chunk_number)
            chunk_number += 1
            current_chunk = {}
            current_size = 0

        # 添加键值对到当前块
        current_chunk[key] = value
        current_size += item_size

        # 显示进度
        if (i + 1) % 1000 == 0 or (i + 1) == total_items:
            progress = (i + 1) / total_items * 100
            print(f"进度: {i + 1}/{total_items} ({progress:.1f}%)...")

    # 保存最后一个块
    if current_chunk:
        save_chunk(current_chunk, output_dir, file_base_name, chunk_number)

    print(f"\n拆分完成！共生成 {chunk_number} 个文件")


def save_chunk(data, output_dir, file_base_name, chunk_number):
    """保存数据块到文件，文件名格式为: 原文件名_part{num}.json"""
    output_file = os.path.join(output_dir, f"{file_base_name}_part{chunk_number}.json")

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    file_size = os.path.getsize(output_file)
    print(f"已创建: {output_file} ({file_size / (1024 * 1024):.2f} MB)")


def verify_split_files(original_file):
    """验证拆分文件的完整性"""
    print("\n开始验证拆分文件...")

    # 获取原文件信息
    file_name = os.path.basename(original_file)
    file_dir = os.path.dirname(original_file) or '.'
    file_base_name = os.path.splitext(file_name)[0]

    # 首先读取原始文件获取元素个数
    try:
        with open(original_file, 'r', encoding='utf-8') as f:
            original_data = json.load(f)

        if isinstance(original_data, list):
            original_count = len(original_data)
            data_type = "列表"
        elif isinstance(original_data, dict):
            original_count = len(original_data)
            data_type = "字典"
        else:
            original_count = 1
            data_type = "单个元素"

        print(f"原始文件类型: {data_type}")
        print(f"原始文件元素个数: {original_count}")

    except Exception as e:
        print(f"无法读取原始文件: {e}")
        return

    # 查找所有拆分文件
    split_files = []
    for file in os.listdir(file_dir):
        if file.startswith(f"{file_base_name}_part") and file.endswith('.json'):
            split_files.append(os.path.join(file_dir, file))

    split_files.sort()  # 按文件名排序
    print(f"找到 {len(split_files)} 个拆分文件")

    total_size = 0
    total_elements = 0
    file_details = []

    # 验证每个文件
    for file in split_files:
        try:
            with open(file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            file_size = os.path.getsize(file)
            total_size += file_size

            # 获取文件序号
            file_num = file.split('_part')[-1].split('.json')[0]

            # 计算元素个数
            if isinstance(data, list):
                element_count = len(data)
            elif isinstance(data, dict):
                element_count = len(data)
            else:
                element_count = 1

            total_elements += element_count

            file_details.append({
                'filename': os.path.basename(file),
                'size': file_size,
                'elements': element_count,
                'data': data
            })

            print(
                f"✓ {os.path.basename(file)}: 格式正确, 大小: {file_size / (1024 * 1024):.2f} MB, 包含 {element_count} 个元素")

        except Exception as e:
            print(f"✗ {os.path.basename(file)}: 验证失败 - {e}")

    # 验证元素个数是否匹配
    print(f"\n{'=' * 50}")
    print("元素个数验证结果:")
    print(f"{'=' * 50}")
    print(f"原始文件元素个数: {original_count}")
    print(f"拆分文件元素总数: {total_elements}")

    if original_count == total_elements:
        print(f"✅ 元素个数匹配成功! 原始文件与拆分文件元素个数一致")
    else:
        print(f"❌ 元素个数不匹配! 差异: {abs(original_count - total_elements)} 个元素")

        # 如果是字典类型，可以进一步检查具体的键差异
        if isinstance(original_data, dict) and all(isinstance(detail['data'], dict) for detail in file_details):
            print("\n详细键值对比:")
            original_keys = set(original_data.keys())
            split_keys = set()
            for detail in file_details:
                split_keys.update(detail['data'].keys())

            missing_keys = original_keys - split_keys
            extra_keys = split_keys - original_keys

            if missing_keys:
                print(
                    f"缺失的键 ({len(missing_keys)} 个): {list(missing_keys)[:10]}{'...' if len(missing_keys) > 10 else ''}")
            if extra_keys:
                print(
                    f"多余的键 ({len(extra_keys)} 个): {list(extra_keys)[:10]}{'...' if len(extra_keys) > 10 else ''}")

    # 文件大小统计
    original_size = os.path.getsize(original_file)
    print(f"\n{'=' * 50}")
    print("文件大小统计:")
    print(f"{'=' * 50}")
    print(f"原始文件大小: {original_size / (1024 * 1024):.2f} MB")
    print(f"拆分文件总大小: {total_size / (1024 * 1024):.2f} MB")

    # 显示每个拆分文件的详细信息
    print(f"\n{'=' * 50}")
    print("拆分文件详细信息:")
    print(f"{'=' * 50}")
    for detail in file_details:
        print(f"{detail['filename']}: {detail['elements']} 个元素, {detail['size'] / (1024 * 1024):.2f} MB")


# 使用示例
if __name__ == "__main__":
    # 配置参数
    input_json_file = "completeAuthorInfo.json"  # 替换为您的JSON文件路径
    max_size_mb = 70  # 每个拆分文件的最大大小(MB)

    # 执行拆分
    split_json_file(input_json_file, max_size_mb)

    # 可选：验证拆分文件
    verify_split_files(input_json_file)