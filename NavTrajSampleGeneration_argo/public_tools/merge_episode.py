import os
import gzip
import json
import argparse
from collections import defaultdict

def merge_json_gz_files(input_dir, output_dir, origin_path, target_scene=None):
    """
    遍历目录A下的所有子目录，合并同名.json.gz文件中的JSON内容。
    字段a、b、c取自第一个文件，字段d的内容会追加到列表中。
    
    参数:
        input_dir: 输入目录路径（目录A）
        output_dir: 输出目录路径，用于保存合并后的文件
    """
    # 创建输出目录
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # 用于按文件名分组存储文件路径
    file_groups = defaultdict(list)
    
    # 遍历输入目录及其所有子目录
    for root, dirs, files in os.walk(input_dir):
        for file in files:
            if file.endswith('.json.gz'):
                if target_scene and target_scene not in file:
                    continue
                full_path = os.path.join(root, file)
                file_groups[file].append(full_path)

    print("file_groups", file_groups)
    
    # 处理每个同名文件组
    for filename, file_paths in file_groups.items():
        if target_scene and target_scene not in filename:
            continue

        merged_data = None
        
        for file_path in file_paths:
            try:
                # 读取.gz文件并解析JSON内容
                with gzip.open(file_path, "rb") as file:
                    data = json.loads(file.read(), encoding="utf-8")
                
                    # 如果是第一个文件，初始化合并数据
                    if merged_data is None:
                        origin_file = os.path.join(origin_path, filename)
                        with gzip.open(origin_file, "rb") as file:
                            origin_data = json.loads(file.read(), encoding="utf-8")
                        merged_data = origin_data
                        merged_data['episodes'] = []
                    
                    merged_data['episodes'].extend(data['episodes'])
                
            except Exception as e:
                print(f"错误处理文件 {file_path}: {e}")
                continue
        
        # 如果有有效数据，保存合并结果
        if merged_data is not None and len(merged_data['episodes']) > 0:
            output_path = os.path.join(output_dir, filename)
            
            try:
                with gzip.open(output_path, 'wt', encoding='utf-8') as f:
                    json.dump(merged_data, f, indent=4)
                print(f"已合并 {len(file_paths)} 个文件到: {output_path}")
            except Exception as e:
                print(f"保存文件 {output_path} 时出错: {e}")
        else:
            print(f"警告: 没有有效数据可用于文件 {filename}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='合并同名.json.gz文件的JSON内容')

    parser.add_argument(
        "--input_path",
        type=str,
        default="/mnt/sfs-turbo-workflow/data-platform/data/traj_datasets/objectnav/hm3d_v2_l3mvn_refine_v2/train/content"
    )
    parser.add_argument(
        '--output_path',
        type=str,
        default="/mnt/sfs-turbo-workflow/data-platform/data/traj_datasets/objectnav/hm3d_v2_l3mvn_refine_v2_merge/train/content",
    )

    parser.add_argument(
        '--origin_path',
        type=str,
        default="/mnt/sfs-turbo-workflow/data-platform/data/task_datasets/objectnav/hm3d_v2/train/content",
    )

    parser.add_argument(
        '--scene_name',
        type=str,
        default="YHmAkqgwe2p",
    )

    args = parser.parse_args()
    
    if not os.path.exists(args.input_path):
        print(f"错误: 输入目录 {args.input_path} 不存在")
        exit(1)
    
    merge_json_gz_files(args.input_path, args.output_path, args.origin_path, args.scene_name)
    print("处理完成!")