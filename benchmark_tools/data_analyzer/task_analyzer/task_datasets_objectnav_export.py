## PYTHON 
## ******************************************************************** ##
## author: CTO_TI_FBSYJG
## create time: 2025/09/23 12:03:01 GMT+08:00
## ******************************************************************** ##
import sys
import os
import uuid
import argparse
import json
import gzip

os.chdir('/mnt/sfs-turbo-workflow/data-platform/')

# UUID 生成逻辑
def generate_task_uuid(scene_type, scene_name, split, start_position, start_rotation, object_category):
    namespace = uuid.NAMESPACE_DNS
    key = f"{scene_type}:{scene_name}:{split}:{start_position}:{start_rotation}:{object_category}"
    return str(uuid.uuid5(namespace, key))

# UUID 生成逻辑
def generate_traj_uuid(scene_type, scene_name, split, start_position, start_rotation, object_category, gen_traj_method, experiment_name):
    namespace = uuid.NAMESPACE_DNS
    key = f"{scene_type}:{scene_name}:{split}:{start_position}:{start_rotation}:{object_category}:{gen_traj_method}:{experiment_name}"
    return str(uuid.uuid5(namespace, key))

# 主程序逻辑
def main(args):
    # 创建输出目录
    os.makedirs(args.output_path, exist_ok=True)
    filter_ids = args.filter_ids.split(",")
    print(f"🔍 过滤的 UUID 列表: {filter_ids}")

    for file_name in os.listdir(args.input_path):
        if file_name.startswith(args.scene_name) and file_name.endswith(".json.gz"):
            input_file_path = os.path.join(args.input_path, file_name)
            output_file_path = os.path.join(args.output_path, file_name)
            print(f"📂 处理文件: {input_file_path}")
            with gzip.open(input_file_path, 'rt', encoding='utf-8') as f:
                data = json.load(f)
                # 测试10个episodes
                data["episodes"] = data["episodes"][:10]
                filtered_episodes = []
                for episode in data["episodes"]:
                    new_uuid = None
                    if args.dataset_type == "traj_datasets":
                        new_uuid = generate_traj_uuid(
                            args.scene_type,
                            args.scene_name,
                            args.split,
                            episode["start_position"],
                            episode["start_rotation"],
                            episode["object_category"],
                            args.gen_traj_method,
                            args.experiment_name
                        )
                    elif args.dataset_type == "task_datasets":
                        new_uuid = generate_task_uuid(
                            args.scene_type,
                            args.scene_name,
                            args.split,
                            episode["start_position"],
                            episode["start_rotation"],
                            episode["object_category"]
                        )
                    if new_uuid in filter_ids:
                        print(f"✅ 保留 episode_id: {episode['episode_id']}, uuid: {new_uuid}")
                        filtered_episodes.append(episode)
                    else:
                        print(f"❌ 忽略 episode_id: {episode['episode_id']}, uuid: {new_uuid}")
                data["episodes"] = filtered_episodes
                with gzip.open(output_file_path, "wt", encoding="utf-8") as out_f:
                    json.dump(data, out_f)

                json_output_file_path = output_file_path.replace(".json.gz", ".json")
                with open(json_output_file_path, "w", encoding="utf-8") as json_out_f:
                    json.dump(data, json_out_f, indent=4)
            print(f"✅ 文件已保存到: {output_file_path}")

def precheck_args(args):
    invalid_args = []
    if args.dataset_type not in ["task_datasets", "traj_datasets"]:
        print(f"❌ 参数 dataset_type {args.dataset_type} 必须是 'task_datasets' 或 'traj_datasets'")
        invalid_args.append("dataset_type")
    if not os.path.isdir(args.input_path):
        print(f"❌ 参数 input_path {args.input_path} 指定的路径不存在或不是目录: {args.input_path}")
        invalid_args.append("input_path")

    from pathlib import Path

    # 如果dataset_type是task_datasets, input_path里面必须包含task_datasets或traj_datasets
    if args.dataset_type == "task_datasets":
        if not any(part in ["task_datasets", "traj_datasets"] for part in Path(args.input_path).parts):
            print(f"❌ 参数 input_path {args.input_path} 路径中必须包含 'task_datasets' 或 'traj_datasets'")
            invalid_args.append("input_path")
    # 如果dataset_type是traj_datasets, input_path里面必须包含traj
    elif args.dataset_type == "traj_datasets":
        # 检查 input_path, 路径里必须包含traj的子字符串
        if not any("traj" in part for part in Path(args.input_path).parts):
            print(f"❌ 参数 input_path {args.input_path} 路径中必须包含 'traj' 子字符串")
            invalid_args.append("input_path")

    # 修改成检查args.input_path的子目录的子字符串包含args.scene_type 
    is_scene_type_valid = False
    for part in Path(args.input_path).parts:
        if args.scene_type in part:
            is_scene_type_valid = True
            break
    
    if not is_scene_type_valid:
        print(f"❌ 参数 input_path {args.input_path} 路径中必须包含 scene_type 的子字符串: {args.scene_type}")
        invalid_args.append("input_path")


    # 检查 input_path, 路径里必须包含args.split的子目录   
    if args.split not in Path(args.input_path).parts:
        print(f"❌ 参数 input_path {args.input_path} 路径中必须包含 split 的子目录: {args.split}")
        invalid_args.append("input_path")

    # 检查 gen_traj_method, 仅当 dataset_type 是 traj_datasets 时检查
    # input_path 里必须包含 gen_traj_method 的子字符串
    if args.dataset_type == "traj_datasets":
        # 检查 input_path, 路径里必须包含gen_traj_method的子字符串，并且以gen_traj_method结尾
        if not any(part.endswith(args.gen_traj_method) for part in Path(args.input_path).parts):
            print(f"❌ 参数 input_path {args.input_path} 路径中必须以 gen_traj_method 结尾: {args.gen_traj_method}")
            invalid_args.append("input_path")
        # 如果input_path里面包含traj_datasets，那么experiment_name必须为"null"
        if "traj_datasets" in Path(args.input_path).parts and args.experiment_name != "null":
            print(f"❌ 参数 experiment_name 必须为null，因为 input_path {args.input_path} 包含 'traj_datasets'")
            invalid_args.append("experiment_name")
        # 如果input_path里面不包含traj_datasets，那么experiment_name不能为"null"
        if "traj_datasets" not in Path(args.input_path).parts and args.experiment_name == "null":
            print(f"❌ 参数 experiment_name 不能为null，因为 input_path {args.input_path} 不包含 'traj_datasets'")
            invalid_args.append("experiment_name")
        
    # 检查 recipe_tag_input是否为空，如果为空，则报错
    if not args.recipe_tag_input:
        print(f"❌ 参数 recipe_tag_input 不能为空，必须提供")
        invalid_args.append("recipe_tag_input")
    # 检查 recipe_tag_output是否为空，如果为空，则报错
    if not args.recipe_tag_output:
        print(f"❌ 参数 recipe_tag_output 不能为空，必须提供")
        invalid_args.append("recipe_tag_output")
    # recipe_tag_output不能和 recipe_tag_input 相同，并且必须以recipt_tag_input开头
    if args.recipe_tag_output == args.recipe_tag_input or not args.recipe_tag_output.startswith(args.recipe_tag_input):
        print(f"❌ 参数 recipe_tag_output {args.recipe_tag_output} 必须以 recipe_tag_input {args.recipe_tag_input} 开头，且不能相同")
        invalid_args.append("recipe_tag_output")

    # input_path 里必须包含 recipe_tag_input 的子字符串
    if args.recipe_tag_input not in Path(args.input_path).parts:
        print(f"❌ 参数 input_path {args.input_path} 路径中必须包含 recipe_tag_input: {args.recipe_tag_input}")
        invalid_args.append("input_path")

    # output_path 不能和 input_path 相同
    if args.input_path == args.output_path:
        print(f"❌ 参数 output_path 不能和 input_path 相同: {args.output_path}")
        invalid_args.append("output_path")
    return invalid_args

# argparse 参数解析
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="基于过滤出来的id列表，导出对应的ObjectNav数据集")
    parser.add_argument("--dataset_type", default="traj_datasets", required=True, help="task_datasets or traj_datasets")
    parser.add_argument("--input_path", default="data/task_datasets/objectnav/hm3d_v1/train/content", help="输入数据集路径")
    parser.add_argument("--recipe_tag_input", default="hm3d_v1", help="输入数据集的recipe_tag")
    parser.add_argument("--recipe_tag_output", default="hm3d_v1_chair", help="输出数据集的recipe_tag")
    parser.add_argument("--scene_name", default="1UnKg1rAb8A", help="场景名称")
    parser.add_argument("--filter_ids", default="e5521116-bb82-519d-bd4b-3613aa796041,e57bcc21-ed99-5cc5-a67f-c87b7fbada62", help="过滤的id列表，逗号分隔")
    parser.add_argument("--scene_type", default="hm3d_v1", help="场景类型")
    parser.add_argument("--split", default="train", help="数据集划分类型")

    # 轨迹相关参数
    parser.add_argument("--gen_traj_method", default="hd", help="轨迹生成方法: sp(shortest_path) or hd(human_demonstration) or l3mvn")
    parser.add_argument("--experiment_name", default="null", help="实验名称")

    args = parser.parse_args()
    args.output_path = args.input_path.replace(args.recipe_tag_input, args.recipe_tag_output)

    invalid_args = precheck_args(args)
    if len(invalid_args) == 0:
        for arg, value in vars(args).items():
            print(f"🔧 参数 {arg}: {value}")
        main(args)
    elif len(invalid_args) > 0:
        for arg in invalid_args:
            print(f"❌ 参数 {arg} 无效，请检查后重新运行脚本。")
        sys.exit(1)