## PYTHON 
## ******************************************************************** ##
## author: CTO_TI_FBSYJG
## create time: 2025/09/23 12:03:01 GMT+08:00
## ******************************************************************** ##
import os
import re
import sys
import gzip
import json
import uuid
import argparse

from numpy import integer, short
from sqlalchemy import create_engine, Column, String, Float, Integer, ForeignKey, ARRAY
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from urllib.parse import quote_plus

#os.chdir('/mnt/sfs-turbo-workflow/data-platform/')

# ORM 基类
Base = declarative_base()

# 表模型定义
class TaskDatasetObjectNav(Base):
    __tablename__ = "task_datasets_objectnav"
    __table_args__ = {'schema': 'public'}

    id = Column(String(36), primary_key=True)
    scene_type = Column(String(50), nullable=False)
    split = Column(String(10), nullable=False)
    scene_name = Column(String(255), nullable=False)
    object_category = Column(String(255), nullable=False)
    geodesic_distance = Column(Float, nullable=False)
    euclidean_distance = Column(Float, nullable=False)
    nav_complexity_ratio = Column(Float, nullable=False)
    recipe_tags = Column(ARRAY(String), default=[])

class TrajDatasetObjectNav(Base):
    __tablename__ = "traj_datasets_objectnav"
    __table_args__ = {'schema': 'public'}

    id = Column(String(36), primary_key=True)                     # 唯一轨迹ID
    gen_traj_method = Column(String(50), nullable=False)          # 轨迹生成方法
    task_id = Column(String(36), nullable=False)  # 关联任务ID
    success = Column(Integer, nullable=False)                     # 是否成功完成任务
    spl = Column(Float)                                           # SPL指标
    traj_len = Column(Integer)                                      # 轨迹长度
    experiment_name = Column(String(255))                         # 实验名称
    # 添加一个recipe_tags字段，类型为ARRAY(String)，默认值为空列表
    recipe_tags = Column(ARRAY(String), default=[])

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

# 查找 JSON.GZ 文件
def find_json_gz_file(dataset_path, scene_name):
    for filename in os.listdir(dataset_path):
        if filename.startswith(scene_name) and filename.endswith(".json.gz"):
            return os.path.join(dataset_path, filename)
    return None

def gen_record(args, ep):
    record = None

    start_position = ep["start_position"]
    start_rotation = ep["start_rotation"]
    object_category = ep["object_category"]
    info = ep["info"]

    if args.dataset_type == "task_datasets":
        task_id = generate_task_uuid(
            args.scene_type, args.scene_name, args.split,
            start_position, start_rotation, object_category
        )

        record = TaskDatasetObjectNav(
            id=task_id,
            scene_type=args.scene_type,
            split=args.split,
            scene_name=args.scene_name,
            object_category=object_category,
            geodesic_distance=info["geodesic_distance"],
            euclidean_distance=info["euclidean_distance"],
            nav_complexity_ratio=info["geodesic_distance"] / info["euclidean_distance"]
            if info["euclidean_distance"] != 0 else 0,
            recipe_tags=[args.recipe_tag]
        )
    elif args.dataset_type == "traj_datasets":
        task_id = generate_task_uuid(
            args.scene_type, args.scene_name, args.split,
            start_position, start_rotation, object_category
        )

        traj_id = generate_traj_uuid(
            args.scene_type, args.scene_name, args.split,
            start_position, start_rotation, object_category,
            args.gen_traj_method, args.experiment_name
        )

        metrics = ep.get("metrics", {})
        if not metrics:
            print("⚠️ 警告: 该 episode 缺少 metrics 信息，跳过该记录")
            return None

        record = TrajDatasetObjectNav(
            id=traj_id,
            gen_traj_method=args.gen_traj_method,
            task_id=task_id,
            success=metrics.get("success", 0),
            spl=metrics.get("spl", 0.0),
            traj_len=metrics.get("traj_len", 0),
            experiment_name=args.experiment_name,
            recipe_tags=[args.recipe_tag]
        )

    return record

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

    # 如果experiment_name是null，表示不是基于实验生成的轨迹，则进行scene_type检查
    if args.experiment_name == "null":
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

        if args.recipe_tag not in Path(args.input_path).parts:
            print(f"❌ 参数 input_path {args.input_path} 路径中必须包含 recipe_tag: {args.recipe_tag}")
            invalid_args.append("input_path")

    # 检查 gen_traj_method, 仅当 dataset_type 是 traj_datasets 时检查
    # input_path 里必须包含 gen_traj_method 的子字符串
    if args.dataset_type == "traj_datasets":
        # 检查input_path中是否包含content_metrics, 如果不包含，就提示用户修改为content_metrics
        if "content_metrics" not in Path(args.input_path).parts:
            print(f"❌ 参数 input_path {args.input_path} 路径中必须包含 'content_metrics' 子目录")
            invalid_args.append("input_path")

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
        
    # input_path 里必须包含 recipe_tag
    # 检查 recipe_tag是否为空，如果为空，则报错
    if not args.recipe_tag:
        print(f"❌ 参数 recipe_tag 不能为空，当 dataset_type 是 task_datasets 时必须提供")
        invalid_args.append("recipe_tag")



    return invalid_args

# 主程序逻辑
def main(args):
    json_path = find_json_gz_file(args.input_path, args.scene_name)
    if not json_path:
        print(f"❌ 未找到以 {args.scene_name} 开头的 .json.gz 文件")
        return

    encoded_password = quote_plus(args.db_password)
    db_url = f"postgresql+psycopg2://{args.db_user}:{encoded_password}@{args.db_host}:{args.db_port}/{args.db_name}"
    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)
    session = Session()
    print(f"🔗 连接数据库：{db_url}")

    try:
        with gzip.open(json_path, 'rt', encoding='utf-8') as f:
            data = json.load(f)
            episodes = data.get("episodes", [])
            print(f"📦 共找到 {len(episodes)} 个 episodes")

            count = 0
            for ep in episodes:
                record = gen_record(args, ep)
                # 如果task_id 或 traj_id已经存在，就合并task_dataset_tasgs或recipe_tags字段，然后更新记录
                # tags要求不重复

                # 是否是重复记录
                duplicate = False

                pre_record = None
                if args.dataset_type == "task_datasets" and record:
                    pre_record = session.query(TaskDatasetObjectNav).filter_by(id=record.id).first()
                elif args.dataset_type == "traj_datasets" and record:
                    pre_record = session.query(TrajDatasetObjectNav).filter_by(id=record.id).first()
                if pre_record:
                    # 如果record的tags在pre_record的tags里已经存在，就跳过该episode
                    if record.recipe_tags[0] in pre_record.recipe_tags:
                        print(f"⚠️ 记录已存在且 tags 相同，跳过该 episode")
                        duplicate = True
                    else:
                        print(f"🔄 记录已存在，合并 tags: {pre_record.recipe_tags} + {record.recipe_tags}")
                        if pre_record.recipe_tags is None:
                            pre_record.recipe_tags = []
                        merged_tags = list(set(pre_record.recipe_tags + record.recipe_tags))
                        pre_record.recipe_tags = merged_tags
                        record = pre_record
                
                if record and not duplicate:
                    session.merge(record)
                    count += 1
                    if count % 100 == 0:
                        session.commit()
                        print(f"💾 已提交 {count} 条记录")
                elif record is None:
                    print("⚠️ 记录生成失败，跳过该 episode")

            # 提交剩余未满100条的记录
            if count % 100 != 0:
                session.commit()
                print(f"💾 最后提交剩余 {count % 100} 条记录")
            print("✅ 数据插入完成")
    except SQLAlchemyError as e:
        print(f"⚠️ 数据库操作失败：{e}")
        session.rollback()
    except Exception as e:
        print(f"⚠️ 程序异常：{e}")
    finally:
        session.close()

# argparse 参数解析
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="导入 ObjectNav 任务&轨迹数据集到 PostgreSQL 数据库")
    parser.add_argument("--dataset_type", default="traj_datasets", required=True, help="task_datasets or traj_datasets")
    parser.add_argument("--input_path", default="data/traj_datasets/objectnav/hm3d_v1_hd/train/content_metrics", required=True, help="数据集路径")
    parser.add_argument("--scene_name", default="DoSbsoo4EAg", required=True, help="场景名称")
    parser.add_argument("--scene_type", default="hm3d_v1", help="场景类型")
    parser.add_argument("--split", default="train", help="数据集划分类型")
    # 数据配方tag
    parser.add_argument("--recipe_tag", default='', help="数据配方标签")

    parser.add_argument("--gen_traj_method", default="hd", help="轨迹生成方法: sp(shortest_path) or hd(human_demonstration) or l3mvn")
    parser.add_argument("--experiment_name", default="null", help="实验名称")

    # database connection parameters
    parser.add_argument("--db_user", default='dbadmin', help="数据库用户名")
    parser.add_argument("--db_password", default='dataplatform@123', help="数据库密码")
    parser.add_argument("--db_host", default="dws-z00562901.dws.myhuaweiclouds.com", help="数据库主机")
    parser.add_argument("--db_port", default="8000", help="数据库端口")
    parser.add_argument("--db_name", default='postgres' , help="数据库名称")

    args = parser.parse_args()
    invalid_args = precheck_args(args)
    if len(invalid_args) == 0:
        for arg, value in vars(args).items():
            print(f"🔧 参数 {arg}: {value}")
        main(args)
    elif len(invalid_args) > 0:
        for arg in invalid_args:
            print(f"❌ 参数 {arg} 无效，请检查后重新运行脚本。")
        sys.exit(1)