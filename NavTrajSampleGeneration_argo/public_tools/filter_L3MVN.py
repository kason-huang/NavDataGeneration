import numpy as np
import json
import os
import argparse
import gzip


def write_json(data, path):
    with open(path, 'w') as file:
        file.write(json.dumps(data, indent=4))


def write_gzip(dataset, output_path):
    with gzip.open(output_path, 'wt', encoding='utf-8') as f:
        json.dump(dataset, f, indent=4)

def load_dataset(path):
    try:
        with gzip.open(path, "rb") as file:
            data = json.loads(file.read(), encoding="utf-8")
        return data
    except Exception as e:
        print(f"Error reading file {path}: {e}")
        return


def filter_exceed_500_episodes(reference_replay):
    if len(reference_replay) >= 500 or reference_replay[-1]["action"] != "STOP":
        return False
    return True


def filter_look_up_down_data(reference_replay):
    new_reference_replay = []
    if reference_replay[0]["action"] != "STOP":
        new_record = {}
        new_record["action"] = "STOP"
        new_record["agent_state"] = ""
        reference_replay = [new_record] + reference_replay
    
    for record in reference_replay:
        if record["action"] != "LOOK_UP" and record["action"] != "LOOK_DOWN":
            new_reference_replay.append(record)
        # else:
        #     count_filter += 1
    
    return new_reference_replay


def filter_stair(reference_replay, pos_diff_max=0.3):
    min_pos = 1000.0
    max_pos = -1000.0
    
    for record in reference_replay:
        if record["action"] == "STOP":
            continue

        agent_height = record["agent_state"]["position"][1]
        if agent_height > max_pos:
            max_pos = agent_height
        if agent_height < min_pos:
            min_pos = agent_height
    
    if max_pos - min_pos >= pos_diff_max:
        return False
    
    return True

def process_filter(dataset, scene_name, pos_diff_max=0.3):

    if len(dataset["episodes"]) == 0:
        print(f"File {scene_name} has no episodes, skipping.")
        return

    print(f"Scene: {scene_name}, before filtering, dataset len: {len(dataset['episodes'])}")
    
    new_episodes = []
    for ep in dataset["episodes"]:
        reference_replay = ep.get("reference_replay", [])
        if not filter_exceed_500_episodes(reference_replay):  # 超过500条的筛掉
            continue

        if not filter_stair(reference_replay):         # 高度差超过0.3的筛掉
            continue
        
        ep["reference_replay"] = filter_look_up_down_data(reference_replay) # 去掉look_up_down
        new_episodes.append(ep)

    dataset["episodes"] = new_episodes
    
    print(f"Scene: {scene_name}, after filtering, dataset len: {len(dataset['episodes'])}")




if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--input_path", type=str, default="/home/fsq/ovon/data/check_zb_tarj/hm3d_v2_l3mvn_77k/over_300/train_set"
    )
    parser.add_argument(
        "--output_path", type=str, default="/home/fsq/ovon/data/check_zb_tarj/hm3d_v2_l3mvn_77k/"
    )
    parser.add_argument(
        "--scene_name", type=str, default="1234"
    )

    args = parser.parse_args()

    if not os.path.exists(args.output_path):
        os.makedirs(args.output_path, exist_ok=True)

    dataset_file = os.path.join(args.input_path, args.scene_name + '.json.gz')
    print("dataset_file:", dataset_file)
    dataset = load_dataset(dataset_file)

    process_filter(dataset, args.scene_name)

    output_path = os.path.join(args.output_path, args.scene_name + '.json.gz')
    print("output_path:", output_path)
    write_gzip(dataset, output_path)

    
