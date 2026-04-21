import argparse
import attr
import habitat
import numpy as np
import gzip
import json
import os

from habitat import logger
from habitat.tasks.nav.shortest_path_follower import ShortestPathFollower
from habitat.sims.habitat_simulator.actions import HabitatSimActions

from habitat_sim.utils import viz_utils as vut
from habitat_sim.utils.common import quat_to_coeffs, quat_from_magnum


def write_json(data, path):
    with open(path, 'w') as file:
        file.write(json.dumps(data, indent=4))


def write_gzip(input_path, output_path):
    with open(input_path, "rb") as input_file:
        with gzip.open(output_path + ".gz", "wb") as output_file:
            output_file.writelines(input_file)

def load_dataset(path):
    with gzip.open(path, "rb") as file:
        data = json.loads(file.read(), encoding="utf-8")
    return data


def get_action(action):
    if action == HabitatSimActions.TURN_RIGHT:
        return "TURN_RIGHT"
    elif action == HabitatSimActions.TURN_LEFT:
        return "TURN_LEFT"
    elif action == HabitatSimActions.MOVE_FORWARD:
        return "MOVE_FORWARD"
    elif action == HabitatSimActions.LOOK_UP:
        return "LOOK_UP"
    elif action == HabitatSimActions.LOOK_DOWN:
        return "LOOK_DOWN"
    return "STOP"


def get_agent_pose(sim):
    agent_translation = sim._default_agent.body.object.translation
    agent_rotation = sim._default_agent.body.object.rotation
    for sensor_key, v in sim._default_agent._sensors.items():
        rotation = quat_from_magnum(v.object.rotation)
        rotation = quat_to_coeffs(rotation).tolist()
        translation = v.object.translation
    
    return {
        "position": np.array(agent_translation).tolist(),
        "rotation": quat_to_coeffs(quat_from_magnum(agent_rotation)).tolist()
    }


def get_action_data(action, sim):
    data = {}
    data["action"] = get_action(action)
    data["agent_state"] = get_agent_pose(sim)
    return data


def get_episode_json(episode, reference_replay):
    episode.reference_replay = reference_replay
    episode.scene_id = episode.scene_id
    ep_json = attr.asdict(episode)
    del ep_json["_shortest_path_cache"]
    return ep_json


def get_closest_goal(episode, sim):
    min_dist = 1000.0
    goal_location = None
    agent_position = sim.get_agent_state().position
    for goal in episode.goals:
        for view_point in goal.view_points:
            position = view_point.agent_state.position
            
            dist = sim.geodesic_distance(
                agent_position, position
            )
            if min_dist > dist:
                min_dist = dist
                goal_location = position
    return goal_location

def get_closest_goal_by_l3_end(episode, sim):
    min_dist = 1000.0
    goal_location = None
    agent_position = sim.get_agent_state().position
    l3_end_pos = episode.reference_replay[-1]["agent_state"]["position"]
    for goal in episode.goals:
        for view_point in goal.view_points:
            position = view_point.agent_state.position

            dist = sim.geodesic_distance(
                l3_end_pos, position
            )
            if min_dist > dist:
                min_dist = dist
                goal_location = position
    return goal_location

def generate_trajectories(cfg, episode_path, output_path=""):
    with habitat.Env(cfg) as env:
        goal_radius = 0.1
        spl = 0
        total_success = 0.0
        total_episodes = 0.0

        dataset = load_dataset(episode_path)

        dataset["episodes"] = []

        logger.info("Total episodes: {}".format(len(env.episodes)))
        for _ in range(len(env.episodes)):
            follower = ShortestPathFollower(
                env._sim, goal_radius, False
            )
            env.reset()
            success = 0
            reference_replay = []
            episode = env.current_episode
            goal_position = get_closest_goal_by_l3_end(episode, env.sim)
            
            info = {}
            reference_replay.append(get_action_data(HabitatSimActions.STOP, env._sim))
            if goal_position is None:
                continue
            while not env.episode_over:
                best_action = follower.get_next_action(
                    goal_position
                )

                if "distance_to_goal" in info.keys() and info["distance_to_goal"] < 0.1 and best_action != HabitatSimActions.STOP:
                    best_action = HabitatSimActions.STOP

                env.step(best_action)

                info = env.get_metrics()
                success = info["success"]

                action_data = get_action_data(best_action, env._sim)
                reference_replay.append(action_data)

            ep_data = get_episode_json(env.current_episode, reference_replay)
            total_success += success
            spl += info["spl"]
            total_episodes += 1

            dataset["episodes"].append(ep_data)
        
        print("Total episodes: {}".format(total_episodes))

        print("\n\nEpisode success: {}".format(total_success / total_episodes))
        print("SPL: {}, {}, {}".format(spl/total_episodes, spl, total_episodes))
        print("Success: {}, {}, {}".format(total_success/total_episodes, total_success, total_episodes))
        write_json(dataset, output_path)
        write_gzip(output_path, output_path)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--traj_path", type=str, default="/home/fsq/ovon/data/ShortestPath/HD_30K_L3/"
    )
    parser.add_argument(
        "--scene_name", type=str, default='1S7LAXRdDqK'
    )
    parser.add_argument(
        "--output_path", type=str, default="/home/fsq/ovon/data/ShortestPath/HD_30K_SP/"
    )
    args = parser.parse_args()
    input_path = os.path.join(args.traj_path, args.scene_name + ".json.gz")
    os.makedirs(args.output_path, exist_ok=True)
    output_path = os.path.join(args.output_path, args.scene_name + ".json")

    config = habitat.get_config("/app/data/z00562901/NavDataGeneration/NavTrajSampleGeneration/ShortestPath/objectnav_hm3d.yaml")
    cfg = config
    cfg.defrost()
    cfg.DATASET.DATA_PATH = input_path
    cfg.freeze()

    generate_trajectories(cfg, input_path, output_path)

if __name__ == "__main__":
    main()
