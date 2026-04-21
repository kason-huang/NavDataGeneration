import gzip
import json
file_path = '/root/NavDataGeneration/model/objnav/ovon/experiments/eval/nav-ovon-eval-bc-w6bvq_20250929/traj/content/ckpt_42/4ok3usBNeis.json.gz'
with gzip.open(file_path, 'rt', encoding='utf-8') as f:
    json_data = json.load(f)