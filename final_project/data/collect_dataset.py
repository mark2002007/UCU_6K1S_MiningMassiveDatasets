import json
import argparse
import pandas as pd

from typing import Optional
from tqdm import tqdm
from sseclient import SSEClient as EventSource


def wiki_recentchange_stream(server_name: Optional[str] = None, action_type: Optional[str] = None):
    url = 'https://stream.wikimedia.org/v2/stream/recentchange'
    for event in EventSource(url):
        if event.event == 'message':
            try:
                change = json.loads(event.data)
            except ValueError:
                continue
            if change['meta']['domain'] == 'canary': # We are discarding monitoring events
                continue
            if server_name is not None and change['server_name'] != server_name:
                continue
            if action_type is not None and change['type'] != action_type:
                continue
            yield change


def collect_dataset(dataset_size: int, server_name: Optional[str] = None, action_type: Optional[str] = None, sampling_freq: float = 0.2):
    changes_list = []
    lister = tqdm(total=dataset_size)
    for change in wiki_recentchange_stream(server_name, action_type):
        if hash(str(change)) % 1000 / 1000 < sampling_freq:
            changes_list.append(change)
            lister.update(1)
            if len(changes_list) >= dataset_size:
                break
    
    changes = pd.DataFrame(changes_list)
    return changes


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
                    prog='WikipediaDatasetCollector',
                    description='Collect dataset from wikipedia stream of changes')
    parser.add_argument('output_filepath')
    parser.add_argument('dataset_size', type=int)
    parser.add_argument('-s', '--server_name', default='en.wikipedia.org')
    parser.add_argument('-a', '--action_type', default='edit')
    parser.add_argument('--sampling_freq', type=float, default=0.2)

    args = parser.parse_args()

    if args.sampling_freq < 0 or args.sampling_freq > 1:
        raise ValueError(f"Illegal sampling frequency passed: {args.sampling_freq}")
    if args.dataset_size <= 0:
        raise ValueError(f"Illegal dataset size passed: {args.dataset_size}")

    dataset = collect_dataset(args.dataset_size, args.server_name, args.action_type, args.sampling_freq)
    dataset.to_csv(args.output_filepath, index=False)
