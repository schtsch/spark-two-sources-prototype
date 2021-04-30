import uuid

guids_cnt = 3
repeats_cnt = 5
events_file_path = 'data/events.csv'
ref_file_path = 'data/refs.csv'

guids = [str(uuid.uuid4()) for idx in range(0, guids_cnt)]

with open(ref_file_path, mode='wt') as ref_file:
    for idx, guid in enumerate(guids):
        ref_file.write(f'{guid},{idx}\n')

with open(events_file_path, mode='wt') as events_file:
    global_idx = 0
    for repeat_idx in range(0, repeats_cnt):
        for guid in guids:
            events_file.write(f'{guid},{global_idx},{repeat_idx}\n')
            global_idx += 1
