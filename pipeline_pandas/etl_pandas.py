import pandas as pd
from multiprocessing import Pool, cpu_count
from tqdm import tqdm
from pathlib import Path
import time

CONCURRENCY = cpu_count() # logic processor
chunksize = 1_000_000

def process_chunk(chunk):
    aggregated = chunk.groupby('station')['temperature'].agg(['min', 'max', 'mean']).reset_index()
    return aggregated


def create_df_with_pandas(path_file:Path, chunksize=chunksize):
    results = []
    with pd.read_csv(path_file, sep=';', header=None, names=['station', 'temperature'], chunksize=chunksize) as reader:
        with Pool(CONCURRENCY) as pool:
            for chunk in tqdm(reader, desc="Processando"):
                result = pool.apply_async(process_chunk, (chunk,))
                results.append(result)

            results = [result.get() for result in results]

    final_df = pd.concat(results, ignore_index=True)
    final_aggregated_df = final_df.groupby('station').agg({
        'min': 'min',
        'max': 'max',
        'mean': 'mean'
    }).reset_index().sort_values('station')

    return final_aggregated_df

if __name__ == '__main__':
    path_file = Path("../data/measurements.txt")
    start_time = time.time()
    df = create_df_with_pandas(path_file, chunksize)
    end_time = time.time()
    duration = end_time - start_time
    print(duration)
