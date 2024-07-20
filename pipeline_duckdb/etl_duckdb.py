import duckdb
import time

def query_duckdb():
    return duckdb.sql("""
    SELECT station,
        MIN(temperature) min_temp,
        AVG(temperature) avg_temp,
        MAX(temperature) max_temp
    FROM read_csv("../data/measurements.txt", AUTO_DETECT=FALSE, sep=';', columns={'station': VARCHAR, 'temperature': 'DECIMAL(3,1)'})
    GROUP BY station
    ORDER BY station
    """)

if __name__ == '__main__':
    start_time = time.time()
    result = query_duckdb()
    end_time = time.time()
    duration = end_time - start_time
    print(query_duckdb())
    print(duration)