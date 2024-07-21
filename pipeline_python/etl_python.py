from pathlib import Path
from collections import defaultdict
import time

def read_data_from_file(path_txt: Path) -> defaultdict[list]:
    """
    Reads data from the txt file and returns a defaultdict with lists of temperatures by season.

    The text file must have lines with the format 'season;temperature', where 'season' is a string 
    representing the name of the station and 'temperature' is a floating point value representing 
    the recorded temperature.

    Parameters
    ----------
    path_txt : Path
        Path of the txt file to be read.

    Returns
    -------
    defaultdict[list]
        Defaultdict where keys are station(str) and values are lists of temperatures(float).
    """

    temperature_by_station = defaultdict(list)
    with open(path_txt, mode="r", encoding="utf-8") as file:
        for row in file.readlines():
            splited_row = row.split(';')
            station, temperature = str(splited_row[0]), float(splited_row[1])
            temperature_by_station[station].append(temperature)

    return temperature_by_station


def calculate_metrics(datas:dict[list]) -> defaultdict[tuple]:
    """    
    Calculates the average, lowest and highest temperature by station and return results order by statios

    Parameters
    ----------
    datas : dict[list]
        Dict with lists of temperatures by station

    Returns
    -------
    defaultdict[tuple]
        Defaultdict where the keys are station(str) and the values are tuples with lowest, average and highest temperatures.
    """

    results = defaultdict(tuple)
    for station, temperatures in datas.items():
        min_temp = min(temperatures)
        mean_temp = sum(temperatures) / len(temperatures)
        max_temp = max(temperatures)
        results[station] = (min_temp, mean_temp, max_temp)
    results = dict(sorted(results.items()))
    return results


if __name__ == "__main__":
    path_txt = Path('../data/measurements.txt')

    start_time = time.time()
    datas = read_data_from_file(path_txt)
    results = calculate_metrics(datas)
    end_time = time.time()
    
    duration = end_time - start_time
    print(duration)
    
