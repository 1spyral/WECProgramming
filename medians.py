import pandas as pd

power_median = 0
turn_median = 0
core_median = 0

def set_medians(tasks: pd.DataFrame, servers: pd.DataFrame):
    power_median = servers['watts'].median()
    turn_median = tasks['turns'].median()
    core_median = servers['cpu'].median()

    print(f"Power median: {power_median}")
    print(f"Turns median: {turn_median}")
    print(f"Cores median: {core_median}")

    return (power_median, turn_median, core_median)