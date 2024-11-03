import pandas as pd

SERVERS_PATH = "input/server.csv"
TASKS_PATH = "input/tasks.csv"
OUTPUT_PATH = "output/output.csv"

def read_servers(path = SERVERS_PATH):
    """
    Reads the server data from a CSV file and sets the column names.

    Parameters:
    path (str): The path to the CSV file containing server data. Defaults to SERVERS_PATH.

    Returns:
    pd.DataFrame: A DataFrame containing the server data with columns ["id", "cpu", "watts", "ram"].
    """
    server_df = pd.read_csv(path)
    server_df.columns = ["id", "cpu", "watts", "ram"]
    return server_df

def read_tasks(path = TASKS_PATH):
    """
    Reads the tasks data from a CSV file and sets the column names.

    Parameters:
    path (str): The path to the CSV file containing tasks data. Defaults to TASKS_PATH.

    Returns:
    pd.DataFrame: A DataFrame containing the tasks data with columns ["id", "cores", "turns", "ram", "completed_by"].
    """
    tasks_df = pd.read_csv(path)
    tasks_df.columns = ["id", "cores", "turns", "ram", "completed_by"]
    return tasks_df

# This is to output.csv
# Current Turn, Task Number, Status, Total Power, Server Number
# 2, 2, 0, 0
# 5, 1, 1, 192, 1
def write_task(df, curr_turn, task_id, status, power, server_id):
    df["Current Turn"].append(curr_turn)
    df["Task Number"].append(task_id)
    df["Status"].append(status)
    df["Total Power"].append(power)
    df["Server Number"].append(server_id)

# Need to implement writing the following into a data frame
# simulation output
# Update type, timestamp in seconds, turn number, server/task number, action/cpu cores, RAM
# Task, 0.01, 1, 1, Read, N/A
# Server, 0.02, 1, 1, 4, 20
# Task, 0.04, 2, 2, Read, N/A

# Record the actual timestamp that will be passed in

def write_event(df, event_type, timestamp, turn, id_num, action="Read", cpu=None, ram=None):
    df["Update Type"].append(event_type)
    df["TimeStamp"].append(timestamp)
    df["Turn"].append(turn)
    df["ID"].append(id_num)
    df["Action"].append(action)
    df["CPU"].append(cpu)
    df["RAM"].append(ram)


def write_df_as_file_output(pd_df: pd.DataFrame, path = OUTPUT_PATH):
    pd_df.to_csv(path, index=False)