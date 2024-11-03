from pandas import DataFrame

def find_impossible_tasks(tasks_df: DataFrame, server_df: DataFrame) -> DataFrame:
    """
    Find the impossible tasks in the tasks dataframe
    Impossible tasks are tasks that require more resources than the server has
    or tasks that are completed before they are read

    Note that this does not necessarily mean that the task will be run

    Parameters:
    tasks_df (pandas.DataFrame): The tasks dataframe
    server_df (pandas.DataFrame): The server dataframe

    Returns:
    pandas.DataFrame: The impossible tasks dataframe
    """
    impossible_tasks = tasks_df[
        (tasks_df["ram"] > server_df["ram"].max()) | 
        (tasks_df["cores"] > server_df["cpu"].max()) |
        (tasks_df["completed_by"] < tasks_df["turns"])
    ]
    return impossible_tasks

def remove_impossible_tasks(tasks_df: DataFrame, impossible_tasks: DataFrame) -> DataFrame:
    """
    Sanitize the tasks dataframe by removing the impossible tasks
    previously fetched by find_impossible_tasks

    Parameters:
    tasks_df (pandas.DataFrame): The tasks dataframe
    impossible_tasks (pandas.DataFrame): The impossible tasks dataframe

    Returns:
    pandas.DataFrame: The tasks dataframe with the impossible tasks removed
    """
    tasks_df = tasks_df[~tasks_df["id"].isin(impossible_tasks["id"])]
    return tasks_df

def map_task_with_possible_servers(tasks_df: DataFrame, server_df: DataFrame) -> dict[int, DataFrame]:
    """
    Create a map with key of task and the servers that it can run on
    The value is a dataframe of servers that can run the task

    Helpful for determining which servers can run a task (mapped to each task)

    Parameters:
    tasks_df (pandas.DataFrame): The tasks dataframe

    Returns:
    dict[int, pandas.DataFrame]: A map of tasks to servers that can run the task
    """
    task_servers = {}
    for index, task in tasks_df.iterrows():
        task_servers[task["id"]] = server_df[
            (server_df["ram"] >= task["ram"]) &
            (server_df["cpu"] >= task["cores"])
        ]
    return task_servers

def add_must_run_by(tasks_df: DataFrame) -> DataFrame:
    """
    Add the "must_run_by" column to the tasks dataframe
    Calculated by 'completed_by + id - turns'
    If 'completed_by' is -1, then 'must_run_by' is -1
    Determines the latest turn that a task must be run by

    Parameters:
    tasks_df (pandas.DataFrame): The tasks dataframe

    Returns:
    pandas.DataFrame: The tasks dataframe with the "must_run_by" column added
    """
    tasks_df["must_run_by"] = tasks_df["completed_by"] + tasks_df["id"] - tasks_df["turns"]
    tasks_df.loc[tasks_df["completed_by"] == -1, "must_run_by"] = -1
    return tasks_df