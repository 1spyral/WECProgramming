import time
import pandas as pd
from simulation import Task, Server, find_possible_servers
from preprocess import find_impossible_tasks, remove_impossible_tasks, add_must_run_by
from csv_io import read_servers, read_tasks, write_df_as_file_output, write_event, write_task
from collections import deque
from medians import set_medians

# Initialize global dataframes with column names

global_output_df = pd.DataFrame(columns=["Current Turn", "Task Number", "Status", "Total Power", "Server Number"])
global_simulation_df = pd.DataFrame(columns=["Update Type", "TimeStamp in seconds", "Turn Number", "Server/Task Number", "Action/CPU Cores", "RAM"])

debug_mode = True

if __name__ == "__main__":
    # Counting the number of turns ran
    turns = 0
    # Timing the simulation
    start = time.time()

    # Read the data
    server_df = read_servers()
    tasks_df = read_tasks()
    
    if debug_mode:
        print("Server Dataframe:")
        print(server_df)
        print("Tasks Dataframe:")
        print(tasks_df)

    # Preprocess the data
    # We only sanitize the tasks, and reorder the servers
    # Servers are sorted by power consumption (watt per core)
    # server_df = server_df.sort_values(by="watts", ascending=False)

    # Find impossible tasks
    impossible_tasks = find_impossible_tasks(tasks_df, server_df)

    if debug_mode:
        print("Impossible Tasks:")
        print(impossible_tasks)
    
    # Remove impossible tasks
    tasks_df = remove_impossible_tasks(tasks_df, impossible_tasks)

    if debug_mode:
        print("Tasks Dataframe After Removing Impossible Tasks:")
        print(tasks_df)

    # Set the medians
    medians = set_medians(tasks_df, server_df)

    # Add must run by column/label
    tasks_df = add_must_run_by(tasks_df)

    if debug_mode:
        print("Tasks Dataframe After Adding Must Run By:")
        print(tasks_df)

    # Queue of tasks to be executed, must maintain original dataframe order
    # We should already know before the simulation whether or not a task will be run, and on which server

    # make a tasks array that is the same order as the original dataframe, read 1 at a time
    tasks = deque([])
    for index, row in tasks_df.iterrows():
        tasks.append(Task(row["id"], row["cores"], row["ram"], row["turns"], row["completed_by"], row["must_run_by"]))

    # Initialize the servers as objects so easier to work with
    servers = []
    for index, row in server_df.iterrows():
        servers.append(Server(row["id"], row["cpu"], row["watts"], row["ram"], medians[0], medians[1], medians[2]))

    # Start the simulation (runs until all tasks are done)
    # Check if all tasks are read and all servers are done with their tasks
    while tasks and (False in [server.is_empty() for server in servers]):
        # Turns are 1-indexed
        turns += 1
        # Print the number of turns
        print(f"Turn {turns}")
                
        for server in servers:
            server.clean(turns) # at the start of a turn, clean the server
            # This should free the RAM, delete task, update power consumption, etc.

        current_task = tasks.popleft()
        print(f"Current Task: {current_task}")

        # Compute possible servers the current task can run on
        possible_servers = find_possible_servers(current_task, servers)

        # Find possible servers to run on
        print(f"Possible Servers: {possible_servers}")

        if not possible_servers:
            print(f"Task {current_task} cannot be run on any server.")
            # TODO: discard the task
            continue

        # Weights computed for instructions
        weights = []

        # Compute the weights for each server
        for server in possible_servers:
            # Compute the weight for the server
            weight = server.compute_weight(current_task)
            weights.append(weight)

        threshold = 10000

        # TODO: also discard if the weights for the servers are too high
        if min(weights) > threshold:
            print(f"Task {current_task} cannot be run on any server due to high weights.")
            write_task(globals()["global_output_df"], turns, current_task.task_id, status=0, power=0, server_id=0)
            write_event(globals()["global_output_df"], "Task", time.time(), turns, current_task.task_id, action="Failed")
            continue
        
        # Choose the server with the lowest weight
        chosen_server = possible_servers[weights.index(min(weights))]
        print(f"Chosen Server: {chosen_server}")

        # Read the task
        chosen_server.read_task(current_task)
        print(f"Task Read by Server: {chosen_server}")

        # Tick the servers
        for server in servers:
            server.tick(turns)

    write_df_as_file_output(global_output_df, path="output.csv")
    write_df_as_file_output(global_simulation_df, path="simulation.csv")