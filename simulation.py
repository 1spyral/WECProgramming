import pandas as pd
import time

from csv_io import write_event, write_task

class Task:
    # Constants
    task_id: int
    cores: int
    ram: int
    turns: int
    complete_by: int
    # Gives you the # of turns that are allowed as "gaps" (i.e. you can still do other things for this many turns)
    must_run_by: int

    # Variables
    read_turn: int # The turn in which the task was read

    def __init__(self, task_id, cores, ram, turns, complete_by, must_run_by):
        self.task_id = task_id
        self.cores = cores
        self.ram = ram
        self.turns = turns
        self.complete_by = complete_by
        self.must_run_by = must_run_by

        # This should not exist unless the task is read
        self.read_turn = -1
    
    def __str__(self):
        return f"Task {self.task_id} with {self.cores} cores, {self.ram} ram, and must complete by turn {self.complete_by}"
    
    def __repr__(self):
        return self.__str__()

class Server:
    # Constants
    server_id: int # Unique identifier
    total_cpu: int # Total # of cores available
    total_ram: int # Total # of ram available
    watt_per_core: int # Wattage per core

    # Variables
    watt: int # How many watts the server has used so far
    ram_used: int # How much ram is currently being used

    curr_turn: int # Current turn the server is in

    # Only dependent on the current task
    # cpu_used is not required since we can only execute 1 task and we can check the task's cores
    curr_task: Task # The current task being executed

    tasks: list[Task] # Tasks that are currently in queue
    
    def __init__(self, server_id, cpu, watt, ram, power_median, turn_median, core_median):
        self.server_id = server_id
        self.total_cpu = cpu
        self.total_ram = ram
        self.watt = watt
        self.ram_used = 0
        self.cpu_used = 0
        self.curr_task = None
        # 1 indexed
        self.curr_turn = 0
        self.tasks = [] # Queue, not necessarily the order
        self.value = self.total_cpu / core_median + self.watt / power_median

    
    def __str__(self):
        return f"Server {self.server_id} with {self.cpu} cores, {self.watt} wattage, and {self.ram} ram"
    
    def __repr__(self):
        return self.__str__()

    def read_task(self, task: Task):
        # Can only read 1 task per turn, but can execute the task on the same turn
        # Assert: the server can execute the task
        # Make sure there is enough RAM left to execute
        # And make sure the server has enough cores to execute the task in the future
        assert task.cores <= self.total_cpu and task.ram <= self.total_ram - self.ram_used
        # Update the task's read turn
        # This is updated by clean() at the start of the turn already so the server is up to date
        task.read_turn = self.curr_turn

        self.tasks.append(task)
        # self.cpu_used += task.cores
        # Queuing takes up RAM, but not CPU
        self.ram_used += task.ram
    
    def execute_task(self, task: Task):
        """
        All this function does it set the current task to the task
        and remove the task from the queue

        Parameters:
        task (Task): The task to execute
        """
        # Assumption: the server can execute the task
        # assert task in self.tasks and task.turn == self.curr_turn
        self.curr_task = task
        # Don't need the task in queue anymore, but the RAM should still be there
        self.tasks.remove(task)
    
    def compute_weight(self, task: Task) -> int:
        # Compute the weight of the task based on the server's constraints
        # Assumption: the server can execute the task
        # We already know there are unmovable tasks in the queue

        # Some cool algorithm to determine weight

        power = self.watt * task.cores
        ram = task.ram / self.total_ram * self.value

        turn_weight = ((task.must_run_by - self.curr_turn) * 2 - task.turns) / task_median
        ram_weight = task.ram / self.total_ram

        weight = (turn_weight * 10 + ram_weight * 5) * self.value

        return weight
    
    def clean(self, global_turn: int):
        """
        Cleans the server at the start of a turn

        Check if current task is finished (turns_left == 0)
        Deallocate resources, clean the server, update power consumption, and set curr to None
        Update current turn to the global turn

        Parameters:
        turn (int): The current turn
        """

        self.curr_turn = global_turn

        if self.curr_task is not None and self.curr_task.turns_left == 0:
            # Deallocate resources, clean the server
            self.cpu_used -= self.curr_task.cores
            self.ram_used -= self.curr_task.ram
            self.curr_task = None
            # Write task as completed
            write_task(globals()["global_output_df"], global_turn, self.curr_task.task_id, status=1, power=self.watt, server_id=self.server_id)
            write_event(globals()["global_output_df"], "Task", time.time(), global_turn, self.curr_task.task_id, action="Completed")

        for task in self.tasks:
            if task.must_run_by < global_turn:
                self.tasks.remove(task)
                self.ram_used -= task.ram
                # Discard task
                write_task(globals()["global_output_df"], global_turn, task.task_id, status=0, power=0, server_id=0)
                write_event(globals()["global_output_df"], "Task", current_time, turn, task.task_id, action="Failed")
    
    def tick(self):
        """
        Runs at the end of a turn
        
        This function is ran AFTER the server has, potentially, read a task

        If the server is currently executing a task, then decrement the turns_left by 1
        Otherwise, we choose an optimal task to run
        """

        if self.curr_task is None:
            # Choose the optimal task to run
            optimal_task = self.choose_optimal_task_to_run()
            if optimal_task is not None:
                self.execute_task(optimal_task)
        
        # This is still necessary because choose_optimal_task_to_run only runs if queue is not empty
        if self.curr_task is not None:
            # Assumption: no tasks with 0 turns
            # Update the task's turns left, this will be picked up in the clean function if the task is done
            self.curr_task.turns_left -= 1

            # Note that turns_left is updated here because you can read and run on the same turn
        else:
            print("Server with id {self.server_id} has no task to execute")
    
    def choose_optimal_task_to_run(self) -> Task | None:
        """
        Black magic to determine the optimal task to run
        """
        # Choose the optimal task to run based on the server's constraints
        # Assumption: the server can execute the task
        # We already know there are unmovable tasks in the queue

        # Some cool algorithm to determine optimal task to minimize RAM

        # Prioritize tasks with a complete_by value

        for task in self.tasks:
            if task.cores <= self.total_cpu - self.cpu_used and task.ram <= self.total_ram - self.ram_used:
                self.execute_task(task)
                return task
        return None

    def is_empty(self):
        """
        Check if the server has any tasks in the queue and is not currently executing a task
        """
        return len(self.tasks) == 0 and self.curr_task is None
    
def find_possible_servers(task: Task, servers: list[Server]) -> list[Server]:
    # Find possible servers that can execute the task
    possible_servers = []
    for server in servers:
        if task.cores <= server.total_cpu and task.ram <= server.total_ram - server.ram_used:
            possible_servers.append(server)
    return possible_servers