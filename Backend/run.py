import os
import subprocess
import time
import platform


def run_command(folder, command, new_terminal=False):
    os.chdir(folder)
    if new_terminal:
        if platform.system() == 'Windows':
            terminal_command = f'start cmd /c "{command}"'
        elif platform.system() == 'Darwin':  # macOS
            terminal_command = f'osascript -e \'tell application "Terminal" to do script "{command}"\''
        else:  # Linux
            terminal_command = f'gnome-terminal -- bash -c "{command}; exec bash"'
        process = subprocess.Popen(terminal_command, shell=True)
    else:
        process = subprocess.Popen(command, shell=True)
    os.chdir('..')
    return process


# List of tasks with folder and command
tasks = [
    ('mqtt_broker', 'docker-compose up -d'),
    ('spark_docker', 'docker-compose up -d'),
    ('sensors_to_mqtt', 'python ingest.py', True),
    ('spark_to_duckdb', 'python run_pipeline.py')
]

processes = []

for task in tasks:
    folder = task[0]
    command = task[1]
    new_terminal = task[2] if len(task) > 2 else False
    print(f"Running '{command}' in '{folder}'")
    process = run_command(folder, command, new_terminal)
    processes.append(process)

    # Wait a bit between starting processes
    time.sleep(5)

# Wait for all processes to complete
for process in processes:
    process.wait()

print("All steps processed successfully.")
