import os
import subprocess


def run_command(folder, command):
    print(f"Running command in {folder}: {command}")
    try:
        subprocess.run(command, shell=True, check=True, cwd=folder)
        print(f"Command in {folder} completed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error running command in {folder}: {e}")


# Define the folder structure and commands
folder_commands = [
    ("mqtt_broker", "docker-compose build"),
    ("spark_docker", "docker-compose build"),
    ("sensors_to_mqtt", "pip install -r requirements.txt"),
    ("spark_to_duckdb", "pip install -r requirements.txt")
]

# Get the current working directory
base_dir = os.getcwd()

# Iterate through the folder_commands list
for folder, command in folder_commands:
    folder_path = os.path.join(base_dir, folder)

    if os.path.exists(folder_path):
        run_command(folder_path, command)
    else:
        print(f"Folder not found: {folder_path}")

print("All commands have been executed.")
