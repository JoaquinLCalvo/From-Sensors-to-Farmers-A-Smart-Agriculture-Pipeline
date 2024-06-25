import os
import subprocess


def execute_command(command):
    process = subprocess.Popen(
        command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, error = process.communicate()

    if process.returncode != 0:
        print(f"Error executing command: {command}")
        print(f"Error message: {error.decode('utf-8')}")
    else:
        print(f"Command executed successfully: {command}")
        print(f"Output: {output.decode('utf-8')}")


def main():
    # Change to the api_server directory
    api_server_path = os.path.join(os.getcwd(), 'api_server')
    os.chdir(api_server_path)

    # Build the Docker image
    build_command = 'docker build -t farming-api .'
    execute_command(build_command)

    # Run the Docker container
    run_command = 'docker run -d -p 8000:8000 farming-api'
    execute_command(run_command)


if __name__ == "__main__":
    main()
    print("API server run in http://localhost:8000")
