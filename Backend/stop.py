import os
import subprocess


def stop_docker_compose(folder):
    current_dir = os.getcwd()
    os.chdir(folder)
    subprocess.run("docker-compose down", shell=True)
    os.chdir(current_dir)


# Stop docker-compose processes
print("Stopping mqtt_broker...")
stop_docker_compose("mqtt_broker")

print("Stopping spark_docker...")
stop_docker_compose("spark_docker")


print("All containers are stopped !")
