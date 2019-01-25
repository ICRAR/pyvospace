# Program to start all the various implementations and things
import multiprocessing as mp
import subprocess
import os
from sys import exit
import atexit

def run_docker():
    # Function to run the docker container
    cmd = ["docker", "container", "start", "pyvospace-db"]
    os.chdir(os.path.join(os.env["PYVOSPACE_DIR"], "server", "deploy"))
    logfile=os.path.join(os.env["PYVOSPACE_DIR"], "scripts", "log", "pyvospace_db.log")
    with open(logfile, "w") as fd:
        subprocess.call(["docker-compose", "up"], stdout=fd, stderr=fd)

def run_ngas():
    logfile = os.path.join(os.env["PYVOSPACE_DIR"], "scripts", "log", "ngas.log")
    cmd=[]
    cmd.append("ngamsServer")
    cmd.append("-cfg")
    cmd.append(os.path.join(os.environ["NGAS_STORAGE_DIR"],"cfg","ngamsServer.conf"))
    cmd.append("-v 4 -autoonline")
    while open(logfile, "w") as fd:
        subprocess.call(cmd, stdout=fd, stderr=fd)

def exit_fun():
    #os.killpg(p_docker.pid, signal.SIGTERM)
    p_docker.terminate()
    p_ngas.terminate()

# Open two processes
atexit.register(exit_fun)
p_docker=mp.Process(run_docker)
p_ngas=mp.Process(run_ngas)