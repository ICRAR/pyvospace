# Program to start all the various implementations and things
import multiprocessing as mp
import subprocess
import os
from sys import exit
import atexit

def run_container_db():
    # Function to run the database in a docker container
    cmd = ["docker", "container", "start", "pyvospace-db"]
    os.chdir(os.path.join(os.environ["PYVOSPACE_DIR"], "pyvospace", "server", "deploy"))
    logfile=os.path.join(os.environ["PYVOSPACE_DIR"], "scripts", "log", "pyvospace_db.log")
    with open(logfile, "w") as fd:
        subprocess.call(["docker-compose", "up"], stdout=fd, stderr=fd)

def run_ngas():
    # Run the NGAS server with logging
    logfile = os.path.join(os.environ["PYVOSPACE_DIR"], "scripts", "log", "ngas.log")
    cmd=[]
    cmd.append("ngamsServer")
    cmd.append("-cfg")
    cmd.append(os.path.join(os.environ["NGAS_STORAGE_DIR"],"cfg","ngamsServer.conf"))
    cmd=cmd+["-v", "4", "-autoonline"]
    with open(logfile, "w") as fd:
        subprocess.call(cmd, stdout=fd, stderr=fd)

def exit_fun():
#    #os.killpg(p_docker.pid, signal.SIGTERM)
    p_docker.terminate()
    p_ngas.terminate()
    p_ngas.join()
    p_docker.join()

# Open two processes
atexit.register(exit_fun)
p_docker=mp.Process(target=run_container_db)
p_docker.start()
p_ngas=mp.Process(target=run_ngas)
p_ngas.start()
