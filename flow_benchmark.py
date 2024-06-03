# Copyright 2023 Greptime Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# benchmark for basic flow task & inserting rows into the database

import requests
import sched, time
import sys
import subprocess
import os
import traceback

TOTAL = 25_000 * 20
WRITE_SPEED = 25_000
WRITE_CNT = 0
CHILD_PROCS: list[subprocess.Popen] = []
PARQUET_PATH = "/home/discord9/greptimedb/numbers_input.parquet"

def do_sql(sql:str):
    do_sql_psql_client(sql)

def do_sql_psql_client(sql: str):
    print("SQL:", sql)
    output = subprocess.run(["psql", "-h", "127.0.0.1", "-p", "4003", "-d", "public", "-c",sql])
    print("Psql=>", output)

def do_sql_http(sql: str):
    print("SQL: ", sql)
    url = 'http://localhost:4000/v1/sql?db=public'
    data = {
    'sql': sql
    }
    response = requests.post(url, data=data)
    # raise an exception if the request failed
    response.raise_for_status()
    print("RESPONSE: ", response.text)
    return response

def send_lp(start, data):
    # Send the POST request
    url = "http://localhost:4000/v1/influxdb/write?db=public&precision=ms"
    response = requests.post(url, data=data)
    # Check the response
    if response.status_code >= 300:
        print(
            f"Failed to send log line {start}: {response.status_code} {response.text}"
        )




def do_insert(speed: int):
    global WRITE_CNT

    if WRITE_CNT>= TOTAL:
        print(f"\rWrite {WRITE_CNT} rows. Done.")
        return 
    print(f"\rWrite {WRITE_CNT} rows", end="")
    index = WRITE_CNT
    start_time = 1716446904527
    per_request = 1000
    for i in range(0, int(speed/per_request)):
        data = ""
        for cur_index in range(index, index + per_request):
            data += f"numbers_input, number={cur_index}i {start_time+cur_index}\n"
        index += per_request
        send_lp(i, data)
    WRITE_CNT += speed

def create_source_numbers_input_table_to_copy_from():
    speed = WRITE_SPEED
    while WRITE_CNT < TOTAL:
        do_insert(speed)
    do_sql(f"""
COPY numbers_input TO 
"{PARQUET_PATH}"
WITH (format = 'parquet');
""")

def call_copy_from():
    do_sql(f"""
COPY numbers_input FROM 
"{PARQUET_PATH}"
WITH (format = 'parquet');
""")

def clean_stuff():
    # TODO: wait in here for a bit till database open ports
    pass

def with_flow_n(n:int, verbose:bool):
    clean_stuff()
    do_sql(
"""
CREATE TABLE numbers_input (
    number Int64,
    ts TimestampNanosecond,
    TIME INDEX(ts)
);
"""
    )
    for i in range(n):
        do_sql(
f"""
CREATE FLOW test_numbers_{i}
SINK TO out_num_cnt_{i}
AS 
select count(number) from numbers_input;
"""
    )
    
    call_copy_from()
    print("main loop done, wait for 5 seconds")
    time.sleep(5)
    if verbose:
        for i in range(n):
            do_sql(f"select * from out_num_cnt_{i};")

def with_flow():
    clean_stuff()
    do_sql(
"""
CREATE TABLE numbers_input (
    number Int64,
    ts TimestampNanosecond,
    TIME INDEX(ts)
);
"""
    )
    do_sql(
"""
CREATE FLOW test_numbers 
SINK TO out_num_cnt
AS 
select count(number) from numbers_input;
"""
    )
    call_copy_from()
    print("main loop done, wait for 5 seconds")
    time.sleep(5)
    do_sql("select * from out_num_cnt;")

def without_flow():
    clean_stuff()
    do_sql(
"""
CREATE TABLE numbers_input (
    number Int64,
    ts TimestampNanosecond,
    TIME INDEX(ts)
);
"""
    )

    call_copy_from()
    print("main loop done, wait for 5 seconds")
    time.sleep(5)
    do_sql("select count(number) from numbers_input;")


def run_database(run_name:str, binary_path = "./greptime", use_adb = False):
    """
    Run the database in standalone mode
    run_name: str - the name of the run, used for log file names
    binary_path: str - the path to the binary, default to "./greptime"
    remote: bool - whether to run the database on a remote device using adb, default to False
    """
    global CHILD_PROCS
    subprocess.run(["adb", "forward", "--remove-all"])
    if use_adb:
        subprocess.run(["adb", "shell", "rm", "-rf", "/data/local/tmp/greptimedb/"])
    else:
        subprocess.run(["rm", "-rf", "/tmp/greptimedb"])
    run_name.replace(" ", "_")

    my_env = os.environ.copy()
    my_env["RUST_LOG"] = "info,flow=debug"

    db_log = open(f"db_{run_name}.log", "w")
    db_start_cmds = [binary_path, "standalone", "start"]
    if use_adb:
        subprocess.run(["adb", "forward", "tcp:4000", "tcp:4000"])
        subprocess.run(["adb", "forward", "tcp:4003", "tcp:4003"])

        subprocess.run(["adb", "root"])
        db_start_cmds = ["adb", "shell"] + ["/data/greptime_binary/greptime", "standalone", "start", "-c", "/data/greptime_binary/config.toml"]
    
    p = subprocess.Popen(db_start_cmds, stdout=db_log, stderr=db_log, env=my_env)
    CHILD_PROCS.append(p)

    if not use_adb:
        pid = p.pid
    elif use_adb:
        find_pid_cmd = ["adb", "shell", "pidof", "greptime"]
        pid = subprocess.run(find_pid_cmd, stdout=subprocess.PIPE).stdout.decode("utf-8").strip()

    mon_log = open(f"cpu_memory_usage_{run_name}.log", "w")
    moniter_cmd = ["pidstat","-r", "-u", "-h", "-p",f"{pid}", "1"]
    if use_adb:
        moniter_cmd = ["adb", "shell"] + moniter_cmd
    monitor = subprocess.Popen(moniter_cmd, stdout=mon_log, stderr=mon_log)
    CHILD_PROCS.append(monitor)
    time.sleep(5)

if __name__ == "__main__":
    try:
        print(sys.argv)
        run_type = sys.argv[1] #create/baseline/flow/full
        is_remote = sys.argv[2]=="adb" if len(sys.argv) >= 3 else False
        is_remote = bool(is_remote)
        if is_remote:
            print("Running on remote device")
        else:
            print("Running on local device")

        binary_path = sys.argv[3] if len(sys.argv) >= 4 else None
        parquet_path = sys.argv[4] if len(sys.argv) >= 5 else None
        if parquet_path:
            PARQUET_PATH = parquet_path

        # wait for the database to start
        if run_type =="create":# create parquet file to copy from
            run_database("create", binary_path, is_remote)
            create_source_numbers_input_table_to_copy_from()
        elif run_type == "flow":# run flow benchmark
            run_database("flow", binary_path, is_remote)
            with_flow()
        elif run_type.startswith("flow_"):
            run_args = run_type.split("_")
            n = int(run_args[1])
            verbose = len(run_args) >= 3 and run_args[2] == "v"
            run_database(run_type, binary_path, is_remote)
            with_flow_n(n, verbose)
        elif run_type == "baseline":# run baseline benchmark
            run_database("baseline", binary_path, is_remote)
            without_flow()
        elif run_type == "full":# run full benchmark(baseline+flow)
            print("Running full test")
            print("Baseline first")
            run_database("baseline", binary_path, is_remote)
            without_flow()
            for p in CHILD_PROCS:
                p.kill()
            CHILD_PROCS= []
            WRITE_CNT = 0
            # Reset everything
            print("Then flow")
            run_database("flow", binary_path, is_remote)
            with_flow()
    except Exception as e:
        print(e)
        # print traceback
        print(traceback.format_exc())
        print()

        raise e
    finally:
        print("Prepare to terminate all child processes.")
        time.sleep(5) # wait for the result to be written back
        # wait for the result to be written back
        for p in CHILD_PROCS:
            p.kill()
        print("All child processes terminated.")
        os._exit(0)