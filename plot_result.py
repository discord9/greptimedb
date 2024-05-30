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



import numpy as np
import matplotlib.pyplot as plt

def parse_log(file_content: str):
    lines = file_content.split("\n")
    idx = 0
    all_logs = []
    while idx < len(lines):
        cur_line = lines[idx]
        idx += 1
        if cur_line.startswith("Linux"):
            continue
        if len(cur_line) == 0:
            continue
        if cur_line.startswith("# "):
            column_names = [r for r in cur_line.split(" ") if len(r) > 0]
            # remove the first element which is "#"
            column_names = column_names[1:]
            continue
        column_data = [r for r in cur_line.split(" ") if len(r) > 0]
        column_with_name = {name:val for name, val in zip(column_names, column_data)}
        all_logs.append(column_with_name)
    return all_logs
        
def get_cpu_rss(logs):
    cpu = []
    mem = []
    for log in logs:
        cpu.append(float(log["%CPU"]))
        mem.append(float(log["RSS"])/1000)
    cpu = np.array(cpu)
    mem = np.array(mem)
    return cpu, mem

if __name__ == "__main__":
    with open("cpu_memory_usage_baseline.log", "r") as f:
        baseline = parse_log(f.read())
        baseline_cpu_mem = get_cpu_rss(baseline)
    
    with open("cpu_memory_usage_flow.log", "r") as f:
        flow = parse_log(f.read())
        flow_cpu_mem = get_cpu_rss(flow)

    # 创建图表和子图
    fig, (ax1, ax2) = plt.subplots(2)

    ax1.plot(baseline_cpu_mem[0], label="baseline")
    ax1.plot(flow_cpu_mem[0], label="flow")
    ax1.plot(flow_cpu_mem[0] - baseline_cpu_mem[0], label="diff", color="b")
    ax1.set_ylabel("CPU(%)")
    ax1.set_xlabel("Time(s)")
    ax1.legend()

    ax2.plot(baseline_cpu_mem[1], label="baseline", color="r")
    ax2.plot(flow_cpu_mem[1], label="flow", color="g")
    ax2.plot(flow_cpu_mem[1] - baseline_cpu_mem[1], label="diff", color="b")
    ax2.set_ylabel("Memory(RSS) MB")
    ax1.set_xlabel("Time(s)")
    ax2.legend()

    plt.savefig("cpu_memory_usage.png")
