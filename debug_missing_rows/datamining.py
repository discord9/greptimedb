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

import re
    
with open("db_flow.log", "r") as f:
    # search all pattern like "flow::compute::render::src_sink: Rendered Source All send: <number> rows"
    # and extract the number
    src_send = 0
    cur_output = 0
    for (line_num, line) in enumerate(f.readlines()):
        if "flow::compute::render::src_sink: Rendered Source All send:" in line:
            src_send += int(re.search(r"flow::compute::render::src_sink: Rendered Source All send: (\d+) rows", line).group(1))
        elif "Reduce Accum Subgraph send: [(Row { inner: [Int64(" in line:
            cur_output = int(re.search(r"Reduce Accum Subgraph send: \[\(Row { inner: \[Int64\((\d+)\)", line).group(1))
            if src_send != cur_output:
                print(f"Error: line {line_num} src_send: {src_send} cur_output: {cur_output}")
                break
    