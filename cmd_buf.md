# 本地测试
首先创建 parquet 文件，再测试
```bash

python3 flow_benchmark.py create local target/release/greptime;

python3 flow_benchmark.py baseline local target/release/greptime;

python3 flow_benchmark.py flow local target/release/greptime;
```

# adb 测试
```bash
# 先把 release binary 推上去
adb push target/aarch64-linux-android/release/greptime /data/greptime_binary/greptime

# 可选：创建 parquet 文件（如果之前没做的话）
python3 flow_benchmark.py create adb xxx "/data/greptime_binary/numbers_input.parquet";
# 基线
python3 flow_benchmark.py baseline adb xxx "/data/greptime_binary/numbers_input.parquet";
# flow
python3 flow_benchmark.py flow adb xxx "/data/greptime_binary/numbers_input.parquet";
```
