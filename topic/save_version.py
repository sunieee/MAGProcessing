# 这个脚本用来保存之前生成的topic信息，以当前时间作为topic版本
import shutil
import os
import datetime

current_time = str(datetime.datetime.now())
current_time_lst = current_time.split(' ')
dir_version = current_time_lst[0][2:] + '_' + current_time_lst[1][0:8]

directory = ["output", "model", "paper"]
target_dir = os.path.join("version", dir_version)
if os.path.exists(target_dir) == False:
    os.mkdir(target_dir)

for source_dir in directory:
    shutil.move(source_dir, target_dir)