import pandas as pd
import matplotlib.pyplot as plt
import glob
import os

database = os.environ.get('database', 'scigene_visualization_field')

def analyze_csv(filename, column_name):
    # 读取CSV文件
    if filename.endswith('.csv'):
        df = pd.read_csv(filename)
    else:
        # 使用glob找到文件夹下所有的CSV文件
        files = glob.glob(f"{filename}/links/*.csv")
        
        print('len(files):', len(files))
        # 读取并合并所有CSV文件
        all_dfs = [pd.read_csv(file) for file in files]
        df = pd.concat(all_dfs, ignore_index=True)
    
    # 计算均值和方差
    mean = df[column_name].mean()
    variance = df[column_name].var()

    print(f"Mean of {column_name}: {mean}")
    print(f"Variance of {column_name}: {variance}")

    # 绘制分布图
    plt.figure(figsize=(10, 6))
    plt.hist(df[column_name], bins=50, edgecolor='k', alpha=0.7)
    plt.title(f"Distribution of {column_name}")
    plt.xlabel(column_name)
    plt.ylabel('Frequency')
    plt.axvline(mean, color='r', linestyle='dashed', linewidth=1)
    plt.text(mean*1.05, plt.ylim()[1]*0.9, 'Mean', rotation=0, color='r')
    plt.grid(True, which='both', linestyle='--', linewidth=0.5)
    plt.tight_layout()

    # 保存分布图
    plt.savefig(f"out/{database}/{column_name}_distribution.png")
    # plt.show()

# 使用
analyze_csv(f'out/{database}/edge_proba.csv', 'proba')
