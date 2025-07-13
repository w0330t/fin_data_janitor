import os
import sys
import argparse
import zipfile
import pandas as pd
from datetime import datetime, timedelta
from rich.progress import track

import re
import numpy as np

# Binance K线数据列定义
COLUMNS = [
    'open_time', 'open', 'high', 'low', 'close', 'volume',
    'close_time', 'quote_volume', 'count',
    'taker_buy_volume', 'taker_buy_quote_volume', 'ignore'
]

# 默认目录配置
BASE_DIR = "/mnt/unraid/TradingData/crypto/binance_data"
PROCESS_DIR ="/data/futures/um/daily/klines/"

DEFAULT_INPUT_DIR = BASE_DIR + "/aws_data" + PROCESS_DIR
DEFAULT_OUTPUT_DIR = BASE_DIR + "/processed" + PROCESS_DIR


def process_binance_data(input_path, output_path):
    """
    处理Binance数据：添加表头、检查数据完整性、保存为Parquet格式
    """
    print(f"开始处理数据: {input_path}")
    print(f"输出目录: {output_path}")
    
    os.makedirs(output_path, exist_ok=True)
    
    if not os.path.exists(input_path):
            print(f"[错误] 输入目录不存在: {input_path}")
            return
    symbols = [s for s in os.listdir(input_path) if os.path.isdir(os.path.join(input_path, s))]
    
    for symbol in track(symbols, description="开始处理中..."):
        symbol_path = os.path.join(input_path, symbol)
        if not os.path.isdir(symbol_path):
            continue
            
        for interval_dir in os.listdir(symbol_path):
            interval_path = os.path.join(symbol_path, interval_dir)
            if not os.path.isdir(interval_path):
                continue
                
            symbol_interval_output_path = os.path.join(output_path, symbol, interval_dir)
            os.makedirs(symbol_interval_output_path, exist_ok=True)
            
            zip_files = [f for f in os.listdir(interval_path) if f.endswith('.zip')]
            for zip_file in zip_files:
                zip_path = os.path.join(interval_path, zip_file)
                
                date_str = re.search(r'(\d{4}-\d{2}-\d{2})', zip_file)
                date_str = date_str.group(1) if date_str else "unknown_date"
                
                output_file = os.path.join(symbol_interval_output_path, f"{symbol}-{interval_dir}-{date_str}.parquet")

                if os.path.exists(output_file):
                    # print(f"    [信息] 文件已存在，跳过: {os.path.basename(output_file)}")
                    continue
                try:
                    df = pd.read_csv(zip_path, header='infer', names=COLUMNS)
                    
                    # 检查第一行是否与列名相同
                    if not df.empty and df.iloc[0].astype(str).tolist() == COLUMNS:
                        df = df.iloc[1:].reset_index(drop=True)
                    if df.empty:
                        continue
                    
                    df['open_time'] = pd.to_datetime(pd.to_numeric(df['open_time'], errors='coerce'), unit='ms', errors='coerce')
                    df['close_time'] = pd.to_datetime(pd.to_numeric(df['close_time'], errors='coerce'), unit='ms', errors='coerce')
                    
                    invalid_mask = df['open_time'].isna() | df['close_time'].isna()
                    if invalid_mask.any():
                        df = df[~invalid_mask]

                    if df.empty:
                        continue

                    check_data_continuity(df, interval_dir)
                    
                    df = df[COLUMNS]
                    # df.to_csv(output_file, index=False)
                    df.to_parquet(output_file)
                except zipfile.BadZipFile:
                    print(f"    [警告] 文件损坏或非ZIP格式，已跳过: {zip_path}")
                    continue
    
    print(f"数据处理完成")

def check_data_continuity(df, interval):
    """检查数据连续性并报告缺失"""
    interval_map = {
        '1m': 60_000, '5m': 300_000, '15m': 900_000,
        '1h': 3_600_000, '4h': 14_400_000, '1d': 86_400_000
    }
    expected_interval = interval_map.get(interval, 3_600_000)
    
    # 此时 'open_time' 已是 datetime 类型，无需重复转换
    time_diffs = df['open_time'].diff().dt.total_seconds() * 1000
    gaps = time_diffs[time_diffs > expected_interval * 1.1]
    
    if not gaps.empty:
        print(f"    [信息] 在 {df.iloc[0]['open_time'].date()} 发现 {len(gaps)} 处数据缺失")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='处理Binance历史数据')
    parser.add_argument('--input', default=DEFAULT_INPUT_DIR, 
                        help=f'原始数据目录 (默认: {DEFAULT_INPUT_DIR})')
    parser.add_argument('--output', default=DEFAULT_OUTPUT_DIR, 
                        help=f'处理后的数据输出目录 (默认: {DEFAULT_OUTPUT_DIR})')
    
    args = parser.parse_args()

    process_binance_data(
        input_path=args.input,
        output_path=args.output
    )