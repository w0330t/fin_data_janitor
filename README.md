# Binance历史数据处理工具

## 项目概述
本工具用于处理从Binance下载的历史K线数据，主要功能包括：
- 添加标准表头
- 转换时间戳格式
- 检查数据连续性
- 将CSV数据转换为高效的Parquet格式

## 环境要求
- Python 3.7+
- 依赖库：`pandas`, `rich`, `argparse`

## 安装依赖
```bash
pip install pandas rich
```

## 使用说明
### 基本命令
```bash
python process_binance_data.py [--input 输入目录] [--output 输出目录]
```

### 参数说明
| 参数     | 默认值                                      | 描述               |
|----------|---------------------------------------------|--------------------|
| `--input`  | `/mnt/unraid/TradingData/crypto/binance_data/aws_data/data/futures/um/daily/klines/` | 原始数据目录       |
| `--output` | `/mnt/unraid/TradingData/crypto/binance_data/processed/data/futures/um/daily/klines/` | 处理后的输出目录   |

### 示例
```bash
# 使用默认目录
python process_binance_data.py

# 自定义目录
python process_binance_data.py \
  --input /path/to/raw_data \
  --output /path/to/processed_data
```

## 输入输出
### 输入数据
- 格式：ZIP压缩的CSV文件
- 目录结构：`/交易对/时间间隔/YYYY-MM-DD.zip`
- 示例：`BTCUSDT/1m/2023-01-01.zip`

### 输出数据
- 格式：Parquet文件
- 文件命名：`{交易对}-{时间间隔}-{日期}.parquet`
- 示例：`BTCUSDT-1m-2023-01-01.parquet`

## 功能特性
1. **自动表头处理**：检测并跳过无效表头行
2. **时间戳转换**：将毫秒时间戳转换为可读时间格式
3. **数据连续性检查**：自动检测K线数据中的缺失时段
4. **错误处理**：跳过损坏的ZIP文件并记录警告

## 注意事项
- 程序会自动创建输出目录结构
- 已存在的输出文件会被跳过（避免重复处理）
- 数据连续性检查支持的时间间隔：1m, 5m, 15m, 1h, 4h, 1d