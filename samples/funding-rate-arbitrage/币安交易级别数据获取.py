import time
import ccxt
import pandas as pd
import os
from datetime import datetime, timedelta
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("binance_data_fetch.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

binance_options = {
    "timeout": 30000,  # 增加超时时间到30秒
    "enableRateLimit": True,  # 启用频率限制
}

def get_all_binance_symbols():
    """获取Binance所有可用的交易对"""
    logging.info("获取Binance所有可用货币对...")
    
    try:
        binance_spot = ccxt.binance(binance_options)
        # binance_usdm = ccxt.binanceusdm(binance_options)
        # binance_coinm = ccxt.binancecoinm(binance_options)
        
        logging.info("测试与Binance的连接...")
        ping_result = binance_spot.fetch_time()
        logging.info(f"连接成功，服务器时间: {ping_result}")

        logging.info("加载Binance现货市场...")
        binance_spot.load_markets()
        # logging.info("加载Binance U本位永续合约市场...")
        # binance_usdm.load_markets()
        # logging.info("加载Binance币本位永续合约市场...")
        # binance_coinm.load_markets()
        
        # 获取现货交易对
        spot_symbols = list(binance_spot.markets.keys())
        
        # # 获取USDT计价的永续合约
        # usdm_symbols = [symbol for symbol in binance_usdm.markets.keys() 
        #                if binance_usdm.markets[symbol].get('swap')]
        
        # # 获取USD计价的永续合约
        # coinm_symbols = [symbol for symbol in binance_coinm.markets.keys() 
        #                 if binance_coinm.markets[symbol].get('swap')]

        # 仅获取特定基准货币的交易对，减少数据量
        base_currencies = ['USDT', 'BTC', 'ETH']
        filtered_symbols = []
        
        for symbol in spot_symbols:
            if '/' in symbol:
                base = symbol.split('/')[1]
                if base in base_currencies:
                    filtered_symbols.append(symbol)

        logging.info(f"找到 {len(spot_symbols)} 个现货交易对")
        # logging.info(f"找到 {len(usdm_symbols)} 个U本位永续合约")
        # logging.info(f"找到 {len(coinm_symbols)} 个币本位永续合约")
        
        # return {
        #     "spot": spot_symbols,
        #     "usdm": usdm_symbols,
        #     "coinm": coinm_symbols
        # }
        return filtered_symbols
    except Exception as e:
        logging.error(f"获取Binance交易对时出错: {str(e)}")
        # 如果是网络错误，提供更多信息
        if "timeout" in str(e).lower() or "connection" in str(e).lower():
            logging.error("网络连接错误，请检查您的代理设置或网络连接")
        return {"spot": [], "usdm": [], "coinm": []}

def fetch_binance_trades_history(symbol, market_type, start_time, end_time):
    logging.info(f"获取 {market_type} {symbol} 在 {start_time} 到 {end_time} 之间的交易数据")
    
    # 转换时间为毫秒时间戳
    since = int(start_time.timestamp() * 1000)
    until = int(end_time.timestamp() * 1000)

    # 创建相应的交易所实例
    if market_type == "spot":
        exchange = ccxt.binance(binance_options)
    elif market_type == "usdm":
        exchange = ccxt.binanceusdm(binance_options)
    else:  # coinm
        exchange = ccxt.binancecoinm(binance_options)
    
    all_trades = []
    
    # 最大重试次数
    max_retries = 3
    page = 1
    
    try:
        end_time_param = None
        limit = 1000
        
        while True:
            retry_count = 0
            while retry_count <= max_retries:
                try:
                    params = {}
                    if end_time_param:
                        params['endTime'] = end_time_param
                    else:
                        params['endTime'] = until
                    
                    trades = exchange.fetch_trades(
                        symbol, 
                        since=since, 
                        limit=limit,
                        params=params
                    )
                    
                    if not trades:
                        break
                    
                    # 筛选时间范围内的交易
                    trades = [t for t in trades if since <= t['timestamp'] <= until]
                    
                    if not trades:
                        break
                    
                    all_trades.extend(trades)
                    
                    # 更新结束时间用于下一次请求
                    end_time_param = trades[-1]['timestamp'] - 1
                    
                    # 打印进度
                    if all_trades:
                        last_time = datetime.fromtimestamp(trades[-1]['timestamp'] / 1000).strftime('%Y-%m-%d %H:%M:%S')
                        logging.info(f"{symbol} 页 {page}: 已获取 {len(all_trades)} 条交易记录，最后时间: {last_time}")
                    
                    page += 1
                    time.sleep(0.2)  # 添加延时避免请求过于频繁
                    break
                    
                except Exception as e:
                    retry_count += 1
                    if retry_count > max_retries:
                        logging.error(f"获取 {symbol} 数据失败，已重试 {max_retries} 次: {str(e)}")
                        break
                    
                    logging.warning(f"获取 {symbol} 数据出错，正在重试 ({retry_count}/{max_retries}): {str(e)}")
                    time.sleep(5 * retry_count)
            
            if not trades or retry_count > max_retries:
                break
            
            if len(trades) < limit or trades[-1]['timestamp'] <= since:
                break
            
    except Exception as e:
        logging.error(f"处理 {symbol} 时出现错误: {str(e)}")
    finally:
        # 关闭交易所连接
        try:
            exchange.close()
        except:
            pass
    
    return all_trades

def aggregate_trades_to_ohlcv(trades, timeframe_ms=100):
    """将交易数据聚合成OHLCV格式的K线，生成固定时间间隔的Bar"""
    if not trades:
        return pd.DataFrame()
    
    df = pd.DataFrame(trades)
    
    if 'timestamp' not in df.columns or 'price' not in df.columns:
        return pd.DataFrame()
    
    # 确保有amount列，如果没有，尝试使用size或amount属性
    if 'amount' not in df.columns:
        if 'size' in df.columns:
            df['amount'] = df['size']
        elif 'quantity' in df.columns:
            df['amount'] = df['quantity']
        else:
            df['amount'] = 1.0  # 默认值，如果没有数量信息
    
    # 转换时间戳到datetime
    df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
    
    # 计算时间区间标记 - 向下取整到最近的timeframe_ms边界
    df['timegroup'] = df['timestamp'] // timeframe_ms * timeframe_ms
    
    # 获取整个时间范围
    min_time = df['timegroup'].min()
    max_time = df['timegroup'].max()
    
    # 创建完整的时间序列（固定100ms间隔）
    all_times = pd.DataFrame({
        'timegroup': range(int(min_time), int(max_time) + timeframe_ms, timeframe_ms)
    })
    
    # 按时间分组聚合数据
    grouped = df.groupby('timegroup').agg({
        'price': ['first', 'max', 'min', 'last'],
        'amount': 'sum',
        'datetime': 'first'
    })
    
    # 设置列名
    grouped.columns = ['open', 'high', 'low', 'close', 'volume', 'datetime']
    grouped = grouped.reset_index()
    
    # 与所有时间点合并，填充没有交易的时间点
    ohlcv = pd.merge(all_times, grouped, on='timegroup', how='left')
    
    # 向前填充OHLC数据（对于没有交易的时间点，使用前一个有效价格）
    ohlcv[['open', 'high', 'low', 'close']] = ohlcv[['open', 'high', 'low', 'close']].ffill()
    
    # 将未交易时间点的成交量设为0
    ohlcv['volume'] = ohlcv['volume'].fillna(0)
    
    # 对于缺失的datetime，基于timegroup创建
    ohlcv['datetime'] = ohlcv['datetime'].fillna(
        pd.to_datetime(ohlcv['timegroup'], unit='ms')
    )
    
    # 删除开始没有价格的行（无法前向填充）
    ohlcv = ohlcv.dropna(subset=['open'])
    
    # 按照Backtrader标准列排序
    ohlcv = ohlcv[['datetime', 'open', 'high', 'low', 'close', 'volume']]
    
    return ohlcv

def save_ohlcv_to_csv(ohlcv, market_type, symbol):
    if ohlcv.empty:
        logging.warning(f"{market_type} {symbol}: 没有数据可保存")
        return
    
    # 确保输出目录存在
    output_dir = os.path.join("data", "Binance")
    os.makedirs(output_dir, exist_ok=True)
    
    # 标准化符号名称以用作文件名
    safe_symbol = symbol.replace('/', '_')
    file_path = os.path.join(output_dir, f"{safe_symbol}.csv")
    
    try:
        # 确保datetime列格式正确
        if isinstance(ohlcv['datetime'].iloc[0], pd.Timestamp):
            ohlcv['datetime'] = ohlcv['datetime'].dt.strftime('%Y-%m-%d %H:%M:%S.%f')
        
        ohlcv.to_csv(file_path, index=False)
        logging.info(f"数据已保存到 {file_path}, 共 {len(ohlcv)} 条K线记录")
    except Exception as e:
        logging.error(f"保存数据到 {file_path} 失败: {str(e)}")

def main():
    # 创建输出目录
    os.makedirs("data/Binance", exist_ok=True)
    start_time = datetime(2025, 4, 6, 12, 0, 0)
    end_time = datetime(2025, 4, 6, 14, 0, 0)
    try:
        # 获取所有Binance前20个交易对
        symbols = get_all_binance_symbols()
        if not symbols:
            logging.error("无法获取有效的交易对，请检查网络连接或代理设置")
            return
        max_symbols = 20
        selected_symbols = symbols[:max_symbols]
        logging.info(f"将处理 {len(selected_symbols)} 个交易对，从 {start_time} 到 {end_time}")

        # 处理现货交易对
        for i, symbol in enumerate(selected_symbols):
            try:
                logging.info(f"处理交易对 {i+1}/{len(selected_symbols)}: {symbol}")
                trades = fetch_binance_trades_history(symbol, "spot", start_time, end_time)
                if trades:
                    ohlcv = aggregate_trades_to_ohlcv(trades, timeframe_ms=100)
                    save_ohlcv_to_csv(ohlcv, "spot", symbol)
                else:
                    logging.warning(f"{symbol} 指定时间范围内没有交易数据")
                time.sleep(0.2)  # 避免请求过于频繁
            except Exception as e:
                logging.error(f"处理 {symbol} 时出错: {str(e)}")
    except Exception as e:
        logging.error(f"程序执行过程中发生错误: {str(e)}")

if __name__ == "__main__":
    start_time = time.time()
    logging.info("开始获取Binance交易所数据...")
    main()
    elapsed_time = time.time() - start_time
    logging.info(f"数据获取完成，总耗时: {elapsed_time/60:.2f} 分钟")