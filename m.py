#!/usr/bin/env python3
"""
Binance & Hyperliquid 
"""

import json
import time
import os
import sys
import threading
import requests
from datetime import datetime

try:
    import websocket
except ImportError:
    print("缺少 websocket-client 库，请执行: pip install websocket-client")
    sys.exit(1)

# 开启 Windows 终端的 ANSI 转义字符支持 (防闪烁必备)
if os.name == 'nt':
    os.system('')

# ----------------- 全局配置 -----------------
# 币安标识
BN_SYMBOLS = ["SKHYUSDT", "SKHYNIXUSDT", "BZUSDT", "CLUSDT"]

# Hyperliquid 标识映射 (严格对应底层 xyz: 前缀)
HL_SYMBOLS_MAP = {
    "SKHY-USDC": "xyz:SKHY",
    "SKHYNIX-USDC": "xyz:SKHX",
    "BRENTOIL-USDC": "xyz:BRENTOIL",
    "WTIOIL-USDC": "xyz:CL"     # 修正为固定的 WTIOIL
}
HL_COIN_TO_DISPLAY = {v: k for k, v in HL_SYMBOLS_MAP.items()}

REFRESH_INTERVAL = 0.2  # 降低到 0.2s 刷新，因为不闪屏了，可以更快

# 数据仓库
bn_prices = {s: None for s in BN_SYMBOLS}
hl_prices = {s: None for s in HL_SYMBOLS_MAP.keys()}

# 资金费率数据仓库
# Binance: 每个 symbol 一个费率 + 下次结算时间(ms) + 结算周期(小时，默认8h，部分币种会被调整为1h/4h)
bn_funding = {s: {"rate": None, "next_time": None} for s in BN_SYMBOLS}
bn_funding_interval_hours = {
    "SKHYUSDT": 8,
    "SKHYNIXUSDT": 4,  # 币安官方设定 SKHYNIXUSDT 结算周期为 4 小时
    "BZUSDT": 8,
    "CLUSDT": 8,
}

# Hyperliquid: 官方固定为每 1 小时结算一次，币种之间不会有差异
HL_FUNDING_INTERVAL_HOURS = 1
hl_funding = {s: None for s in HL_SYMBOLS_MAP.keys()}

# 连接状态
bn_book_connected = False
bn_mark_connected = False
bn_connected = False
bn_error_msg = ""
hl_connected = False
hl_error_msg = ""

def update_bn_connected():
    global bn_connected
    bn_connected = bn_book_connected or bn_mark_connected


# ----------------- 初始化数据 (REST) -----------------
def fetch_initial_prices():
    """启动时秒抓双平台的打底数据，防止一直 Waiting"""
    global bn_error_msg, hl_error_msg
    
    # 1. 抓币安
    try:
        url_bn = "https://fapi.binance.com/fapi/v1/ticker/bookTicker"
        resp_bn = requests.get(url_bn, timeout=5)
        if resp_bn.status_code == 200:
            for item in resp_bn.json():
                sym = item["symbol"]
                if sym in bn_prices:
                    bn_prices[sym] = float(item["bidPrice"])
    except Exception as e:
        bn_error_msg = f"REST拉取失败: {str(e)[:30]}"

    # 1b. 抓币安资金费率打底 (premiumIndex 里同时带 lastFundingRate 和 nextFundingTime)
    try:
        url_bn_premium = "https://fapi.binance.com/fapi/v1/premiumIndex"
        resp_premium = requests.get(url_bn_premium, timeout=5)
        if resp_premium.status_code == 200:
            for item in resp_premium.json():
                sym = item.get("symbol")
                if sym in bn_funding:
                    rate = item.get("lastFundingRate")
                    next_t = item.get("nextFundingTime")
                    if rate is not None:
                        bn_funding[sym]["rate"] = float(rate)
                    if next_t is not None:
                        bn_funding[sym]["next_time"] = int(next_t)
    except Exception as e:
        bn_error_msg = f"资金费率拉取失败: {str(e)[:30]}"

    # 1c. 抓币安资金费率结算周期 (大部分是8小时一笔，部分币种会被动态调整为1h/2h/4h)
    try:
        url_bn_info = "https://fapi.binance.com/fapi/v1/fundingInfo"
        resp_info = requests.get(url_bn_info, timeout=5)
        if resp_info.status_code == 200:
            for item in resp_info.json():
                sym = item.get("symbol")
                if sym in bn_funding_interval_hours:
                    bn_funding_interval_hours[sym] = int(item.get("fundingIntervalHours", 8))
        # 注意: fundingInfo 接口只返回"被调整过结算周期"的币种，没出现在返回列表里的都保持默认 8 小时
    except Exception:
        pass  # 拉取失败就保持默认 8 小时，不影响主流程

    # 2. 抓 Hyperliquid
    url_hl = "https://api.hyperliquid.xyz/info"
    headers = {"Content-Type": "application/json"}
    for display_sym, coin in HL_SYMBOLS_MAP.items():
        try:
            resp_hl = requests.post(url_hl, json={"type": "l2Book", "coin": coin}, headers=headers, timeout=5)
            if resp_hl.status_code == 200:
                data = resp_hl.json()
                if "levels" in data and len(data["levels"]) > 0 and len(data["levels"][0]) > 0:
                    hl_prices[display_sym] = float(data["levels"][0][0]["px"])
        except Exception as e:
            hl_error_msg = f"REST拉取失败: {str(e)[:30]}"
    # Hyperliquid 资金费率的打底不走 REST（币种在 meta 里的索引和 xyz: 前缀映射关系不稳定，
    # 直接等下面 WS 的 activeAssetCtx 推送即可，通常连接建立后几百毫秒内就会到，
    # 期间面板上资金费率会短暂显示 Waiting...）


# ----------------- Binance WebSocket -----------------
def bn_on_message(ws, message):
    try:
        data = json.loads(message)
        payload = data.get("data", {})

        # markPrice 推送：携带实时资金费率(r)和下次结算时间(T)，约每1秒推一次
        if payload.get("e") == "markPriceUpdate":
            sym = payload.get("s")
            if sym in bn_funding:
                rate = payload.get("r")
                next_t = payload.get("T")
                if rate is not None:
                    bn_funding[sym]["rate"] = float(rate)
                if next_t is not None:
                    bn_funding[sym]["next_time"] = int(next_t)
                    # 动态校验结算周期：若下次结算时间未落在8小时整点上(非00/08/16 UTC)，推导为4小时或更短
                    if (int(next_t) // 1000) % 28800 != 0 and bn_funding_interval_hours.get(sym) == 8:
                        bn_funding_interval_hours[sym] = 4
            return

        # bookTicker 推送：盘口最优买一价
        if "s" in payload and "b" in payload:
            sym = payload["s"]
            if sym in bn_prices:
                bn_prices[sym] = float(payload["b"])
    except:
        pass

def start_bn_book_ws():
    global bn_book_connected, bn_error_msg
    book_streams = [f"{s.lower()}@bookTicker" for s in BN_SYMBOLS]
    streams = "/".join(book_streams)
    url = f"wss://fstream.binance.com/stream?streams={streams}"

    def on_open(ws):
        global bn_book_connected, bn_error_msg
        bn_book_connected = True
        bn_error_msg = ""
        update_bn_connected()

    def on_close(ws, *args):
        global bn_book_connected
        bn_book_connected = False
        update_bn_connected()

    def on_error(ws, error):
        global bn_book_connected, bn_error_msg
        bn_book_connected = False
        bn_error_msg = str(error)
        update_bn_connected()

    while True:
        ws = websocket.WebSocketApp(url, on_message=bn_on_message, on_error=on_error, on_close=on_close, on_open=on_open)
        ws.run_forever()
        time.sleep(3)

def start_bn_mark_ws():
    global bn_mark_connected, bn_error_msg
    # Binance USDⓈ-M Futures 将 markPrice / 实时资金费率独立路由在 /market/stream 路径下
    mark_streams = [f"{s.lower()}@markPrice@1s" for s in BN_SYMBOLS]
    streams = "/".join(mark_streams)
    url = f"wss://fstream.binance.com/market/stream?streams={streams}"

    def on_open(ws):
        global bn_mark_connected, bn_error_msg
        bn_mark_connected = True
        bn_error_msg = ""
        update_bn_connected()

    def on_close(ws, *args):
        global bn_mark_connected
        bn_mark_connected = False
        update_bn_connected()

    def on_error(ws, error):
        global bn_mark_connected, bn_error_msg
        bn_mark_connected = False
        bn_error_msg = str(error)
        update_bn_connected()

    while True:
        ws = websocket.WebSocketApp(url, on_message=bn_on_message, on_error=on_error, on_close=on_close, on_open=on_open)
        ws.run_forever()
        time.sleep(3)

def start_bn_ws():
    # 启动两个并发连接：分别拉取 bookTicker (盘口) 和 markPrice (资金费率)
    t_mark = threading.Thread(target=start_bn_mark_ws, daemon=True)
    t_mark.start()
    start_bn_book_ws()


# ----------------- Hyperliquid WebSocket -----------------
def hl_keep_alive(ws):
    global hl_connected
    while hl_connected:
        time.sleep(5)  # 5秒心跳，防任何代理断切
        try:
            ws.send(json.dumps({"method": "ping"}))
        except:
            break

def hl_on_message(ws, message):
    global hl_connected
    hl_connected = True
    try:
        data = json.loads(message)
        channel = data.get("channel")

        if channel == "l2Book" and "data" in data:
            book = data["data"]
            coin, levels = book.get("coin"), book.get("levels")
            if coin and levels and len(levels) > 0 and len(levels[0]) > 0:
                if coin in HL_COIN_TO_DISPLAY:
                    hl_prices[HL_COIN_TO_DISPLAY[coin]] = float(levels[0][0]["px"])
            return

        # activeAssetCtx 推送：ctx.funding 就是当前这一小时的资金费率(小数，未年化)
        # Hyperliquid 结算周期固定为 1 小时，不需要像币安那样额外查询周期
        if channel == "activeAssetCtx" and "data" in data:
            body = data["data"]
            coin = body.get("coin")
            ctx = body.get("ctx", {})
            funding = ctx.get("funding")
            if coin in HL_COIN_TO_DISPLAY and funding is not None:
                hl_funding[HL_COIN_TO_DISPLAY[coin]] = float(funding)
            return
    except:
        pass

def hl_on_error(ws, error):
    global hl_error_msg, hl_connected
    hl_connected = False
    hl_error_msg = str(error)

def hl_on_close(ws, *args):
    global hl_connected
    hl_connected = False

def hl_on_open(ws):
    global hl_connected, hl_error_msg
    hl_connected = True
    hl_error_msg = ""
    threading.Thread(target=hl_keep_alive, args=(ws,), daemon=True).start()
    for coin in HL_SYMBOLS_MAP.values():
        ws.send(json.dumps({"method": "subscribe", "subscription": {"type": "l2Book", "coin": coin}}))
        ws.send(json.dumps({"method": "subscribe", "subscription": {"type": "activeAssetCtx", "coin": coin}}))

def start_hl_ws():
    url = "wss://api.hyperliquid.xyz/ws"
    while True:
        ws = websocket.WebSocketApp(url, on_message=hl_on_message, on_error=hl_on_error, on_close=hl_on_close, on_open=hl_on_open)
        ws.run_forever()
        time.sleep(2)


# ----------------- 终端渲染 UI -----------------
def fmt_p(val, sign=False):
    """格式化价格输出"""
    if val is None: return "Waiting..."
    return f"{val:+.6f}" if sign else f"{val:.6f}"

def fmt_price_funding(price, funding_rate, interval_hours,  next_time_ms=None):
    """价格 + 资金费率并排显示在同一个单元格里，中间留足间隔，不会挤在一起"""
    p_str = fmt_p(price)
    if funding_rate is None:
        f_str = "Waiting..."
    else:
        f_str = f"{funding_rate*100:+.4f}%/{interval_hours}h"
    if next_time_ms is not None:
        f_str += f" ({fmt_countdown(next_time_ms)})"
    return f"{p_str:<14}{f_str}"

def fmt_countdown(next_time_ms):
    """格式化距离下次资金费结算的倒计时 (hh:mm:ss 或 mm:ss)"""
    if next_time_ms is None:
        return "--:--"
    remain = (next_time_ms / 1000.0) - time.time()
    if remain < 0:
        return "00:00"
    h, remainder = divmod(int(remain), 3600)
    m, s = divmod(remainder, 60)
    if h > 0:
        return f"{h:02d}:{m:02d}:{s:02d}"
    return f"{m:02d}:{s:02d}"

def hl_next_funding_ms():
    """Hyperliquid 固定整点结算，计算距离下一个整点的毫秒时间戳"""
    now = time.time()
    next_hour = (int(now // 3600) + 1) * 3600
    return next_hour * 1000

def render_ui(time_str, running):
    """使用 ANSI 转义代码进行无闪烁原地渲染"""
    # 取出所有数据
    b_sk, b_skx = bn_prices['SKHYUSDT'], bn_prices['SKHYNIXUSDT']
    b_bz, b_cl = bn_prices['BZUSDT'], bn_prices['CLUSDT']
    
    h_sk, h_skx = hl_prices['SKHY-USDC'], hl_prices['SKHYNIX-USDC']
    h_bz, h_cl = hl_prices['BRENTOIL-USDC'], hl_prices['WTIOIL-USDC']
    
    # 算指标
    b_k = (b_sk * 10 / b_skx) if (b_sk and b_skx) else None
    b_d = (b_bz - b_cl) if (b_bz and b_cl) else None
    h_k = (h_sk * 10 / h_skx) if (h_sk and h_skx) else None
    h_d = (h_bz - h_cl) if (h_bz and h_cl) else None

    # -- 控制台无闪烁渲染 (核心) --
    sys.stdout.write("\033[H") # 光标归位到左上角 (取代 cls)
    
    lines = [
        "",
        "  Binance & Hyperliquid 实时监测系统 ",
        f"  时间: {time_str}   |   运行时间: {running} 秒",
        f"  BN状态: {'[连通] ●' if bn_connected else '[断开] X ' + bn_error_msg}",
        f"  HL状态: {'[连通] ●' if hl_connected else '[断开] X ' + hl_error_msg}",
        "  " + "="*76,
        "  [ 组1: SKHY & SKHYNIX ]",
        "                   |  Binance (USDT)                       |  Hyperliquid (USDC)",
        f"  SKHY             |  "
        f"{fmt_price_funding(b_sk, bn_funding['SKHYUSDT']['rate'], bn_funding_interval_hours['SKHYUSDT'], bn_funding['SKHYUSDT']['next_time']):<36} |  "
        f"{fmt_price_funding(h_sk, hl_funding['SKHY-USDC'], HL_FUNDING_INTERVAL_HOURS, hl_next_funding_ms())}",
        f"  SKHYNIX          |  "
        f"{fmt_price_funding(b_skx, bn_funding['SKHYNIXUSDT']['rate'], bn_funding_interval_hours['SKHYNIXUSDT'], bn_funding['SKHYNIXUSDT']['next_time']):<36} |  "
        f"{fmt_price_funding(h_skx, hl_funding['SKHYNIX-USDC'], HL_FUNDING_INTERVAL_HOURS, hl_next_funding_ms())}",
        f"  比值 k           |  {fmt_p(b_k):<36} |  {fmt_p(h_k)}",
        "  " + "-"*76,
        "  [ 组2: 原油系列 ]",
        "                   |  Binance (BZ / CL)                    |  Hyperliquid (BRENT / WTI)",
        f"  布伦特 (BRENT)   |  "
        f"{fmt_price_funding(b_bz, bn_funding['BZUSDT']['rate'], bn_funding_interval_hours['BZUSDT'], bn_funding['BZUSDT']['next_time']):<36} |  "
        f"{fmt_price_funding(h_bz, hl_funding['BRENTOIL-USDC'], HL_FUNDING_INTERVAL_HOURS, hl_next_funding_ms())}",
        f"  WTI原油 (WTI)    |  "
        f"{fmt_price_funding(b_cl, bn_funding['CLUSDT']['rate'], bn_funding_interval_hours['CLUSDT'], bn_funding['CLUSDT']['next_time']):<36} |  "
        f"{fmt_price_funding(h_cl, hl_funding['WTIOIL-USDC'], HL_FUNDING_INTERVAL_HOURS, hl_next_funding_ms())}",
        f"  差值 d           |  {fmt_p(b_d, True):<36} |  {fmt_p(h_d, True)}",
        "  " + "="*76,
        "  [Ctrl+C 退出]"
    ]
    
    # 逐行输出，并在每行末尾加上 \033[K (清除光标到行尾的残影)
    for line in lines:
        sys.stdout.write(line + "\n")
    sys.stdout.flush()

def main():
    # 运行前清屏一次即可
    os.system('cls' if os.name == 'nt' else 'clear')
    print("正在拉取底层双端数据...")
    
    fetch_initial_prices()
    
    threading.Thread(target=start_bn_ws, daemon=True).start()
    threading.Thread(target=start_hl_ws, daemon=True).start()
    
    start_time = time.time()
    
    # 先清空屏幕，为后续的无闪烁原地覆写腾出空间
    os.system('cls' if os.name == 'nt' else 'clear')
    
    while True:
        try:
            now = time.time()
            now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            running = int(now - start_time)
            
            render_ui(now_str, running)
            
            time.sleep(REFRESH_INTERVAL)
            
        except KeyboardInterrupt:
            print("\n已退出系统并安全保存。")
            break
        except Exception as e:
            time.sleep(1)

if __name__ == "__main__":
    main()
