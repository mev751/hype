#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Hyperliquid SKHY / SKHYNIX 比率监控 + Bark 报警

修正要点（相对原脚本）：
  1. 币种名必须带 xyz: 前缀 —— SK Hynix 系合约在 Hyperliquid 的 XYZ builder-deployed dex 上：
       SKHY  (ADR / 美股) = xyz:SKHY
       SKHYNIX(韩股本尊)  = xyz:SKHX      <-- 原来写 "SKHYNIX" 会导致订阅无效、连接被服务器直接断开
  2. 订阅后若无 l2Book 返回，打错误提示而不是静默重连
  3. 价格过期检测（超过 STALE_SECONDS 没更新就不判断、不报警）
  4. 断线退避重连；BARK_KEY 支持环境变量，未配置时只打印不发送

用法:
  BARK_KEY=你的Key python3 hl_ratio_alert.py
  python3 hl_ratio_alert.py --test-alert     # 只发一条测试通知后退出
"""

import argparse
import json
import os
import sys
import threading
import time

import websocket

# 强制标准输出不缓存，便于实时看日志
sys.stdout.reconfigure(line_buffering=True)

# ---------------- 配置 ----------------
BARK_KEY = os.environ.get("BARK_KEY", "").strip() or "xxxx"
BARK_URL = "https://api.day.app/push"

COIN_ADR = "xyz:SKHY"      # SKHY（美股 ADR）
COIN_KR = "xyz:SKHX"       # SKHYNIX（韩股，本尊）
SCALE = 10                 # 比率 = SKHY * 10 / SKHX

LOWER_THRESHOLD = 1.445    # 低于此值报警
UPPER_THRESHOLD = 1.477    # 高于此值报警
COOLDOWN = 600             # 同一异常状态重复提醒最短间隔（秒）
STALE_SECONDS = 30         # 价格超过该秒数未更新则视为失效，不参与判断

WS_URL = "wss://api.hyperliquid.xyz/ws"

latest = {
    COIN_ADR: {"px": None, "ts": 0.0},
    COIN_KR: {"px": None, "ts": 0.0},
}


def bark_configured():
    return BARK_KEY and BARK_KEY != "xxxx"


def send_bark_alert(title, body, sound="calypso.caf"):
    if not bark_configured():
        print(f"[BARK未配置] 跳过推送: {title} | {body.replace(chr(10), ' ')}")
        return
    payload = {
        "device_key": BARK_KEY,
        "title": title,
        "body": body,
        "group": "SKHY监控",
        "level": "active",      # 锁屏强提醒
        "sound": sound,
    }
    try:
        import requests
        r = requests.post(BARK_URL, json=payload, timeout=8)
        print(f"Bark 推送 HTTP {r.status_code}: {r.text[:120]}")
    except Exception as e:
        print(f"Bark 发送异常: {e}")


def on_open(ws):
    print("=== WebSocket 已连接，订阅 l2Book ===")
    for coin in (COIN_ADR, COIN_KR):
        ws.send(json.dumps({"method": "subscribe", "subscription": {"type": "l2Book", "coin": coin}}))
        print(f"  订阅 {coin}")


def on_message(ws, message):
    try:
        data = json.loads(message)
        channel = data.get("channel")

        if channel == "subscriptionResponse":
            sub = data.get("data", {})
            print(f"[订阅确认] {sub.get('subscription', {}).get('coin')}")
            return

        if channel != "l2Book":
            return

        book = data.get("data", {})
        coin = book.get("coin")
        levels = book.get("levels") or []
        if coin not in latest:
            return
        if len(levels) < 2 or not levels[0] or not levels[1]:
            return

        best_bid = float(levels[0][0]["px"])
        best_ask = float(levels[1][0]["px"])
        latest[coin]["px"] = (best_bid + best_ask) / 2
        latest[coin]["ts"] = time.time()

    except Exception as e:
        print(f"解析消息出错: {e}")


def on_error(ws, error):
    print(f"WebSocket 错误: {error}")


def on_close(ws, code, msg):
    print(f"WebSocket 断开 (code={code}, msg={msg})")


def monitor_loop():
    last_state = "normal"
    last_alert = 0.0
    last_print = 0.0
    started = time.time()

    while True:
        time.sleep(1)
        adr = latest[COIN_ADR]
        kr = latest[COIN_KR]

        if adr["px"] is None or kr["px"] is None:
            if time.time() - started > 20 and int(time.time() - started) % 20 == 0:
                print("!! 20 秒内未收到完整行情，请检查订阅是否被拒（币种名/网络）")
            continue

        # 过期行情不判断
        if time.time() - min(adr["ts"], kr["ts"]) > STALE_SECONDS:
            continue

        ratio = (adr["px"] * SCALE) / kr["px"]

        now = time.time()
        if now - last_print >= 5:
            print(f"[{time.strftime('%H:%M:%S')}] SKHY={adr['px']:.4f} | SKHYNIX={kr['px']:.4f} | 比率={ratio:.4f}")
            last_print = now

        state = "high" if ratio > UPPER_THRESHOLD else ("low" if ratio < LOWER_THRESHOLD else "normal")

        if state != "normal" and (state != last_state or now - last_alert > COOLDOWN):
            if state == "high":
                title = f"🚨 SKHY 比率过高 ({ratio:.4f})"
                body = (f"突破上限 {UPPER_THRESHOLD}\n"
                        f"SKHY: {adr['px']}\nSKHYNIX: {kr['px']}\n比率: {ratio:.4f}")
                send_bark_alert(title, body, sound="calypso.caf")
            else:
                title = f"⚠️ SKHY 比率过低 ({ratio:.4f})"
                body = (f"跌破下限 {LOWER_THRESHOLD}\n"
                        f"SKHY: {adr['px']}\nSKHYNIX: {kr['px']}\n比率: {ratio:.4f}")
                send_bark_alert(title, body, sound="alarm.caf")
            print(f"-> 触发[{state}]报警: {ratio:.4f}")
            last_alert = now

        if state == "normal" and last_state != "normal":
            print("-> 比率回到正常区间")

        last_state = state


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--test-alert", action="store_true", help="只发一条测试通知后退出")
    args = parser.parse_args()

    if args.test_alert:
        send_bark_alert("✅ SKHY 监控测试", "这是一条 Bark 测试通知，收到即配置正确。")
        print("测试完成。Bark 已配置:" , bark_configured())
        return

    print(f"=== 监控启动 | 比率 = SKHY*{SCALE}/SKHYNIX ({COIN_ADR} / {COIN_KR}) ===")
    print(f"=== 报警区间: 低于 {LOWER_THRESHOLD} 或 高于 {UPPER_THRESHOLD} | Bark 已配置: {bark_configured()} ===")

    threading.Thread(target=monitor_loop, daemon=True).start()

    headers = {
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    }

    backoff = 5
    while True:
        try:
            ws = websocket.WebSocketApp(
                WS_URL,
                header=headers,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )
            ws.run_forever(ping_interval=30, ping_timeout=10)
        except Exception as e:
            print(f"WebSocket 运行异常: {e}")
        print(f"{backoff} 秒后重连...")
        time.sleep(backoff)
        backoff = min(backoff * 2, 60)


if __name__ == "__main__":
    main()
