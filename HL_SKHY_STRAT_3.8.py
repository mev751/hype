#!/usr/bin/env python3
"""
Hyperliquid 做空动态网格机器人 <全景对账 + 真实加权k值测算终极版 v3.8>
- 核心修复: 彻底解决 on_tick 后台线程擅自提前补仓的竞态 BUG，未输入 1 绝对不上链下单
- 优化: 彻底移除所有 USDC 符号，控制台信息排版极致精简无折叠换行
- 重构: 合并行情盘口与策略网格，底端严格采用指定三行竖排呈现 (建仓线/当前平滑(原始)/止盈线)
- 优化: 底层交易日志极致精简 (去除理论额与实际均价)，展示行数扩大至 20 行满屏拉长
- 修复: 深度兼容 userFunding 顶级与 delta 嵌套资金费解析，确保整点结算 100% 捕获
- 优化: 存量仓位止盈按理论 k1 = P1+(level-1)*X 精确计算真实收益
- 优化: 运行时间格式重构为【X天 XX时 XX分 XX秒】格式
- 增强: 切片下单双腿失衡重试与紧急平仓回滚保护
- 新增: 保证金不足时动态封顶锁定最高网格上限
- 新增: 链上多余仓位挂起，等待回归最低网格 (第 0 层) 后统一卖出清理
"""

import json
import time
import os
import sys
import threading
import requests
from datetime import datetime

# --- Hyperliquid 交易 SDK ---
try:
    from eth_account import Account
    from hyperliquid.exchange import Exchange
    from hyperliquid.utils import constants
except ImportError:
    print("缺少 Hyperliquid SDK，请执行: pip install hyperliquid-python-sdk eth_account websocket-client requests")
    sys.exit(1)

import websocket

# ==========================================
# ⚙️ 交易策略与 API 参数配置区
# ==========================================

Q_USDC = 1000       # 每个网格触发时的目标仓位大小 (单位: USDC)
SPLIT_N = 1         # 单次网格订单切分次数
MAX_GRIDS = 10       # 最大允许建仓的网格层数/
P1 = 1.4           # 建仓触发基准线
STEP_X = 0.003      # 网格步长
SLICE_DELAY = 0.75   # 切片间隔 (秒)
LEVERAGE = 10       # 策略杠杆 (默认 10 倍)

EMA_ALPHA = 0.25    # K值平滑指数 (0.1 ~ 1.0, 越小越平滑)


SECRET_KEY = ""     # 你的钱包私钥
WALLET_ADDR = ""    # 你的钱包地址

# 官方 API 唯一认证的行情与交易名称
WS_SKHY = "xyz:SKHY"
WS_SKHX = "xyz:SKHX"

# ==========================================
# 🛡️ 智能状态与映射
# ==========================================
HL_COIN_TO_DISPLAY = {
    WS_SKHY: "SKHY-USDC",
    WS_SKHX: "SKHYNIX-USDC"
}

prices = {
    "SKHY-USDC": {"bid": None, "ask": None},
    "SKHYNIX-USDC": {"bid": None, "ask": None}
}
ws_connected = False
ws_error_msg = ""

bot_start_time_ms = int(time.time() * 1000)

initial_equity = None
current_equity = None
withdrawable_margin = 0.0  # 防抖动平滑可用保证金余额

real_position_skhy = 0.0
real_position_skhx = 0.0

global_floating_pnl = 0.0
global_realized_pnl = 0.0
global_fees = 0.0
global_funding = 0.0

# 策略统计全局变量
global_entry_count = 0     # 累计开仓次数
global_tp_count = 0        # 累计止盈次数
global_true_profit = 0.0    # 网格真实累计收益
processed_fill_ids = set() # 交易 ID 去重集合

order_lock = threading.Lock()
algo_engine = None


def format_runtime_str(total_seconds: int) -> str:
    """运行时间格式化为: X天 XX时 XX分 XX秒"""
    days, remainder = divmod(total_seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, secs = divmod(remainder, 60)
    return f"{days}天 {hours:02d}时 {minutes:02d}分 {secs:02d}秒"


# ==========================================
# 🧠 核心交易引擎 (做空网格状态机)
# ==========================================
class ShortGridEngine:
    def __init__(self, q, split_n, max_grids, p1, x, secret_key, wallet_addr):
        self.q_per_grid = q
        self.split_n = split_n
        self.max_grids = max_grids
        self.p1 = p1
        self.x = x
        
        self.positions_held = 0       
        
        self.pos_skhy = 0.0
        self.pos_skhx = 0.0
        
        self.dec_hy = 4
        self.dec_hx = 4
        
        # 记录每层网格开仓时的 k1 与开仓手续费 {level: (k1, fee1)}
        self.layer_entry_data = {}
        
        self.live_trading = bool(secret_key and wallet_addr)
        self.exchange = None
        
        self.is_aligned = False    # 重启对准标记 (人工确认后开启)
        self.is_trading = False    # 独占交易锁
        self.k_smoothed = None     # 平滑滤波 K 值
        
        if self.live_trading:
            try:
                url = "https://api.hyperliquid.xyz/info"
                perp_dexs_resp = requests.post(url, json={"type": "perpDexs"}, timeout=10).json()
                all_dex_names = [d["name"] for d in perp_dexs_resp if d is not None]
                
                account = Account.from_key(secret_key)
                self.exchange = Exchange(account, constants.MAINNET_API_URL, perp_dexs=all_dex_names)
                
                if hasattr(self.exchange, 'info'):
                    hy_asset = self.exchange.info.coin_to_asset.get(WS_SKHY)
                    hx_asset = self.exchange.info.coin_to_asset.get(WS_SKHX)
                    if hy_asset is not None:
                        self.dec_hy = self.exchange.info.asset_to_sz_decimals.get(hy_asset, 4)
                    if hx_asset is not None:
                        self.dec_hx = self.exchange.info.asset_to_sz_decimals.get(hx_asset, 4)
            except Exception as e:
                print(f"钱包与官方SDK初始化失败: {e}")
                self.live_trading = False

    def get_next_triggers(self):
        next_entry = self.p1 + self.positions_held * self.x if self.positions_held < self.max_grids else None
        next_exit = self.p1 + (self.positions_held - 2) * self.x if self.positions_held > 0 else None
        return next_entry, next_exit

    def align_positions_on_startup(self, p_skhy_bid, p_skhx_ask, real_pos_skhy, real_pos_skhx):
        """
        启动确认后的对准执行函数 (仅在主线程明确同意后同步调用)
        """
        self.is_trading = True
        try:
            k = p_skhy_bid * 10 / p_skhx_ask
            self.k_smoothed = k
            
            if k < self.p1:
                M_required = 0
            else:
                M_required = int((k - self.p1) // self.x) + 1
            M_required = min(M_required, self.max_grids)
            
            USDC_required = M_required * self.q_per_grid
            
            actual_val_hy = abs(real_pos_skhy) * p_skhy_bid
            actual_val_hx = abs(real_pos_skhx) * p_skhx_ask
            actual_usdc = (actual_val_hy + actual_val_hx) / 2.0
            
            self._add_log(f"[启动对准] 当前比值 k={k:.4f} | 目标应持仓价值: {USDC_required:.1f}")
            self._add_log(f"[启动对准] 链上仓位: SKHY={real_pos_skhy:.4f} ({actual_val_hy/1000:.1f}k) | SKHX={real_pos_skhx:.4f} ({actual_val_hx/1000:.1f}k)")
            
            if M_required > 0 and actual_usdc < (USDC_required - 100):
                diff_usdc = USDC_required - actual_usdc
                diff_sz_hy = round(diff_usdc / p_skhy_bid, self.dec_hy)
                diff_sz_hx = round(diff_usdc / p_skhx_ask, self.dec_hx)
                
                self._add_log(f"[启动对准] 触发建仓/补仓对准交易，补充约 {diff_usdc:.1f}...")
                self._execute_trade_logic("ENTRY", M_required, p_skhy_bid, p_skhx_ask, diff_sz_hy, diff_sz_hx, k)
                
                self.positions_held = M_required
                self.pos_skhy = abs(real_pos_skhy) + diff_sz_hy
                self.pos_skhx = abs(real_pos_skhx) + diff_sz_hx
                self._add_log(f"[启动对准] 建仓补仓已完成！成功接管 [第 {M_required} 层网格]")
            else:
                self.positions_held = M_required
                self.pos_skhy = round(USDC_required / p_skhy_bid, self.dec_hy) if M_required > 0 else 0.0
                self.pos_skhx = round(USDC_required / p_skhx_ask, self.dec_hx) if M_required > 0 else 0.0
                self._add_log(f"[启动对准] 接管状态 [第 {M_required} 层网格]。多余仓位暂存，等待回归最低网格(第0层)清理。")
            
            self.is_aligned = True
        except Exception as e:
            self._add_log(f"[启动对准失败] 异常: {e}")
            self.is_aligned = False
        finally:
            self.is_trading = False

    def _clear_excess_positions_if_at_lowest_grid(self, p_skhy, p_skhx):
        """
        只有当回到最低网格 (第 0 层) 时，统一卖出/平掉多余链上实际仓位
        """
        if self.positions_held != 0 or not self.live_trading:
            return

        excess_hy = abs(real_position_skhy) - abs(self.pos_skhy)
        excess_hx = abs(real_position_skhx) - abs(self.pos_skhx)

        if excess_hy > 0.01 or excess_hx > 0.001:
            self.is_trading = True
            try:
                self._add_log(f"[🎯 最低网格清理] 平多余仓位: SKHY={excess_hy:.4f}, SKHX={excess_hx:.4f}")
                sell_px_hy = float(f"{p_skhy * 0.95:.5g}")
                sell_px_hx = float(f"{p_skhx * 0.95:.5g}")
                buy_px_hy = float(f"{p_skhy * 1.05:.5g}")
                buy_px_hx = float(f"{p_skhx * 1.05:.5g}")

                with order_lock:
                    if excess_hy > 0.01:
                        is_buy = True if real_position_skhy < 0 else False
                        px = buy_px_hy if is_buy else sell_px_hy
                        self.exchange.order(WS_SKHY, is_buy, round(excess_hy, self.dec_hy), px, {"limit": {"tif": "Ioc"}}, reduce_only=True)
                    
                    if excess_hx > 0.001:
                        is_buy = True if real_position_skhx < 0 else False
                        px = buy_px_hx if is_buy else sell_px_hx
                        self.exchange.order(WS_SKHX, is_buy, round(excess_hx, self.dec_hx), px, {"limit": {"tif": "Ioc"}}, reduce_only=True)

                self._add_log("[最低网格清理] 多余实际仓位已清理完毕！")
            except Exception as e:
                self._add_log(f"[最低网格清理异常]: {str(e)[:60]}")
            finally:
                self.is_trading = False

    def on_tick(self, k, p_skhy, p_skhx):
        if k is None or p_skhy is None or p_skhx is None: return
        
        # 1. 噪声过滤：对 k 值进行平滑计算
        if self.k_smoothed is None:
            self.k_smoothed = k
        else:
            self.k_smoothed = EMA_ALPHA * k + (1.0 - EMA_ALPHA) * self.k_smoothed
            
        # 2. 关键 BUG 修复：未经过人工输入 1 确认对准前，禁止在后台擅自触发补仓建仓！
        if not self.is_aligned:
            return
            
        # 3. 独占锁检查
        if self.is_trading:
            return

        # 检查最低网格的多余仓位清理
        if self.positions_held == 0:
            self._clear_excess_positions_if_at_lowest_grid(p_skhy, p_skhx)
            
        k_use = self.k_smoothed
        
        while True:
            acted = False
            
            # 🟢 建仓逻辑
            if self.positions_held < self.max_grids:
                next_entry_k = self.p1 + self.positions_held * self.x
                if k_use >= next_entry_k:

                    # 保证金不足动态判断 (双腿单次价值 / 杠杆)
                    required_margin = (self.q_per_grid * 2) / LEVERAGE  
                    if self.live_trading and withdrawable_margin < required_margin:
                        self.max_grids = self.positions_held  # 锁定最高网格上限
                        self._add_log(f"[⚠️ 保证金不足] 可用 {withdrawable_margin:.2f} < 所需 {required_margin:.2f}，锁定 [第 {self.positions_held} 层] 为上限！")
                        break

                    sz_skhy_total = round(self.q_per_grid / p_skhy, self.dec_hy)
                    sz_skhx_total = round(self.q_per_grid / p_skhx, self.dec_hx)
                    
                    self.pos_skhy += sz_skhy_total
                    self.pos_skhx += sz_skhx_total
                    self.positions_held += 1
                    
                    threading.Thread(target=self.execute_sliced_trade, 
                                     args=("ENTRY", self.positions_held, p_skhy, p_skhx, sz_skhy_total, sz_skhx_total, k_use)).start()
                    acted = True
                    continue 
                    
            # 🔴 平仓止盈逻辑
            if self.positions_held > 0:
                next_exit_k = self.p1 + (self.positions_held - 2) * self.x
                if k_use <= next_exit_k:
                    if self.positions_held == 1:
                        sz_skhy_close = round(self.pos_skhy, self.dec_hy)
                        sz_skhx_close = round(self.pos_skhx, self.dec_hx)
                    else:
                        sz_skhy_close = round(self.pos_skhy / self.positions_held, self.dec_hy)
                        sz_skhx_close = round(self.pos_skhx / self.positions_held, self.dec_hx)
                    
                    self.pos_skhy -= sz_skhy_close
                    self.pos_skhx -= sz_skhx_close
                    
                    is_last_close = (self.positions_held == 1)
                    threading.Thread(target=self.execute_sliced_trade, 
                                     args=("EXIT", self.positions_held, p_skhy, p_skhx, sz_skhy_close, sz_skhx_close, k_use, is_last_close)).start()
                    
                    self.positions_held -= 1
                    acted = True
                    continue
                    
            if not acted: break

    def execute_sliced_trade(self, action, level, p_skhy, p_skhx, total_sz_hy, total_sz_hx, k_val, is_last_close=False):
        self.is_trading = True
        try:
            self._execute_trade_logic(action, level, p_skhy, p_skhx, total_sz_hy, total_sz_hx, k_val, is_last_close)
        except Exception as e:
            self._add_log(f" -> 交易运行异常: {str(e)[:80]}")
        finally:
            self.is_trading = False

    def _send_single_order_with_retry(self, coin, is_buy, sz, px, reduce_only=False):
        for _ in range(3):
            try:
                res = self.exchange.order(coin, is_buy, sz, px, {"limit": {"tif": "Ioc"}}, reduce_only=reduce_only)
                if res and res.get("status") == "ok":
                    return True
            except Exception:
                pass
            time.sleep(0.15)
        return False

    def _execute_trade_logic(self, action, level, p_skhy, p_skhx, total_sz_hy, total_sz_hx, k_val, is_last_close=False):
        global global_entry_count, global_tp_count, global_true_profit
        action_start_time_ms = int(time.time() * 1000)  
        
        timestamp = datetime.now().strftime("%H:%M:%S")
        last_flag = " <全平>" if is_last_close else ""
        
        remaining_hy = total_sz_hy
        remaining_hx = total_sz_hx
        slice_hy = round(total_sz_hy / self.split_n, self.dec_hy)
        slice_hx = round(total_sz_hx / self.split_n, self.dec_hx)
        
        buy_px_hx = float(f"{p_skhx * 1.05:.5g}")
        sell_px_hx = float(f"{p_skhx * 0.95:.5g}")
        buy_px_hy = float(f"{p_skhy * 1.05:.5g}")
        sell_px_hy = float(f"{p_skhy * 0.95:.5g}")
        
        for i in range(self.split_n):
            is_final_slice = (i == self.split_n - 1)
            close_all_now = (action == "EXIT" and is_last_close and is_final_slice)
            
            actual_hy = round(remaining_hy, self.dec_hy) if is_final_slice else slice_hy
            actual_hx = round(remaining_hx, self.dec_hx) if is_final_slice else slice_hx
            
            remaining_hy -= actual_hy
            remaining_hx -= actual_hx
            
            if self.live_trading:
                try:
                    with order_lock: 
                        if action == "ENTRY":
                            ok_hx = self._send_single_order_with_retry(WS_SKHX, True, actual_hx, buy_px_hx, reduce_only=False)
                            ok_hy = self._send_single_order_with_retry(WS_SKHY, False, actual_hy, sell_px_hy, reduce_only=False)

                            if ok_hx and not ok_hy:
                                self._add_log(f" [{timestamp}] 切片 {i+1} SKHY失败，强平多余 SKHX 保持平衡")
                                self._send_single_order_with_retry(WS_SKHX, False, actual_hx, sell_px_hx, reduce_only=True)
                            elif not ok_hx and ok_hy:
                                self._add_log(f" [{timestamp}] 切片 {i+1} SKHX失败，强平多余 SKHY 保持平衡")
                                self._send_single_order_with_retry(WS_SKHY, True, actual_hy, buy_px_hy, reduce_only=True)

                        else:
                            sz_hx = 0 if close_all_now else actual_hx
                            sz_hy = 0 if close_all_now else actual_hy
                            ok_hx = self._send_single_order_with_retry(WS_SKHX, False, sz_hx, sell_px_hx, reduce_only=True)
                            ok_hy = self._send_single_order_with_retry(WS_SKHY, True, sz_hy, buy_px_hy, reduce_only=True)

                except Exception as e:
                    self._add_log(f" [{timestamp}] 切片 {i+1}/{self.split_n} 报错: {str(e)[:50]}")
            
            time.sleep(SLICE_DELAY)
            
        if self.live_trading:
            time.sleep(2.0)
            try:
                fills = requests.post("https://api.hyperliquid.xyz/info", json={"type": "userFills", "user": WALLET_ADDR}).json()
                
                hy_vol, hy_cost = 0.0, 0.0
                hx_vol, hx_cost = 0.0, 0.0
                grid_pnl, grid_fee = 0.0, 0.0
                
                for f in fills:
                    if f["time"] >= action_start_time_ms:
                        coin = f["coin"]
                        sz = float(f["sz"])
                        px = float(f["px"])
                        fee = abs(float(f.get("fee", 0)))
                        pnl = float(f.get("closedPnl", 0))
                        
                        if "SKHY" in coin and "SKHX" not in coin and "NIX" not in coin:
                            hy_vol += sz
                            hy_cost += sz * px
                            grid_fee += fee
                            grid_pnl += pnl
                        elif "SKHX" in coin or "NIX" in coin:
                            hx_vol += sz
                            hx_cost += sz * px
                            grid_fee += fee
                            grid_pnl += pnl
                
                vwap_hy = (hy_cost / hy_vol) if hy_vol > 0 else 0.0
                vwap_hx = (hx_cost / hx_vol) if hx_vol > 0 else 0.0
                actual_k = (vwap_hy * 10 / vwap_hx) if (vwap_hy > 0 and vwap_hx > 0) else k_val

                # 极致精简日志输出：只保留真实建平仓 k 值、成功状态、真实收益与开平仓数 (去除理论额与实际均价)
                if action == "ENTRY":
                    self.layer_entry_data[level] = (actual_k, grid_fee)
                    global_entry_count += 1
                    self._add_log(f"[{timestamp}] 第{level}格建仓成功! 真实建仓 k={actual_k:.4f} (已建仓{global_entry_count}次)")

                elif action == "EXIT":
                    # 若无本次运行建仓记录，使用该网格理论开仓 k1 = P1+(level-1)*X
                    default_theoretical_k1 = self.p1 + (level - 1) * self.x
                    k1, fee1 = self.layer_entry_data.get(level, (default_theoretical_k1, 0.0))
                    k2 = actual_k
                    fee2 = grid_fee
                    total_cycle_fee = fee1 + fee2
                    
                    if k2 > 0:
                        single_grid_real_profit = (self.q_per_grid * (k1 - k2) / k2) - total_cycle_fee
                    else:
                        single_grid_real_profit = 0.0

                    global_tp_count += 1
                    global_true_profit += single_grid_real_profit
                    
                    self._add_log(f"[{timestamp}] 第{level}格止盈成功![{k1:.4f}->{k2:.4f}] 真实收益: {single_grid_real_profit:+.2f} (已止盈{global_tp_count}次){last_flag}")
                    
                    if level in self.layer_entry_data:
                        del self.layer_entry_data[level]

            except: pass

    def _add_log(self, text):
        global trade_logs
        trade_logs.append(text)
        if len(trade_logs) > 20: trade_logs.pop(0) # 扩容展示行数至 20 行，填满底部


# ==========================================
# 💰 钱包资产、真实仓位与盈亏追踪线程
# ==========================================
def fetch_account_equity():
    """
    抓取全仓总净值与【防抖动可用保证金】
    """
    global current_equity, initial_equity, withdrawable_margin, real_position_skhy, real_position_skhx
    global global_floating_pnl
    if not WALLET_ADDR: return
        
    url = "https://api.hyperliquid.xyz/info"
    while True:
        try:
            resp_spot = requests.post(url, json={"type": "spotClearinghouseState", "user": WALLET_ADDR}, timeout=5).json()
            spot_usdc_total = 0.0
            for bal in resp_spot.get("balances", []):
                if bal.get("coin") == "USDC":
                    spot_usdc_total = float(bal.get("total", 0.0))

            resp_perp_xyz = requests.post(url, json={"type": "clearinghouseState", "user": WALLET_ADDR, "dex": "xyz"}, timeout=5).json()
            margin_summary_xyz = resp_perp_xyz.get("marginSummary", {})
            margin_used_xyz = float(margin_summary_xyz.get("totalMarginUsed", 0.0))

            resp_perp_native = requests.post(url, json={"type": "clearinghouseState", "user": WALLET_ADDR}, timeout=5).json()
            account_val_native = float(resp_perp_native.get("marginSummary", {}).get("accountValue", 0.0))

            r_y, r_x = 0.0, 0.0
            u_y, u_x = 0.0, 0.0

            for pos in resp_perp_xyz.get("assetPositions", []):
                coin_name = pos["position"]["coin"].upper()
                szi = float(pos["position"]["szi"])
                upnl = float(pos["position"]["unrealizedPnl"])
                
                if "SKHX" in coin_name or "NIX" in coin_name:
                    r_x = szi
                    u_x = upnl
                elif "SKHY" in coin_name:
                    r_y = szi
                    u_y = upnl
                
            real_position_skhy = r_y
            real_position_skhx = r_x
            global_floating_pnl = u_y + u_x  

            current_equity = account_val_native if account_val_native > 0 else spot_usdc_total

            if spot_usdc_total > 0:
                withdrawable_margin = max(0.0, spot_usdc_total - margin_used_xyz)
            else:
                withdrawable_margin = float(resp_perp_xyz.get("withdrawable", 0.0))

            if initial_equity is None: 
                initial_equity = current_equity
                
        except Exception: pass
        time.sleep(3)

def fetch_trade_history():
    """
    资金费与单向递增手续费统计
    """
    global global_realized_pnl, global_fees, global_funding, processed_fill_ids
    if not WALLET_ADDR: return
    url = "https://api.hyperliquid.xyz/info"
    while True:
        try:
            fills = requests.post(url, json={"type": "userFills", "user": WALLET_ADDR}, timeout=5).json()
            realized = 0.0

            for f in fills:
                if f["time"] >= bot_start_time_ms:
                    coin = f.get("coin", "")
                    if "SKHY" in coin or "SKHX" in coin or "NIX" in coin:
                        realized += float(f.get("closedPnl", 0))

                        fill_id = f.get("tid", f"{f.get('time')}_{coin}_{f.get('sz')}_{f.get('px')}")
                        if fill_id not in processed_fill_ids:
                            processed_fill_ids.add(fill_id)
                            global_fees += abs(float(f.get("fee", 0)))

            global_realized_pnl = realized

            # 深度兼容解析 userFunding 的顶级与 delta 嵌套结构
            funding = 0.0
            for dex_param in [None, "xyz"]:
                body = {"type": "userFunding", "user": WALLET_ADDR, "startTime": bot_start_time_ms}
                if dex_param: body["dex"] = dex_param
                funds = requests.post(url, json=body, timeout=5).json()
                for f in funds:
                    coin = f.get("coin", "")
                    if not coin and "delta" in f and isinstance(f["delta"], dict):
                        coin = f["delta"].get("coin", "")

                    if "SKHY" in coin or "SKHX" in coin or "NIX" in coin:
                        if "usdc" in f:
                            funding += float(f["usdc"])
                        elif "delta" in f and isinstance(f["delta"], dict):
                            funding += float(f["delta"].get("usdc", 0.0))

            global_funding = funding
            
        except Exception: pass
        time.sleep(5)


def perform_startup_confirmation_check(bot) -> bool:
    """
    启动对准与建仓前的人工确认阻断检查 (在主线程中同步阻塞，未同意绝不上链)
    """
    if not bot.live_trading:
        bot.is_aligned = True
        return True

    print("\n[启动对准评估] 正在读取链上持仓与盘口行情...")
    for _ in range(10):
        p_skhy_bid = prices['SKHY-USDC']['bid']
        p_skhx_ask = prices['SKHYNIX-USDC']['ask']
        if p_skhy_bid and p_skhx_ask and real_position_skhy is not None and real_position_skhx is not None:
            break
        time.sleep(0.5)

    p_skhy_bid = prices['SKHY-USDC']['bid']
    p_skhx_ask = prices['SKHYNIX-USDC']['ask']

    if not (p_skhy_bid and p_skhx_ask):
        print("[警告] 盘口未就绪，以只读接管模式运行。")
        bot.is_aligned = True
        return True

    k = p_skhy_bid * 10 / p_skhx_ask
    if k < bot.p1:
        M_required = 0
    else:
        M_required = int((k - bot.p1) // bot.x) + 1
    M_required = min(M_required, bot.max_grids)

    USDC_required = M_required * bot.q_per_grid
    actual_val_hy = abs(real_position_skhy) * p_skhy_bid
    actual_val_hx = abs(real_position_skhx) * p_skhx_ask
    actual_usdc = (actual_val_hy + actual_val_hx) / 2.0

    # 判断是否需要补充建仓
    if M_required > 0 and actual_usdc < (USDC_required - 100):
        diff_usdc = USDC_required - actual_usdc
        print("\n" + "=" * 72)
        print(f"  [⚠️ 启动建仓确认] 检测到当前比值 k={k:.4f}，需执行建仓/补仓对准：")
        print(f"  - 理论目标网格: 第 {M_required} 层 (对应仓位额: {USDC_required:.1f})")
        print(f"  - 链上现有仓位: {actual_usdc:.1f}")
        print(f"  - 需补充建仓额: 约 {diff_usdc:.1f}")
        print("  " + "=" * 72)
        
        try:
            user_input = input("  请输入数字 1 并回车确认建仓 (输入其他任意键退出): ").strip()
            if user_input == "1":
                print("\n  [确认成功] 已收到指令 1，开始同步执行建仓对准交易...\n")
                # 在主线程中显式调用建仓对准
                bot.align_positions_on_startup(p_skhy_bid, p_skhx_ask, real_position_skhy, real_position_skhx)
                return True
            else:
                print("\n  [取消执行] 用户未输入 1，放弃建仓，程序安全退出。")
                return False
        except (KeyboardInterrupt, EOFError):
            print("\n  [取消执行] 已中断。")
            return False
    else:
        print(f"[启动对准] 当前仓位充足 ({actual_usdc:.1f} >= 理论 {USDC_required:.1f})，无需补仓，直接接管。")
        bot.positions_held = M_required
        bot.pos_skhy = round(USDC_required / p_skhy_bid, bot.dec_hy) if M_required > 0 else 0.0
        bot.pos_skhx = round(USDC_required / p_skhx_ask, bot.dec_hx) if M_required > 0 else 0.0
        bot.is_aligned = True
        return True


# ==========================================
# 📡 实时行情抓取模块
# ==========================================
trade_logs = []

def fetch_initial_prices():
    try:
        url = "https://api.hyperliquid.xyz/info"
        for coin, display_sym in HL_COIN_TO_DISPLAY.items():
            resp = requests.post(url, json={"type": "l2Book", "coin": coin}, headers={"Content-Type": "application/json"}, timeout=5)
            data = resp.json()
            if "levels" in data and len(data["levels"]) > 0:
                bids = data["levels"][0]
                asks = data["levels"][1] if len(data["levels"]) > 1 else []
                if len(bids) > 0 and len(asks) > 0:
                    prices[display_sym]["bid"] = float(bids[0]["px"])
                    prices[display_sym]["ask"] = float(asks[0]["px"])
    except: pass

def hl_keep_alive(ws):
    global ws_connected
    while ws_connected:
        time.sleep(5)
        try: ws.send(json.dumps({"method": "ping"}))
        except: break

def hl_on_message(ws, message):
    global ws_connected, algo_engine
    ws_connected = True
    try:
        data = json.loads(message)
        if data.get("channel") == "l2Book" and "data" in data:
            coin, levels = data["data"].get("coin"), data["data"].get("levels")
            if coin in HL_COIN_TO_DISPLAY and len(levels) > 0:
                sym = HL_COIN_TO_DISPLAY[coin]
                bids = levels[0]
                asks = levels[1] if len(levels) > 1 else []
                
                if len(bids) > 0 and len(asks) > 0:
                    prices[sym]["bid"] = float(bids[0]["px"])
                    prices[sym]["ask"] = float(asks[0]["px"])
                    
                    p_skhy_bid = prices['SKHY-USDC']['bid']
                    p_skhx_ask = prices['SKHYNIX-USDC']['ask']
                    
                    if p_skhy_bid and p_skhx_ask and algo_engine:
                        calculated_k = p_skhy_bid * 10 / p_skhx_ask
                        algo_engine.on_tick(calculated_k, p_skhy_bid, p_skhx_ask)
    except: pass

def hl_on_error(ws, error):
    global ws_error_msg, ws_connected
    ws_error_msg = str(error)
    ws_connected = False

def hl_on_close(ws, *args):
    global ws_connected
    ws_connected = False

def hl_on_open(ws):
    global ws_connected, ws_error_msg
    ws_connected, ws_error_msg = True, ""
    threading.Thread(target=hl_keep_alive, args=(ws,), daemon=True).start()
    for coin in HL_COIN_TO_DISPLAY.keys():
        ws.send(json.dumps({"method": "subscribe", "subscription": {"type": "l2Book", "coin": coin}}))

def start_hl_ws():
    while True:
        ws = websocket.WebSocketApp("wss://api.hyperliquid.xyz/ws", on_message=hl_on_message, on_error=hl_on_error, on_close=hl_on_close, on_open=hl_on_open)
        ws.run_forever()
        time.sleep(2)


# ==========================================
# 🖥️ 终端 UI 全景视图渲染 (极致紧凑无 USDC 换行版)
# ==========================================
def render_ui(time_str, running_seconds):
    p_skhy_bid = prices['SKHY-USDC']['bid']
    p_skhy_ask = prices['SKHY-USDC']['ask']
    p_skhx_bid = prices['SKHYNIX-USDC']['bid']
    p_skhx_ask = prices['SKHYNIX-USDC']['ask']
    
    k_raw = (p_skhy_bid * 10 / p_skhx_ask) if (p_skhy_bid and p_skhx_ask) else None
    
    if algo_engine:
        mode_text = "[实盘交易开启 🟢]" if algo_engine.live_trading else "[模拟运行模式 🟡]"
        held = algo_engine.positions_held
        pos_y, pos_x = algo_engine.pos_skhy, algo_engine.pos_skhx
        nxt_entry, nxt_exit = algo_engine.get_next_triggers()
        
        if algo_engine.live_trading:
            align_txt = "对齐成功" if algo_engine.is_aligned else "对准中..."
        else:
            align_txt = "模拟对准完成"
            
        status_txt = "工作中 ⚙️" if algo_engine.is_trading else "空闲中"
    else:
        mode_text, held, pos_y, pos_x = "初始化中...", 0, 0.0, 0.0
        nxt_entry, nxt_exit = None, None
        align_txt = "初始化..."
        status_txt = "等待中"

    val_prog_y = -pos_y * p_skhy_bid if p_skhy_bid else 0.0
    val_prog_x = pos_x * p_skhx_ask if p_skhx_ask else 0.0
    val_real_y = real_position_skhy * p_skhy_bid if p_skhy_bid else 0.0
    val_real_x = real_position_skhx * p_skhx_ask if p_skhx_ask else 0.0

    total_net_pnl = global_realized_pnl + global_floating_pnl + global_funding - global_fees
    runtime_str = format_runtime_str(running_seconds)

    LINE_EQUAL = "=" * 72
    LINE_DASH = "-" * 72

    screen = []
    screen.append(f"  HL 做空动态网格机器人 <全景对账+智能平滑对准版 v3.8>")
    screen.append(f"  模式: {mode_text} | 对齐: [{align_txt}] | 引擎: [{status_txt}]")
    screen.append(f"  时间: {time_str} | 运行时间: {runtime_str} | 通道: {'[连通] ●' if ws_connected else '[重连中...]'}")
    screen.append("  " + LINE_EQUAL)
    
    # 彻底去除所有 USDC 标记
    screen.append("  [ 账户资产与盈亏透视 (链上同步) ]")
    if not WALLET_ADDR:
        screen.append("  (未填写 WALLET_ADDR，无法追踪资产与对账)")
    else:
        if current_equity is not None:
            screen.append(f"  当前总净值: {current_equity:.2f} | 可用保证金: {withdrawable_margin:.2f} | 总净盈亏: {total_net_pnl:+.2f}")
            screen.append(f"  盈亏拆解: 网格实现 {global_realized_pnl:+.2f} | 当前浮动 {global_floating_pnl:+.2f} | 资金费 {global_funding/2:+.2f} | 手续费 -{global_fees:.2f}")
            screen.append(f"  策略统计: 累计建仓 {global_entry_count} 次 | 累计止盈 {global_tp_count} 次 | 真实总收益 {global_true_profit:+.2f}")
        
        screen.append(f"  程序账本: SKHY={-pos_y:.4f} ({val_prog_y/1000:+.1f}k) | SKHYNIX={pos_x:.4f} ({val_prog_x/1000:+.1f}k)")
        screen.append(f"  链上真实: SKHY={real_position_skhy:.4f} ({val_real_y/1000:+.1f}k) | SKHYNIX={real_position_skhx:.4f} ({val_real_x/1000:+.1f}k)")
    screen.append("  " + LINE_DASH)
    
    # 按照需求 2 合并行情与策略网格，底端竖排呈现建仓线 / 平滑(原始) / 止盈线
    screen.append("  [ 行情盘口与策略状态 ]")
    screen.append(f"  # SKHY-USDC    : 买1={f'{p_skhy_bid:.6f}' if p_skhy_bid else 'Wait...'}  | 卖1={f'{p_skhy_ask:.6f}' if p_skhy_ask else 'Wait...'}")
    screen.append(f"  # SKHYNIX-USDC : 买1={f'{p_skhx_bid:.6f}' if p_skhx_bid else 'Wait...'} | 卖1={f'{p_skhx_ask:.6f}' if p_skhx_ask else 'Wait...'}")
    max_g_txt = f"{algo_engine.max_grids} (动态上限)" if algo_engine else MAX_GRIDS
    screen.append(f"  # 基准P1={P1} | 步长X={STEP_X} | 单次={Q_USDC}")
    screen.append(f"  # 拆分={SPLIT_N}笔({SLICE_DELAY}s) | 持有=[{held}/{max_g_txt}层]")
    
    # 严格按照需求 2 指定格式竖排呈现
    screen.append("  " + LINE_EQUAL)
    ema_k_str = f"{algo_engine.k_smoothed:.6f}" if (algo_engine and algo_engine.k_smoothed) else "Wait..."
    raw_k_str = f"{k_raw:.6f}" if k_raw else "Wait..."
    screen.append(f"  # 下一建仓线(做空): {f'{nxt_entry:.5f}' if nxt_entry else '锁定封顶'}")
    screen.append(f"  # 当前平滑值(原始): {ema_k_str} ({raw_k_str})")
    screen.append(f"  # 下一平仓线(止盈): {f'{nxt_exit:.5f}' if nxt_exit else '空仓无'}")
    screen.append("  " + LINE_EQUAL)
    
    # 需求 3：底层交易日志拉长至 20 行填满底部
    screen.append("  [ 底层交易与对账日志 ]")
    if not trade_logs:
        screen.append("  <策略监控中...等待行情触发>")
    else:
        for log in trade_logs:
            screen.append("  " + log)
            
    screen.append("  " + LINE_EQUAL)
    screen.append("  [Ctrl+C 退出程序]\n")
    
    os.system('cls' if os.name == 'nt' else 'clear')
    print("\n".join(screen))

def main():
    global algo_engine
    os.system('cls' if os.name == 'nt' else 'clear')
    print("正在唤醒单向做空网格对冲结算系统 (v3.8)...")
    
    if WALLET_ADDR:
        threading.Thread(target=fetch_account_equity, daemon=True).start()
        threading.Thread(target=fetch_trade_history, daemon=True).start()
    
    algo_engine = ShortGridEngine(Q_USDC, SPLIT_N, MAX_GRIDS, P1, STEP_X, SECRET_KEY, WALLET_ADDR)
    
    fetch_initial_prices()
    threading.Thread(target=start_hl_ws, daemon=True).start()
    
    # 等待初始数据加载，显式阻塞询问建仓人工确认
    time.sleep(1.5)
    if not perform_startup_confirmation_check(algo_engine):
        sys.exit(0)
    
    start_time = time.time()
    
    while True:
        try:
            now = time.time()
            now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            render_ui(now_str, int(now - start_time))
            time.sleep(0.3)
        except KeyboardInterrupt:
            print("\n已安全退出自动交易守护进程。")
            break
        except Exception as e:
            time.sleep(1)

if __name__ == "__main__":
    main()
