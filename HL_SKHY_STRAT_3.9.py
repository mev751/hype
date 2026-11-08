#!/usr/bin/env python3
"""
Hyperliquid SKHY/SKHX 相对价值做空网格机器人 v3.9

v3.9 重点改造：
1. 所有交易通过单一工作线程串行执行，消除 on_tick 多线程竞态。
2. 内部仓位只在下单成功并完成成交回报后更新。
3. 修复最后一层平仓数量被设成 0 的严重 BUG。
4. 严格解析 Hyperliquid 嵌套订单状态，并使用 cloid 支持超时后的幂等查询。
5. 成对下单失败时执行定向回滚，并立即读取链上仓位对账。
6. 无行情/行情过期时禁止交易，不再绕过人工确认。
7. 启动接管校验两腿方向、对冲比例和链上仓位。
8. 修正资金费重复统计和 UI 显示口径。
9. 增加可配置的浮动亏损紧急停止与链上不一致停机保护。

注意：本程序涉及真实资金和杠杆交易。请先使用只读地址或模拟模式验证，
并自行确认 Hyperliquid SDK 版本、账户模式、杠杆设置和风险预算。
"""

import json
import logging
import os
import sys
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests

# --- Hyperliquid 交易 SDK ---
try:
    from eth_account import Account
    from hyperliquid.exchange import Exchange
    from hyperliquid.utils import constants
    from hyperliquid.utils.types import Cloid
except ImportError:
    print("缺少 Hyperliquid SDK，请执行: pip install hyperliquid-python-sdk eth_account websocket-client requests")
    sys.exit(1)

import websocket


# ==========================================
# ⚙️ 交易策略与 API 参数配置区
# ==========================================

Q_USDC = 1000                 # 每层每条腿的目标名义价值 (USDC)
SPLIT_N = 1                   # 单次网格订单切分次数，必须 >= 1
MAX_GRIDS = 10                # 最大建仓层数
P1 = 1.4                      # 建仓触发基准线
STEP_X = 0.003                # 网格步长
SLICE_DELAY = 0.75            # 切片间隔 (秒)
LEVERAGE = 10                 # 期望杠杆；实盘启动时会尝试同步到交易所
EMA_ALPHA = 0.25              # K 值 EMA 平滑系数

# 私钥不要写进源码。建议通过环境变量注入：
#   export HL_SECRET_KEY='0x...'
# WALLET_ADDR 可选；不填时会从 SECRET_KEY 自动推导。
SECRET_KEY = os.getenv("HL_SECRET_KEY", "").strip()
WALLET_ADDR = os.getenv("HL_WALLET_ADDR", "").strip()

if SECRET_KEY and not WALLET_ADDR:
    try:
        WALLET_ADDR = Account.from_key(SECRET_KEY).address
    except Exception as exc:
        print(f"无法从 HL_SECRET_KEY 推导钱包地址: {type(exc).__name__}: {exc}")
        sys.exit(1)

# 行情与执行保护参数
PRICE_MAX_AGE_SECONDS = 5.0    # 任一腿盘口超过该秒数即禁止交易
PAIR_SLIPPAGE_PCT = 0.02       # IOC 限价相对盘口的最大滑点
ORDER_RETRIES = 3
ORDER_RETRY_SLEEP = 0.25
FILL_WAIT_SECONDS = 1.0
HEDGE_IMBALANCE_TOLERANCE = 0.15   # 两腿名义价值最大允许偏差 15%
MARGIN_BUFFER_MULTIPLIER = 1.5     # 新增一层前要求的最低保证金缓冲
AUTO_FLATTEN_ON_ERROR = True       # 成对下单失败且回滚失败时，尝试清空两腿
AUTO_FLATTEN_ON_RECONCILE_ERROR = False  # 链上仓位异常时默认只停机，避免误平手工仓位
FLATTEN_ON_EXIT = False            # Ctrl+C 时是否自动全平；默认关闭
STOP_LOSS_USDC = float(os.getenv("HL_STOP_LOSS_USDC", "0") or "0")  # 0 表示关闭
ENFORCE_EXCHANGE_LEVERAGE = True   # 启动时尝试把两只标的设置为 LEVERAGE 倍全仓
MAX_ACTIONS_PER_TICK = 20           # 单次行情最多串行处理的网格动作数

WS_SKHY = "xyz:SKHY"
WS_SKHX = "xyz:SKHX"

HL_COIN_TO_DISPLAY = {
    WS_SKHY: "SKHY-USDC",
    WS_SKHX: "SKHYNIX-USDC",
}

# bid/ask/ts 由 WebSocket 或初始 REST 行情更新
prices: Dict[str, Dict[str, Optional[float]]] = {
    "SKHY-USDC": {"bid": None, "ask": None, "ts": 0.0},
    "SKHYNIX-USDC": {"bid": None, "ask": None, "ts": 0.0},
}

ws_connected = False
ws_error_msg = ""
bot_start_time_ms = int(time.time() * 1000)

initial_equity: Optional[float] = None
current_equity: Optional[float] = None
withdrawable_margin = 0.0

real_position_skhy = 0.0
real_position_skhx = 0.0
global_floating_pnl = 0.0
global_realized_pnl = 0.0
global_fees = 0.0
global_funding = 0.0

global_entry_count = 0
global_tp_count = 0
global_true_profit = 0.0
processed_fill_ids = set()

trade_logs: List[str] = []
algo_engine: Optional["ShortGridEngine"] = None

logger = logging.getLogger("hype_strategy")


def setup_logging() -> None:
    logger.setLevel(logging.INFO)
    logger.propagate = False
    if logger.handlers:
        return
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    log_path = os.getenv("HL_LOG_PATH", "hype_strategy.log")
    try:
        handler = logging.FileHandler(log_path, encoding="utf-8")
    except Exception:
        handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def log_error(context: str, exc: BaseException) -> None:
    message = f"[错误] {context}: {type(exc).__name__}: {exc}"
    logger.error(message)
    _add_global_log(message)


def _add_global_log(text: str) -> None:
    trade_logs.append(text)
    if len(trade_logs) > 20:
        trade_logs.pop(0)


def format_runtime_str(total_seconds: int) -> str:
    days, remainder = divmod(total_seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, secs = divmod(remainder, 60)
    return f"{days}天 {hours:02d}时 {minutes:02d}分 {secs:02d}秒"


def post_info(payload: Dict[str, Any], timeout: float = 10.0) -> Any:
    """调用 Hyperliquid info 接口。所有异常由调用方处理并记录。"""
    response = requests.post(
        "https://api.hyperliquid.xyz/info",
        json=payload,
        headers={"Content-Type": "application/json"},
        timeout=timeout,
    )
    response.raise_for_status()
    return response.json()


def round_size(size: float, decimals: int) -> float:
    return round(float(size), decimals)


def make_limit_price(price: float, is_buy: bool, slippage: float = PAIR_SLIPPAGE_PCT) -> float:
    """生成 IOC 保护限价。Hyperliquid 价格使用最多 5 位有效数字。"""
    factor = (1.0 + slippage) if is_buy else (1.0 - slippage)
    return float(f"{price * factor:.5g}")


def update_price(symbol: str, bid: float, ask: float) -> None:
    prices[symbol]["bid"] = float(bid)
    prices[symbol]["ask"] = float(ask)
    prices[symbol]["ts"] = time.time()


def get_fresh_prices() -> Optional[Tuple[float, float]]:
    now = time.time()
    hy = prices["SKHY-USDC"]
    hx = prices["SKHYNIX-USDC"]
    if not hy.get("bid") or not hx.get("ask"):
        return None
    if now - float(hy.get("ts") or 0.0) > PRICE_MAX_AGE_SECONDS:
        return None
    if now - float(hx.get("ts") or 0.0) > PRICE_MAX_AGE_SECONDS:
        return None
    return float(hy["bid"]), float(hx["ask"])


def calculate_k(p_skhy_bid: float, p_skhx_ask: float) -> float:
    return p_skhy_bid * 10.0 / p_skhx_ask


def parse_order_response(res: Any) -> Tuple[bool, str]:
    """
    严格解析 Hyperliquid 下单结果。

    顶层 status == 'ok' 并不代表订单成功，真正结果在：
      response.data.statuses[*]
    """
    if not isinstance(res, dict):
        return False, "返回值不是 dict"
    if res.get("status") != "ok":
        return False, f"顶层状态异常: {res.get('status')!r}"

    try:
        statuses = res["response"]["data"]["statuses"]
    except (KeyError, TypeError):
        return False, "缺少 response.data.statuses"

    if not isinstance(statuses, list) or not statuses:
        return False, "statuses 为空"

    for item in statuses:
        if not isinstance(item, dict):
            return False, "status 条目格式异常"
        if item.get("error"):
            return False, f"订单错误: {item['error']}"
        if "filled" in item:
            return True, "filled"
        if "resting" in item:
            return False, "IOC 订单意外进入 resting 状态"
    return False, f"未识别的订单状态: {statuses}"


@dataclass
class OrderOutcome:
    ok: bool
    reason: str
    cloid: Optional[str] = None
    filled_sz: float = 0.0
    avg_px: float = 0.0


@dataclass
class PairResult:
    ok: bool
    reason: str = ""
    requested_hy: float = 0.0
    requested_hx: float = 0.0
    filled_hy: float = 0.0
    filled_hx: float = 0.0
    avg_hy: float = 0.0
    avg_hx: float = 0.0
    fees: float = 0.0
    closed_pnl: float = 0.0
    fills_verified: bool = False
    fill_details: List[Dict[str, Any]] = field(default_factory=list)


# ==========================================
# 🧠 核心交易引擎 (做空网格状态机)
# ==========================================
class ShortGridEngine:
    def __init__(self, q: float, split_n: int, max_grids: int, p1: float, x: float,
                 secret_key: str, wallet_addr: str):
        if split_n < 1:
            raise ValueError("SPLIT_N 必须 >= 1")
        if x <= 0:
            raise ValueError("STEP_X 必须 > 0")

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
        self.layer_entry_data: Dict[int, Dict[str, float]] = {}

        self.live_trading = bool(secret_key and wallet_addr)
        self.exchange = None
        self.is_aligned = False
        self.halted = False
        self.halt_reason = ""
        self.k_smoothed: Optional[float] = None

        self._state_lock = threading.Lock()
        self._pending_lock = threading.Lock()
        self._order_lock = threading.RLock()
        self._flatten_requested = False
        self._flatten_request_prices: Optional[Tuple[float, float]] = None
        self._flatten_request_reason = ""
        self._pending_tick: Optional[Tuple[float, float, float]] = None
        self._tick_event = threading.Event()
        self._stop_event = threading.Event()
        self._worker: Optional[threading.Thread] = None

        if self.live_trading:
            try:
                self._initialize_exchange(secret_key)
            except Exception as exc:
                log_error("钱包与官方 SDK 初始化失败，已降级为模拟模式", exc)
                self.live_trading = False
                self.exchange = None

    def _initialize_exchange(self, secret_key: str) -> None:
        perp_dexs_resp = post_info({"type": "perpDexs"})
        all_dex_names = [d["name"] for d in perp_dexs_resp if d is not None]

        account = Account.from_key(secret_key)
        self.exchange = Exchange(account, constants.MAINNET_API_URL, perp_dexs=all_dex_names)

        if hasattr(self.exchange, "info"):
            hy_asset = self.exchange.info.coin_to_asset.get(WS_SKHY)
            hx_asset = self.exchange.info.coin_to_asset.get(WS_SKHX)
            if hy_asset is not None:
                self.dec_hy = self.exchange.info.asset_to_sz_decimals.get(hy_asset, 4)
            if hx_asset is not None:
                self.dec_hx = self.exchange.info.asset_to_sz_decimals.get(hx_asset, 4)

        if ENFORCE_EXCHANGE_LEVERAGE:
            for coin in (WS_SKHY, WS_SKHX):
                res = self.exchange.update_leverage(LEVERAGE, coin, True)
                if not isinstance(res, dict) or res.get("status") != "ok":
                    raise RuntimeError(f"设置 {coin} 杠杆失败: {res}")
            logger.info("已将 %s / %s 设置为 %s 倍全仓", WS_SKHY, WS_SKHX, LEVERAGE)

    # ------------------------------
    # 工作线程与行情入口
    # ------------------------------
    def start_worker(self) -> None:
        if self._worker and self._worker.is_alive():
            return
        self._stop_event.clear()
        self._worker = threading.Thread(target=self._worker_loop, name="grid-trade-worker", daemon=True)
        self._worker.start()

    def stop_worker(self) -> None:
        self._stop_event.set()
        self._tick_event.set()
        if self._worker:
            self._worker.join(timeout=3.0)

    def on_tick(self, k: float, p_skhy: float, p_skhx: float) -> None:
        """只更新信号，不直接交易；实际执行全部交给单工作线程。"""
        if k is None or p_skhy is None or p_skhx is None:
            return

        with self._state_lock:
            if self.k_smoothed is None:
                self.k_smoothed = float(k)
            else:
                self.k_smoothed = EMA_ALPHA * float(k) + (1.0 - EMA_ALPHA) * self.k_smoothed
            smoothed = self.k_smoothed
            allowed = self.is_aligned and not self.halted

        if not allowed:
            return

        with self._pending_lock:
            # 只保留最新行情信号，旧信号由工作线程自然丢弃。
            self._pending_tick = (smoothed, float(p_skhy), float(p_skhx))
        self._tick_event.set()

    def _worker_loop(self) -> None:
        while not self._stop_event.is_set():
            if not self._tick_event.wait(timeout=0.5):
                continue
            self._tick_event.clear()
            if self._stop_event.is_set():
                break

            with self._pending_lock:
                tick = self._pending_tick
                self._pending_tick = None

            if tick is not None:
                try:
                    self._process_tick(*tick)
                except Exception as exc:
                    log_error("交易工作线程异常", exc)
                    self._halt(f"交易工作线程异常: {exc}")

            # 紧急平仓请求没有伴随行情 tick 时也必须被执行。
            with self._state_lock:
                flatten_requested = self._flatten_requested
                flatten_prices = self._flatten_request_prices
                flatten_reason = self._flatten_request_reason
                if flatten_requested:
                    self._flatten_requested = False
            if flatten_requested and flatten_prices:
                try:
                    self.flatten_all(flatten_prices[0], flatten_prices[1], reason=flatten_reason)
                except Exception as exc:
                    log_error("保护性平仓失败", exc)

    def _halt(self, reason: str) -> None:
        with self._state_lock:
            if not self.halted:
                self.halted = True
                self.halt_reason = reason
                _add_global_log(f"[⛔ 已停机] {reason}")
                logger.error("交易停机: %s", reason)

    def request_emergency_flatten(self, p_skhy: float, p_skhx: float, reason: str) -> None:
        """由 UI/风控线程发起，真正下单仍由交易工作线程串行执行。"""
        self._halt(reason)
        with self._state_lock:
            self._flatten_requested = True
            self._flatten_request_prices = (p_skhy, p_skhx)
            self._flatten_request_reason = reason
        self._tick_event.set()

    def _process_tick(self, k: float, p_skhy: float, p_skhx: float) -> None:
        global global_entry_count, global_tp_count, global_true_profit
        if self.halted:
            return
        with self._state_lock:
            if not self.is_aligned or self.halted:
                return

        actions = 0
        while actions < MAX_ACTIONS_PER_TICK and not self.halted:
            with self._state_lock:
                held = self.positions_held

            # 🟢 建仓：只在当前 k 仍高于下一触发线时执行一层。
            if held < self.max_grids and k >= self.p1 + held * self.x:
                required_margin = (self.q_per_grid * 2.0 / LEVERAGE) * MARGIN_BUFFER_MULTIPLIER
                if self.live_trading and withdrawable_margin < required_margin:
                    with self._state_lock:
                        self.max_grids = held
                    self._add_log(
                        f"[⚠️ 保证金缓冲不足] 可用 {withdrawable_margin:.2f} "
                        f"< 缓冲要求 {required_margin:.2f}，锁定第 {held} 层为上限"
                    )
                    break

                sz_hy = round_size(self.q_per_grid / p_skhy, self.dec_hy)
                sz_hx = round_size(self.q_per_grid / p_skhx, self.dec_hx)
                if sz_hy <= 0 or sz_hx <= 0:
                    self._halt(f"计算出的下单数量无效: SKHY={sz_hy}, SKHX={sz_hx}")
                    break

                result = self._execute_pair(
                    action="ENTRY",
                    level=held + 1,
                    p_skhy=p_skhy,
                    p_skhx=p_skhx,
                    total_sz_hy=sz_hy,
                    total_sz_hx=sz_hx,
                    k_val=k,
                )
                if not result.ok:
                    self._handle_pair_failure("建仓", result)
                    break

                with self._state_lock:
                    self.pos_skhy += result.filled_hy if result.filled_hy > 0 else sz_hy
                    self.pos_skhx += result.filled_hx if result.filled_hx > 0 else sz_hx
                    self.positions_held += 1
                    level = self.positions_held
                    actual_k = self._vwap_k(result, fallback=k)
                    self.layer_entry_data[level] = {
                        "k": actual_k,
                        "fee": result.fees,
                        "entry_px_hy": result.avg_hy,
                        "entry_px_hx": result.avg_hx,
                    }
                    global_entry_count += 1

                self._add_log(
                    f"[{datetime.now().strftime('%H:%M:%S')}] 第{level}格建仓成功! "
                    f"真实k={self.layer_entry_data[level]['k']:.4f} "
                    f"(累计建仓{global_entry_count}次)"
                )
                actions += 1
                if not self._reconcile_after_action(expect_held=self.positions_held, p_skhy=p_skhy, p_skhx=p_skhx):
                    break
                continue

            # 🔴 止盈：每次只平一层；最后一层必须发送真实数量，绝不发 0。
            if held > 0 and k <= self.p1 + (held - 2) * self.x:
                with self._state_lock:
                    if held == 1:
                        close_hy = round_size(self.pos_skhy, self.dec_hy)
                        close_hx = round_size(self.pos_skhx, self.dec_hx)
                    else:
                        close_hy = round_size(self.pos_skhy / held, self.dec_hy)
                        close_hx = round_size(self.pos_skhx / held, self.dec_hx)

                if close_hy <= 0 or close_hx <= 0:
                    self._halt(f"计算出的平仓数量无效: SKHY={close_hy}, SKHX={close_hx}")
                    break

                result = self._execute_pair(
                    action="EXIT",
                    level=held,
                    p_skhy=p_skhy,
                    p_skhx=p_skhx,
                    total_sz_hy=close_hy,
                    total_sz_hx=close_hx,
                    k_val=k,
                )
                if not result.ok:
                    self._handle_pair_failure("止盈", result)
                    break

                entry = self.layer_entry_data.pop(held, {
                    "k": self.p1 + (held - 1) * self.x,
                    "fee": 0.0,
                    "entry_px_hy": 0.0,
                    "entry_px_hx": 0.0,
                })
                exit_k = self._vwap_k(result, fallback=k)
                cycle_profit = self._cycle_profit(entry, result, exit_k)

                with self._state_lock:
                    self.pos_skhy = max(0.0, self.pos_skhy - (result.filled_hy if result.filled_hy > 0 else close_hy))
                    self.pos_skhx = max(0.0, self.pos_skhx - (result.filled_hx if result.filled_hx > 0 else close_hx))
                    self.positions_held -= 1
                    global_tp_count += 1
                    global_true_profit += cycle_profit

                last_flag = " <全平>" if held == 1 else ""
                self._add_log(
                    f"[{datetime.now().strftime('%H:%M:%S')}] 第{held}格止盈成功! "
                    f"[{entry['k']:.4f}->{exit_k:.4f}] 净收益: {cycle_profit:+.2f} "
                    f"(累计止盈{global_tp_count}次){last_flag}"
                )
                actions += 1
                if not self._reconcile_after_action(expect_held=self.positions_held, p_skhy=p_skhy, p_skhx=p_skhx):
                    break
                if self.positions_held == 0:
                    # 正常应在最后一层全部平掉；若链上仍有残仓，做一次兜底清理。
                    self._cleanup_remaining_positions(p_skhy, p_skhx)
                continue

            break

    def _reconcile_after_action(self, expect_held: int, p_skhy: float, p_skhx: float) -> bool:
        if not self.live_trading:
            return True
        actual = self._sync_chain_positions_with_retry()
        if actual is None:
            self._halt("成交后无法读取链上仓位，不能确认风险状态")
            return False

        pos_hy, pos_hx = actual
        healthy, reason = self.positions_are_healthy(pos_hy, pos_hx, p_skhy, p_skhx)
        if not healthy:
            self._halt(f"链上仓位不一致: {reason}")
            if AUTO_FLATTEN_ON_RECONCILE_ERROR:
                self.flatten_all(p_skhy, p_skhx, reason="仓位不一致兜底")
            return False

        with self._state_lock:
            self.pos_skhy = abs(pos_hy)
            self.pos_skhx = abs(pos_hx)
            derived = self._derive_layer_count(pos_hy, pos_hx, p_skhy, p_skhx)
            if abs(derived - expect_held) > 0.75:
                self._halt(f"链上推导层数 {derived} 与内部层数 {expect_held} 不一致")
                return False
            self.positions_held = int(round(derived))
        return True

    def _cleanup_remaining_positions(self, p_skhy: float, p_skhx: float) -> None:
        if not self.live_trading:
            return
        actual = self._sync_chain_positions_with_retry()
        if actual is None:
            return
        pos_hy, pos_hx = actual
        if abs(pos_hy) <= 10 ** (-self.dec_hy) and abs(pos_hx) <= 10 ** (-self.dec_hx):
            return
        self._add_log("[兜底清理] 网格已归零但链上仍有仓位，执行 reduce-only 清理")
        self.flatten_all(p_skhy, p_skhx, reason="网格归零残仓")

    @staticmethod
    def _signal_strength(value: float, decimals: int) -> bool:
        return abs(value) > 10 ** (-decimals)

    def _estimate_leg_notional(self, pos_hy: float, pos_hx: float,
                               p_skhy: float, p_skhx: float) -> Tuple[float, float]:
        return abs(pos_hy) * p_skhy, abs(pos_hx) * p_skhx

    def _derive_layer_count(self, pos_hy: float, pos_hx: float,
                            p_skhy: float, p_skhx: float) -> float:
        n_hy, n_hx = self._estimate_leg_notional(pos_hy, pos_hx, p_skhy, p_skhx)
        if n_hy <= 0 and n_hx <= 0:
            return 0.0
        return ((n_hy + n_hx) / 2.0) / self.q_per_grid

    def _vwap_k(self, result: PairResult, fallback: float) -> float:
        if result.avg_hy > 0 and result.avg_hx > 0:
            return calculate_k(result.avg_hy, result.avg_hx)
        return fallback

    def _cycle_profit(self, entry: Dict[str, float], result: PairResult, exit_k: float) -> float:
        # 优先使用 Hyperliquid 的 closedPnl 并扣除本周期两腿手续费。
        if result.fills_verified:
            return result.closed_pnl - entry.get("fee", 0.0) - result.fees
        # 成交回报未取到时，仅作保守估算，并明确依赖理论值。
        k1 = entry.get("k", 0.0)
        return (self.q_per_grid * (k1 - exit_k) / exit_k) - entry.get("fee", 0.0) - result.fees

    # ------------------------------
    # 订单执行
    # ------------------------------
    def _place_order_with_retry(self, coin: str, is_buy: bool, sz: float, px: float,
                                reduce_only: bool, purpose: str) -> OrderOutcome:
        if sz <= 0:
            return OrderOutcome(False, f"{purpose}: 数量必须大于 0，实际为 {sz}")

        raw_cloid = f"0x{uuid.uuid4().hex}"
        cloid = Cloid.from_str(raw_cloid)
        last_reason = "未执行"

        for attempt in range(1, ORDER_RETRIES + 1):
            try:
                res = self.exchange.order(
                    coin,
                    is_buy,
                    sz,
                    px,
                    {"limit": {"tif": "Ioc"}},
                    reduce_only=reduce_only,
                    cloid=cloid,
                )
                ok, reason = parse_order_response(res)
                if ok:
                    filled_sz, avg_px = self._extract_filled_info(res, fallback_sz=sz, fallback_px=px)
                    return OrderOutcome(True, reason, raw_cloid, filled_sz, avg_px)
                last_reason = reason

                # 同一 cloid 已存在时，先查询其最终状态，避免重复下单。
                if "cloid" in reason.lower() or "duplicate" in reason.lower():
                    status = self._query_order_status_by_cloid(cloid)
                    if status == "filled":
                        return OrderOutcome(True, f"cloid 查询状态={status}", raw_cloid, sz, px)
                # 业务错误重试通常不会成功，直接返回。
                return OrderOutcome(False, reason, raw_cloid)

            except Exception as exc:
                last_reason = f"{type(exc).__name__}: {exc}"
                # 请求异常可能发生在交易所已接受订单之后；先按 cloid 查询。
                status = self._query_order_status_by_cloid(cloid)
                if status == "filled":
                    return OrderOutcome(True, f"异常后 cloid 查询状态={status}", raw_cloid, sz, px)
                if attempt < ORDER_RETRIES:
                    time.sleep(ORDER_RETRY_SLEEP)

        return OrderOutcome(False, last_reason, raw_cloid)

    def _extract_filled_info(self, res: Dict[str, Any], fallback_sz: float,
                             fallback_px: float) -> Tuple[float, float]:
        try:
            status = res["response"]["data"]["statuses"][0]
            filled = status.get("filled", {})
            sz = float(filled.get("totalSz", fallback_sz))
            px = float(filled.get("avgPx", fallback_px))
            return sz, px
        except (KeyError, TypeError, ValueError):
            return fallback_sz, fallback_px

    def _query_order_status_by_cloid(self, cloid: Cloid) -> Optional[str]:
        if not (self.live_trading and self.exchange):
            return None
        try:
            res = self.exchange.info.query_order_by_cloid(WALLET_ADDR, cloid)
        except Exception as exc:
            logger.warning("按 cloid 查询订单失败: %s", exc)
            return None
        if not isinstance(res, dict):
            return None
        if res.get("status") != "order":
            return None
        try:
            return res["order"].get("status")
        except (KeyError, TypeError, AttributeError):
            return None

    def _execute_pair(self, action: str, level: int, p_skhy: float, p_skhx: float,
                      total_sz_hy: float, total_sz_hx: float, k_val: float) -> PairResult:
        # 所有下单路径共用同一把可重入锁，避免保护性平仓与网格动作并发下单。
        with self._order_lock:
            return self._execute_pair_locked(action, level, p_skhy, p_skhx,
                                             total_sz_hy, total_sz_hx, k_val)

    def _execute_pair_locked(self, action: str, level: int, p_skhy: float, p_skhx: float,
                             total_sz_hy: float, total_sz_hx: float, k_val: float) -> PairResult:
        if action not in ("ENTRY", "EXIT"):
            return PairResult(False, f"未知动作 {action}")

        start_ms = int(time.time() * 1000)
        requested_hy = total_sz_hy
        requested_hx = total_sz_hx
        remaining_hy = total_sz_hy
        remaining_hx = total_sz_hx
        slice_hy = round_size(total_sz_hy / self.split_n, self.dec_hy)
        slice_hx = round_size(total_sz_hx / self.split_n, self.dec_hx)
        all_fills: List[Dict[str, Any]] = []

        for i in range(self.split_n):
            is_last_slice = (i == self.split_n - 1)
            actual_hy = round_size(remaining_hy, self.dec_hy) if is_last_slice else slice_hy
            actual_hx = round_size(remaining_hx, self.dec_hx) if is_last_slice else slice_hx
            remaining_hy = round_size(remaining_hy - actual_hy, self.dec_hy)
            remaining_hx = round_size(remaining_hx - actual_hx, self.dec_hx)

            if actual_hy <= 0 or actual_hx <= 0:
                return PairResult(False, f"切片 {i+1} 数量无效: hy={actual_hy}, hx={actual_hx}")

            if not self.live_trading:
                # 模拟模式按传入盘口视为成交。
                all_fills.extend([
                    {"coin": WS_SKHY, "sz": str(actual_hy), "px": str(p_skhy), "fee": "0", "closedPnl": "0"},
                    {"coin": WS_SKHX, "sz": str(actual_hx), "px": str(p_skhx), "fee": "0", "closedPnl": "0"},
                ])
                if not is_last_slice:
                    time.sleep(SLICE_DELAY)
                continue

            buy_px_hx = make_limit_price(p_skhx, is_buy=True)
            sell_px_hx = make_limit_price(p_skhx, is_buy=False)
            buy_px_hy = make_limit_price(p_skhy, is_buy=True)
            sell_px_hy = make_limit_price(p_skhy, is_buy=False)

            if action == "ENTRY":
                # 先建立多头 SKHX，再建立空头 SKHY。
                first = self._place_order_with_retry(WS_SKHX, True, actual_hx, buy_px_hx, False, "建仓-SKHX")
                if not first.ok:
                    return PairResult(False, f"切片 {i+1} SKHX 建仓失败: {first.reason}")
                second = self._place_order_with_retry(WS_SKHY, False, actual_hy, sell_px_hy, False, "建仓-SKHY")
                if not second.ok:
                    rollback_sz = first.filled_sz if first.filled_sz > 0 else actual_hx
                    rollback = self._place_order_with_retry(
                        WS_SKHX, False, rollback_sz, sell_px_hx, True, "建仓失败回滚-SKHX"
                    )
                    reason = f"切片 {i+1} SKHY 建仓失败: {second.reason}"
                    if rollback.ok:
                        reason += "；SKHX 已回滚"
                    else:
                        reason += f"；SKHX 回滚失败: {rollback.reason}"
                        if AUTO_FLATTEN_ON_ERROR:
                            self._emergency_flatten(p_skhy, p_skhx, "成对建仓失败")
                    return PairResult(False, reason)
            else:
                # 先平多头 SKHX，再平空头 SKHY。最后一层仍发送真实数量。
                first = self._place_order_with_retry(WS_SKHX, False, actual_hx, sell_px_hx, True, "止盈-SKHX")
                if not first.ok:
                    return PairResult(False, f"切片 {i+1} SKHX 平仓失败: {first.reason}")
                second = self._place_order_with_retry(WS_SKHY, True, actual_hy, buy_px_hy, True, "止盈-SKHY")
                if not second.ok:
                    rollback_sz = first.filled_sz if first.filled_sz > 0 else actual_hx
                    rollback = self._place_order_with_retry(
                        WS_SKHX, True, rollback_sz, buy_px_hx, False, "止盈失败回滚-SKHX"
                    )
                    reason = f"切片 {i+1} SKHY 平仓失败: {second.reason}"
                    if rollback.ok:
                        reason += "；SKHX 已重新对冲"
                    else:
                        reason += f"；SKHX 回滚失败: {rollback.reason}"
                        if AUTO_FLATTEN_ON_ERROR:
                            self._emergency_flatten(p_skhy, p_skhx, "成对止盈失败")
                    return PairResult(False, reason)

            if not is_last_slice:
                time.sleep(SLICE_DELAY)

        if not self.live_trading:
            fees = 0.0
            closed_pnl = 0.0
            hy_sz = sum(float(f["sz"]) for f in all_fills if f["coin"] == WS_SKHY)
            hx_sz = sum(float(f["sz"]) for f in all_fills if f["coin"] == WS_SKHX)
            return PairResult(
                True, "", requested_hy, requested_hx, hy_sz, hx_sz,
                p_skhy, p_skhx, fees, closed_pnl, False, all_fills
            )

        time.sleep(FILL_WAIT_SECONDS)
        fills = self._fetch_fills_since(start_ms)
        relevant = [f for f in fills if f.get("coin") in (WS_SKHY, WS_SKHX, "xyz:SKHX", "xyz:SKHY")]
        hy_fills = [f for f in relevant if self._is_hy_coin(f.get("coin", ""))]
        hx_fills = [f for f in relevant if not self._is_hy_coin(f.get("coin", ""))]

        filled_hy = sum(float(f.get("sz", 0)) for f in hy_fills)
        filled_hx = sum(float(f.get("sz", 0)) for f in hx_fills)
        avg_hy = self._fill_vwap(hy_fills, fallback=p_skhy)
        avg_hx = self._fill_vwap(hx_fills, fallback=p_skhx)
        fees = sum(abs(float(f.get("fee", 0))) for f in relevant)
        closed_pnl = sum(float(f.get("closedPnl", 0)) for f in relevant)
        verified = bool(hy_fills and hx_fills)

        return PairResult(
            True, "", requested_hy, requested_hx,
            filled_hy if filled_hy > 0 else requested_hy,
            filled_hx if filled_hx > 0 else requested_hx,
            avg_hy, avg_hx, fees, closed_pnl, verified, relevant
        )

    @staticmethod
    def _is_hy_coin(coin: str) -> bool:
        return "SKHY" in coin and "SKHX" not in coin and "NIX" not in coin

    @staticmethod
    def _fill_vwap(fills: List[Dict[str, Any]], fallback: float) -> float:
        total_sz = 0.0
        total_cost = 0.0
        for f in fills:
            try:
                sz = float(f.get("sz", 0))
                px = float(f.get("px", 0))
            except (TypeError, ValueError):
                continue
            if sz > 0 and px > 0:
                total_sz += sz
                total_cost += sz * px
        return total_cost / total_sz if total_sz > 0 else fallback

    def _fetch_fills_since(self, start_ms: int) -> List[Dict[str, Any]]:
        if not WALLET_ADDR:
            return []
        for attempt in range(3):
            try:
                res = post_info({
                    "type": "userFillsByTime",
                    "user": WALLET_ADDR,
                    "startTime": start_ms,
                    "aggregateByTime": False,
                })
                if isinstance(res, list):
                    return res
                return []
            except Exception as exc:
                logger.warning("获取成交记录失败(%s/3): %s", attempt + 1, exc)
                time.sleep(0.4)
        return []

    def _handle_pair_failure(self, action_name: str, result: PairResult) -> None:
        self._add_log(f"[交易失败] {action_name}: {result.reason[:120]}")
        self._halt(f"{action_name}失败，停止继续交易: {result.reason[:120]}")

    def _emergency_flatten(self, p_skhy: float, p_skhx: float, reason: str) -> None:
        self._add_log(f"[🚨 紧急平仓] {reason}")
        self.flatten_all(p_skhy, p_skhx, reason=reason)

    def flatten_all(self, p_skhy: float, p_skhx: float, reason: str = "手动/保护性平仓") -> bool:
        with self._order_lock:
            return self._flatten_all_locked(p_skhy, p_skhx, reason)

    def _flatten_all_locked(self, p_skhy: float, p_skhx: float,
                            reason: str = "手动/保护性平仓") -> bool:
        if not self.live_trading:
            with self._state_lock:
                self.pos_skhy = 0.0
                self.pos_skhx = 0.0
                self.positions_held = 0
            return True

        actual = self._sync_chain_positions_with_retry()
        if actual is None:
            self._add_log(f"[平仓失败] 无法读取链上仓位: {reason}")
            return False
        pos_hy, pos_hx = actual
        ok_all = True

        if self._signal_strength(pos_hy, self.dec_hy):
            is_buy = pos_hy < 0
            px = make_limit_price(p_skhy, is_buy=is_buy)
            res = self._place_order_with_retry(
                WS_SKHY, is_buy, round_size(abs(pos_hy), self.dec_hy), px, True, "保护性平仓-SKHY"
            )
            ok_all = ok_all and res.ok
        if self._signal_strength(pos_hx, self.dec_hx):
            is_buy = pos_hx > 0
            # 平多头应卖出
            is_buy = not is_buy
            px = make_limit_price(p_skhx, is_buy=is_buy)
            res = self._place_order_with_retry(
                WS_SKHX, is_buy, round_size(abs(pos_hx), self.dec_hx), px, True, "保护性平仓-SKHX"
            )
            ok_all = ok_all and res.ok

        time.sleep(FILL_WAIT_SECONDS)
        actual_after = self._sync_chain_positions_with_retry()
        if actual_after is not None:
            pos_hy2, pos_hx2 = actual_after
            with self._state_lock:
                self.pos_skhy = abs(pos_hy2)
                self.pos_skhx = abs(pos_hx2)
                if abs(pos_hy2) <= 10 ** (-self.dec_hy) and abs(pos_hx2) <= 10 ** (-self.dec_hx):
                    self.positions_held = 0
        self._add_log(f"[保护性平仓完成] reason={reason}, all_orders_ok={ok_all}")
        return ok_all

    # ------------------------------
    # 仓位与启动对准
    # ------------------------------
    def _sync_chain_positions_with_retry(self) -> Optional[Tuple[float, float]]:
        if not WALLET_ADDR:
            return None
        for attempt in range(3):
            try:
                res = post_info({"type": "clearinghouseState", "user": WALLET_ADDR, "dex": "xyz"})
                r_y, r_x = 0.0, 0.0
                for pos in res.get("assetPositions", []):
                    p = pos.get("position", {})
                    coin_name = str(p.get("coin", "")).upper()
                    szi = float(p.get("szi", 0.0))
                    if "SKHX" in coin_name or "NIX" in coin_name:
                        r_x = szi
                    elif "SKHY" in coin_name:
                        r_y = szi
                with self._state_lock:
                    self.pos_skhy = abs(r_y)
                    self.pos_skhx = abs(r_x)
                return r_y, r_x
            except Exception as exc:
                logger.warning("读取链上仓位失败(%s/3): %s", attempt + 1, exc)
                time.sleep(0.4)
        return None

    def positions_are_healthy(self, pos_hy: float, pos_hx: float,
                              p_skhy: float, p_skhx: float) -> Tuple[bool, str]:
        eps_y = 10 ** (-self.dec_hy)
        eps_x = 10 ** (-self.dec_hx)
        has_hy = abs(pos_hy) > eps_y
        has_hx = abs(pos_hx) > eps_x

        if not has_hy and not has_hx:
            return True, "空仓"
        if has_hy != has_hx:
            return False, f"单腿裸露: SKHY={pos_hy}, SKHX={pos_hx}"
        if pos_hy > 0:
            return False, f"SKHY 应为空头，实际 szi={pos_hy}"
        if pos_hx < 0:
            return False, f"SKHX 应为多头，实际 szi={pos_hx}"

        n_hy, n_hx = self._estimate_leg_notional(pos_hy, pos_hx, p_skhy, p_skhx)
        base = max(n_hy, n_hx)
        if base <= 0:
            return False, "两腿名义价值无效"
        imbalance = abs(n_hy - n_hx) / base
        if imbalance > HEDGE_IMBALANCE_TOLERANCE:
            return False, f"两腿名义价值失衡 {imbalance:.1%}: {n_hy:.2f} vs {n_hx:.2f}"
        return True, f"对冲平衡偏差 {imbalance:.1%}"

    def align_positions_on_startup(self, p_skhy: float, p_skhx: float,
                                   real_pos_skhy: float, real_pos_skhx: float) -> bool:
        """人工确认后的启动对准。只做一次成对差额交易，并立即对账。"""
        try:
            actual_layers = self._derive_layer_count(
                real_pos_skhy, real_pos_skhx, p_skhy, p_skhx
            )
            target_layers = self._required_layers(calculate_k(p_skhy, p_skhx))
            target_notional = target_layers * self.q_per_grid
            actual_notional = actual_layers * self.q_per_grid
            diff_notional = target_notional - actual_notional

            if abs(diff_notional) > 100.0:
                if diff_notional > 0:
                    sz_hy = round_size(diff_notional / p_skhy, self.dec_hy)
                    sz_hx = round_size(diff_notional / p_skhx, self.dec_hx)
                    action = "ENTRY"
                    self._add_log(f"[启动对准] 补充约 {diff_notional:.1f} 名义价值")
                else:
                    sz_hy = round_size(abs(diff_notional) / p_skhy, self.dec_hy)
                    sz_hx = round_size(abs(diff_notional) / p_skhx, self.dec_hx)
                    action = "EXIT"
                    self._add_log(f"[启动对准] 减少约 {abs(diff_notional):.1f} 名义价值")

                result = self._execute_pair(
                    action=action,
                    level=max(1, target_layers),
                    p_skhy=p_skhy,
                    p_skhx=p_skhx,
                    total_sz_hy=sz_hy,
                    total_sz_hx=sz_hx,
                    k_val=calculate_k(p_skhy, p_skhx),
                )
                if not result.ok:
                    self._add_log(f"[启动对准失败] {result.reason}")
                    return False

            actual = self._sync_chain_positions_with_retry() if self.live_trading else (
                real_pos_skhy + (sz_hy if 'action' in locals() and action == "ENTRY" else -sz_hy if 'action' in locals() else 0.0),
                real_pos_skhx + (sz_hx if 'action' in locals() and action == "ENTRY" else -sz_hx if 'action' in locals() else 0.0),
            )
            if actual is None:
                self._add_log("[启动对准失败] 无法读取对准后的链上仓位")
                return False

            pos_hy, pos_hx = actual
            healthy, reason = self.positions_are_healthy(pos_hy, pos_hx, p_skhy, p_skhx)
            if not healthy:
                self._add_log(f"[启动对准失败] {reason}")
                return False

            with self._state_lock:
                self.pos_skhy = abs(pos_hy)
                self.pos_skhx = abs(pos_hx)
                self.positions_held = int(round(self._derive_layer_count(pos_hy, pos_hx, p_skhy, p_skhx)))
                if self.positions_held > self.max_grids:
                    self._add_log(
                        f"[启动对准] 链上仓位约 {self.positions_held} 层，超过上限 {self.max_grids}；"
                        "将按当前实际仓位接管，不再新增网格"
                    )
                    self.positions_held = self.max_grids

            # 重启后没有每层开仓价，使用当前 k 和按层均摊手续费作为保守基准。
            estimated_fee = 0.0
            for lvl in range(1, self.positions_held + 1):
                if lvl not in self.layer_entry_data:
                    self.layer_entry_data[lvl] = {
                        "k": calculate_k(p_skhy, p_skhx),
                        "fee": estimated_fee,
                        "entry_px_hy": p_skhy,
                        "entry_px_hx": p_skhx,
                    }

            self.is_aligned = True
            self._add_log(
                f"[启动对准] 接管第 {self.positions_held} 层 | SKHY={pos_hy:.{self.dec_hy}f} "
                f"SKHX={pos_hx:.{self.dec_hx}f}"
            )
            return True
        except Exception as exc:
            log_error("启动对准异常", exc)
            self._add_log(f"[启动对准异常] {exc}")
            return False

    def _required_layers(self, k: float) -> int:
        if k < self.p1:
            return 0
        return min(int((k - self.p1) // self.x) + 1, self.max_grids)

    # ------------------------------
    # 日志/UI 辅助
    # ------------------------------
    def _add_log(self, text: str) -> None:
        _add_global_log(text)

    def get_next_triggers(self) -> Tuple[Optional[float], Optional[float]]:
        next_entry = self.p1 + self.positions_held * self.x if self.positions_held < self.max_grids else None
        next_exit = self.p1 + (self.positions_held - 2) * self.x if self.positions_held > 0 else None
        return next_entry, next_exit


# ==========================================
# 💰 钱包资产、真实仓位与盈亏追踪线程
# ==========================================
def fetch_account_equity() -> None:
    global current_equity, initial_equity, withdrawable_margin
    global real_position_skhy, real_position_skhx, global_floating_pnl
    if not WALLET_ADDR:
        return

    while True:
        try:
            spot = post_info({"type": "spotClearinghouseState", "user": WALLET_ADDR}, timeout=5)
            spot_usdc_total = 0.0
            for bal in spot.get("balances", []):
                if bal.get("coin") == "USDC":
                    spot_usdc_total = float(bal.get("total", 0.0))
                    break

            xyz_state = post_info(
                {"type": "clearinghouseState", "user": WALLET_ADDR, "dex": "xyz"}, timeout=5
            )
            xyz_summary = xyz_state.get("marginSummary", {})
            xyz_account_value = float(xyz_summary.get("accountValue", 0.0))
            xyz_withdrawable = float(xyz_state.get("withdrawable", 0.0))

            r_y, r_x = 0.0, 0.0
            u_y, u_x = 0.0, 0.0
            for pos in xyz_state.get("assetPositions", []):
                p = pos.get("position", {})
                coin_name = str(p.get("coin", "")).upper()
                szi = float(p.get("szi", 0.0))
                upnl = float(p.get("unrealizedPnl", 0.0))
                if "SKHX" in coin_name or "NIX" in coin_name:
                    r_x, u_x = szi, upnl
                elif "SKHY" in coin_name:
                    r_y, u_y = szi, upnl

            real_position_skhy = r_y
            real_position_skhx = r_x
            global_floating_pnl = u_y + u_x
            current_equity = xyz_account_value + spot_usdc_total
            withdrawable_margin = max(0.0, xyz_withdrawable)
            if initial_equity is None and current_equity > 0:
                initial_equity = current_equity
        except Exception as exc:
            log_error("账户/仓位刷新失败", exc)
        time.sleep(3)


def fetch_trade_history() -> None:
    global global_realized_pnl, global_fees, global_funding
    if not WALLET_ADDR:
        return

    while True:
        try:
            fills = post_info({"type": "userFills", "user": WALLET_ADDR}, timeout=5)
            realized = 0.0
            if isinstance(fills, list):
                for f in fills:
                    if int(f.get("time", 0)) < bot_start_time_ms:
                        continue
                    coin = str(f.get("coin", ""))
                    if not ("SKHY" in coin or "SKHX" in coin or "NIX" in coin):
                        continue
                    realized += float(f.get("closedPnl", 0))
                    fill_id = f.get("tid") or f.get("hash") or (
                        f"{f.get('time')}_{coin}_{f.get('sz')}_{f.get('px')}_{f.get('fee')}"
                    )
                    if fill_id not in processed_fill_ids:
                        processed_fill_ids.add(fill_id)
                        global_fees += abs(float(f.get("fee", 0)))
            global_realized_pnl = realized

            # 对 None/xyz 两种查询结果按唯一键去重，避免资金费重复累计。
            funding = 0.0
            seen_funding = set()
            for dex_param in (None, "xyz"):
                body: Dict[str, Any] = {
                    "type": "userFunding",
                    "user": WALLET_ADDR,
                    "startTime": bot_start_time_ms,
                }
                if dex_param:
                    body["dex"] = dex_param
                funds = post_info(body, timeout=5)
                if not isinstance(funds, list):
                    continue
                for f in funds:
                    delta = f.get("delta") if isinstance(f.get("delta"), dict) else {}
                    coin = f.get("coin") or delta.get("coin", "")
                    if not ("SKHY" in coin or "SKHX" in coin or "NIX" in coin):
                        continue
                    usdc = f.get("usdc", delta.get("usdc", 0.0))
                    funding_time = f.get("time", delta.get("time", 0))
                    key = f.get("tid") or f.get("hash") or f"{funding_time}_{coin}_{usdc}"
                    if key in seen_funding:
                        continue
                    seen_funding.add(key)
                    funding += float(usdc)
            global_funding = funding
        except Exception as exc:
            log_error("成交/资金费统计失败", exc)
        time.sleep(5)


def perform_startup_confirmation_check(bot: ShortGridEngine) -> bool:
    """
    启动对准与建仓前的人工确认。无有效行情时直接失败，绝不解除交易保护。
    """
    if not bot.live_trading:
        bot.is_aligned = True
        return True

    print("\n[启动对准评估] 正在读取链上持仓与盘口行情...")
    fresh = None
    for _ in range(20):
        fresh = get_fresh_prices()
        if fresh is not None:
            break
        time.sleep(0.5)

    if fresh is None:
        print("[错误] 盘口未就绪或行情已过期，已拒绝启动交易。")
        bot.is_aligned = False
        return False

    p_skhy, p_skhx = fresh
    actual = bot._sync_chain_positions_with_retry()
    if actual is None:
        print("[错误] 无法读取链上仓位，已拒绝启动交易。")
        bot.is_aligned = False
        return False

    real_pos_skhy, real_pos_skhx = actual
    healthy, reason = bot.positions_are_healthy(real_pos_skhy, real_pos_skhx, p_skhy, p_skhx)
    if not healthy:
        print(f"[错误] 启动仓位检查失败: {reason}")
        print("请先人工处理成单腿、反向或严重失衡仓位后再启动。")
        bot.is_aligned = False
        return False

    k = calculate_k(p_skhy, p_skhx)
    target_layers = bot._required_layers(k)
    actual_layers = bot._derive_layer_count(real_pos_skhy, real_pos_skhx, p_skhy, p_skhx)
    diff_notional = (target_layers - actual_layers) * bot.q_per_grid

    print(f"[启动评估] k={k:.4f} | 目标层数={target_layers} | 当前约={actual_layers:.2f} 层")
    if abs(diff_notional) <= 100.0:
        print("[启动对准] 当前仓位已满足目标，无需交易，直接接管。")
        bot.positions_held = target_layers
        bot.pos_skhy = abs(real_pos_skhy)
        bot.pos_skhx = abs(real_pos_skhx)
        bot.is_aligned = True
        return True

    direction = "补充建仓" if diff_notional > 0 else "减少仓位"
    print("\n" + "=" * 72)
    print(f"  [⚠️ 启动对准确认] {direction}约 {abs(diff_notional):.1f} 名义价值")
    print(f"  - 目标: 第 {target_layers} 层 ({target_layers * bot.q_per_grid:.1f})")
    print(f"  - 当前: 约 {actual_layers:.2f} 层 ({actual_layers * bot.q_per_grid:.1f})")
    print(f"  - SKHY={real_pos_skhy:.{bot.dec_hy}f}, SKHX={real_pos_skhx:.{bot.dec_hx}f}")
    print("=" * 72)
    try:
        user_input = input("  请输入数字 1 并回车确认执行对准 (输入其他内容退出): ").strip()
        if user_input != "1":
            print("\n[取消执行] 未输入 1，程序退出。")
            return False
    except (KeyboardInterrupt, EOFError):
        print("\n[取消执行] 已中断。")
        return False

    return bot.align_positions_on_startup(p_skhy, p_skhx, real_pos_skhy, real_pos_skhx)


# ==========================================
# 📡 实时行情抓取模块
# ==========================================
def fetch_initial_prices() -> None:
    try:
        for coin, display_sym in HL_COIN_TO_DISPLAY.items():
            data = post_info({"type": "l2Book", "coin": coin}, timeout=5)
            levels = data.get("levels", [])
            if len(levels) >= 2 and levels[0] and levels[1]:
                update_price(display_sym, float(levels[0][0]["px"]), float(levels[1][0]["px"]))
    except Exception as exc:
        log_error("初始行情获取失败", exc)


def hl_keep_alive(ws) -> None:
    while ws_connected:
        time.sleep(5)
        try:
            ws.send(json.dumps({"method": "ping"}))
        except Exception:
            break


def hl_on_message(ws, message) -> None:
    global ws_connected
    ws_connected = True
    try:
        data = json.loads(message)
        if data.get("channel") == "l2Book" and "data" in data:
            payload = data["data"]
            coin = payload.get("coin")
            levels = payload.get("levels")
            if coin in HL_COIN_TO_DISPLAY and isinstance(levels, list) and len(levels) >= 2:
                if levels[0] and levels[1]:
                    symbol = HL_COIN_TO_DISPLAY[coin]
                    update_price(symbol, float(levels[0][0]["px"]), float(levels[1][0]["px"]))
                    fresh = get_fresh_prices()
                    if fresh is not None and algo_engine is not None:
                        p_skhy, p_skhx = fresh
                        algo_engine.on_tick(calculate_k(p_skhy, p_skhx), p_skhy, p_skhx)
    except Exception as exc:
        log_error("WebSocket 行情处理异常", exc)


def hl_on_error(ws, error) -> None:
    global ws_connected, ws_error_msg
    ws_error_msg = str(error)
    ws_connected = False
    logger.warning("Hyperliquid WebSocket 错误: %s", error)


def hl_on_close(ws, *args) -> None:
    global ws_connected
    ws_connected = False


def hl_on_open(ws) -> None:
    global ws_connected, ws_error_msg
    ws_connected, ws_error_msg = True, ""
    threading.Thread(target=hl_keep_alive, args=(ws,), daemon=True).start()
    for coin in HL_COIN_TO_DISPLAY:
        ws.send(json.dumps({"method": "subscribe", "subscription": {"type": "l2Book", "coin": coin}}))


def start_hl_ws() -> None:
    while True:
        ws = websocket.WebSocketApp(
            "wss://api.hyperliquid.xyz/ws",
            on_message=hl_on_message,
            on_error=hl_on_error,
            on_close=hl_on_close,
            on_open=hl_on_open,
        )
        ws.run_forever()
        time.sleep(2)


# ==========================================
# 🖥️ 终端 UI 全景视图渲染
# ==========================================
def render_ui(time_str: str, running_seconds: int) -> None:
    p_skhy_bid = prices["SKHY-USDC"]["bid"]
    p_skhy_ask = prices["SKHY-USDC"]["ask"]
    p_skhx_bid = prices["SKHYNIX-USDC"]["bid"]
    p_skhx_ask = prices["SKHYNIX-USDC"]["ask"]

    k_raw = calculate_k(float(p_skhy_bid), float(p_skhx_ask)) if (p_skhy_bid and p_skhx_ask) else None

    if algo_engine:
        mode_text = "[实盘交易开启 🟢]" if algo_engine.live_trading else "[模拟运行模式 🟡]"
        held = algo_engine.positions_held
        pos_y, pos_x = algo_engine.pos_skhy, algo_engine.pos_skhx
        nxt_entry, nxt_exit = algo_engine.get_next_triggers()
        if algo_engine.live_trading:
            align_txt = "对齐成功" if algo_engine.is_aligned else "未对准/禁止交易"
        else:
            align_txt = "模拟对准完成"
        if algo_engine.halted:
            status_txt = "已停机 ⛔"
        else:
            status_txt = "工作中 ⚙️" if algo_engine._worker and algo_engine._worker.is_alive() else "空闲中"
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
    screen = [
        "  HL SKHY/SKHX 相对价值做空网格机器人 v3.9",
        f"  模式: {mode_text} | 对齐: [{align_txt}] | 引擎: [{status_txt}]",
        f"  时间: {time_str} | 运行时间: {runtime_str} | 通道: {'[连通] ●' if ws_connected else '[重连中...]'}",
        "  " + LINE_EQUAL,
        "  [ 账户资产与盈亏透视 (链上同步) ]",
    ]

    if not WALLET_ADDR:
        screen.append("  (未填写 WALLET_ADDR，无法追踪资产与对账)")
    else:
        if current_equity is not None:
            screen.append(f"  当前总净值: {current_equity:.2f} | 可用保证金: {withdrawable_margin:.2f} | 总净盈亏: {total_net_pnl:+.2f}")
            screen.append(
                f"  盈亏拆解: 网格实现 {global_realized_pnl:+.2f} | 当前浮动 {global_floating_pnl:+.2f} "
                f"| 资金费 {global_funding:+.2f} | 手续费 -{global_fees:.2f}"
            )
            screen.append(
                f"  策略统计: 累计建仓 {global_entry_count} 次 | 累计止盈 {global_tp_count} 次 "
                f"| 网格估算收益 {global_true_profit:+.2f}"
            )
        screen.append(
            f"  程序账本: SKHY={-pos_y:.4f} ({val_prog_y/1000:+.1f}k) | SKHYNIX={pos_x:.4f} ({val_prog_x/1000:+.1f}k)"
        )
        screen.append(
            f"  链上真实: SKHY={real_position_skhy:.4f} ({val_real_y/1000:+.1f}k) | SKHYNIX={real_position_skhx:.4f} ({val_real_x/1000:+.1f}k)"
        )
        if algo_engine and algo_engine.halted:
            screen.append(f"  ⛔ 停机原因: {algo_engine.halt_reason[:100]}")

    screen.extend([
        "  " + LINE_DASH,
        "  [ 行情盘口与策略状态 ]",
        f"  # SKHY-USDC    : 买1={f'{p_skhy_bid:.6f}' if p_skhy_bid else 'Wait...'}  | 卖1={f'{p_skhy_ask:.6f}' if p_skhy_ask else 'Wait...'}",
        f"  # SKHYNIX-USDC : 买1={f'{p_skhx_bid:.6f}' if p_skhx_bid else 'Wait...'} | 卖1={f'{p_skhx_ask:.6f}' if p_skhx_ask else 'Wait...'}",
        f"  # 基准P1={P1} | 步长X={STEP_X} | 单次={Q_USDC} | 杠杆={LEVERAGE}",
        f"  # 拆分={SPLIT_N}笔({SLICE_DELAY}s) | 持有=[{held}/{algo_engine.max_grids if algo_engine else MAX_GRIDS}层]",
        "  " + LINE_EQUAL,
    ])
    ema_k_str = f"{algo_engine.k_smoothed:.6f}" if (algo_engine and algo_engine.k_smoothed is not None) else "Wait..."
    raw_k_str = f"{k_raw:.6f}" if k_raw is not None else "Wait..."
    screen.append(f"  # 下一建仓线(做空): {f'{nxt_entry:.5f}' if nxt_entry is not None else '锁定封顶'}")
    screen.append(f"  # 当前平滑值(原始): {ema_k_str} ({raw_k_str})")
    screen.append(f"  # 下一平仓线(止盈): {f'{nxt_exit:.5f}' if nxt_exit is not None else '空仓无'}")
    screen.append("  " + LINE_EQUAL)
    screen.append("  [ 底层交易与对账日志 ]")
    if not trade_logs:
        screen.append("  <策略监控中...等待行情触发>")
    else:
        for log in trade_logs:
            screen.append("  " + log)
    screen.append("  " + LINE_EQUAL)
    screen.append("  [Ctrl+C 停止交易并退出；默认不自动平仓]\n")

    os.system("cls" if os.name == "nt" else "clear")
    print("\n".join(screen))


def check_stop_loss() -> None:
    if not algo_engine or not algo_engine.live_trading or algo_engine.halted:
        return
    if STOP_LOSS_USDC > 0 and global_floating_pnl <= -STOP_LOSS_USDC:
        reason = f"浮动亏损 {global_floating_pnl:.2f} <= -{STOP_LOSS_USDC:.2f}"
        fresh = get_fresh_prices()
        if fresh:
            algo_engine.request_emergency_flatten(fresh[0], fresh[1], reason)
        else:
            algo_engine._halt(reason)


def main() -> None:
    global algo_engine
    setup_logging()
    os.system("cls" if os.name == "nt" else "clear")
    print("正在启动 SKHY/SKHX 相对价值网格机器人 v3.9...")

    if WALLET_ADDR:
        threading.Thread(target=fetch_account_equity, daemon=True).start()
        threading.Thread(target=fetch_trade_history, daemon=True).start()

    algo_engine = ShortGridEngine(Q_USDC, SPLIT_N, MAX_GRIDS, P1, STEP_X, SECRET_KEY, WALLET_ADDR)

    fetch_initial_prices()
    threading.Thread(target=start_hl_ws, daemon=True).start()

    time.sleep(1.5)
    if not perform_startup_confirmation_check(algo_engine):
        print("\n启动保护触发，程序退出。")
        sys.exit(1)

    algo_engine.start_worker()
    start_time = time.time()

    while True:
        try:
            now = time.time()
            check_stop_loss()
            now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            render_ui(now_str, int(now - start_time))
            time.sleep(0.3)
        except KeyboardInterrupt:
            print("\n正在停止交易工作线程...")
            algo_engine.stop_worker()
            if FLATTEN_ON_EXIT and algo_engine.live_trading:
                fresh = get_fresh_prices()
                if fresh:
                    print("FLATTEN_ON_EXIT=True，正在执行保护性平仓...")
                    algo_engine.flatten_all(fresh[0], fresh[1], reason="程序退出")
            print("已停止自动交易。")
            break
        except Exception as exc:
            log_error("主循环异常", exc)
            time.sleep(1)


if __name__ == "__main__":
    main()
