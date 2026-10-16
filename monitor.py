"""
加密货币价格监控机器人
监控币安平台价格，触发条件时通过阿里云语音电话通知
"""

import time
import logging
from datetime import datetime
from collections import deque
from config import WATCH_LIST, PHONE_NUMBERS, CHECK_INTERVAL_SECONDS, PUSHCUT_WEBHOOK_URLS
from binance_client import get_price
# from aliyun_voice import send_voice_calls
from pushcut_notify import send_voice_calls

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("monitor.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


class PriceMonitor:
    def __init__(self):
        # 为每个币种维护一个价格历史队列 { symbol: deque([(timestamp, price), ...]) }
        self.price_history: dict[str, deque] = {}
        # 记录上次报警时间，避免同一条件反复轰炸 { symbol+reason: timestamp }
        self.last_alert: dict[str, float] = {}
        # 报警冷却时间（秒），同一触发条件 5 分钟内只报警一次
        self.alert_cooldown = 300

        for cfg in WATCH_LIST:
            symbol = cfg["symbol"]
            # 队列最大长度 = M 分钟 / 采集间隔，多保留一点余量
            max_len = int(cfg["M"] * 60 / CHECK_INTERVAL_SECONDS) + 5
            self.price_history[symbol] = deque(maxlen=max_len)

    # ------------------------------------------------------------------ #
    #  冷却检查                                                             #
    # ------------------------------------------------------------------ #
    def _is_cooling_down(self, key: str) -> bool:
        last = self.last_alert.get(key, 0)
        return (time.time() - last) < self.alert_cooldown

    def _mark_alert(self, key: str):
        self.last_alert[key] = time.time()

    # ------------------------------------------------------------------ #
    #  触发检查                                                             #
    # ------------------------------------------------------------------ #
    def check_triggers(self, cfg: dict, current_price: float) -> list[str]:
        """
        返回需要播报的告警文本列表（可能同时触发多个条件）
        """
        symbol = cfg["symbol"]
        M = cfg["M"]          # 分钟窗口
        N = cfg["N"]          # 跌幅百分比阈值
        P = cfg.get("P")      # 跌破价格阈值（可选，设为 None 则不监控）

        alerts = []
        history = self.price_history[symbol]

        # --- 条件1：M 分钟内跌幅超过 N% ---
        window_seconds = M * 60
        now = time.time()
        # 找到 M 分钟前最早的价格
        baseline_price = None
        for ts, price in history:
            if now - ts >= window_seconds:
                baseline_price = price  # 越旧越好，取窗口起点
            else:
                break  # deque 是时间顺序，后面都是更新的

        if baseline_price is None and len(history) > 0:
            # 历史不足 M 分钟，取最早一条
            baseline_price = history[0][1]

        if baseline_price and baseline_price > 0:
            drop_pct = (baseline_price - current_price) / baseline_price * 100
            if drop_pct >= N:
                key = f"{symbol}_drop_{N}pct"
                if not self._is_cooling_down(key):
                    msg = (
                        f"{symbol} 价格警报！"
                        f"过去{M}分钟内下跌{drop_pct:.2f}个百分点，"
                        f"从{baseline_price:.4f}跌至当前{current_price:.4f}，"
                        f"请立即关注！"
                    )
                    alerts.append(msg)
                    self._mark_alert(key)
                    logger.warning(f"[触发] {symbol} {M}min跌幅 {drop_pct:.2f}% >= {N}%")

        # --- 条件2：跌破价格 P ---
        if P is not None and current_price < P:
            key = f"{symbol}_below_{P}"
            if not self._is_cooling_down(key):
                msg = (
                    f"{symbol} 价格警报！"
                    f"当前价格{current_price:.4f}已跌破警戒线{P}，"
                    f"请立即关注！"
                )
                alerts.append(msg)
                self._mark_alert(key)
                logger.warning(f"[触发] {symbol} 当前价 {current_price} < P={P}")

        return alerts

    # ------------------------------------------------------------------ #
    #  主循环                                                               #
    # ------------------------------------------------------------------ #
    def run(self):
        logger.info("=== 价格监控启动 ===")
        for cfg in WATCH_LIST:
            logger.info(
                f"  监控: {cfg['symbol']}  |  "
                f"跌幅触发: {cfg['M']}min内跌>{cfg['N']}%  |  "
                f"价格触发: 跌破{cfg.get('P', '未设置')}"
            )
        logger.info(f"  通知电话: {PHONE_NUMBERS}")
        logger.info(f"  采集间隔: {CHECK_INTERVAL_SECONDS}秒")

        while True:
            for cfg in WATCH_LIST:
                symbol = cfg["symbol"]
                try:
                    price = get_price(symbol)
                    if price is None:
                        logger.warning(f"[{symbol}] 获取价格失败，跳过本轮")
                        continue

                    now = time.time()
                    self.price_history[symbol].append((now, price))
                    logger.info(f"[{symbol}] 当前价格: {price:.6f}")

                    alerts = self.check_triggers(cfg, price)
                    for alert_text in alerts:
                        logger.warning(f"[报警] {alert_text}")
                        send_voice_calls(PUSHCUT_WEBHOOK_URLS, alert_text)

                except Exception as e:
                    logger.error(f"[{symbol}] 处理异常: {e}", exc_info=True)

            time.sleep(CHECK_INTERVAL_SECONDS)


if __name__ == "__main__":
    monitor = PriceMonitor()
    monitor.run()
