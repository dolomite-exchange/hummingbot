# distutils: language=c++
import aiohttp
import json
from decimal import Decimal
import asyncio 
import logging
from typing import (
    List,
    Tuple,
    Optional,
    Dict,
    Any
)
from libc.stdint cimport int64_t
from datetime import datetime

from hummingbot.core.clock cimport Clock
from hummingbot.logger import HummingbotLogger
from hummingbot.core.event.event_listener cimport EventListener
from hummingbot.core.data_type.limit_order cimport LimitOrder
from hummingbot.core.data_type.limit_order import LimitOrder
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.core.data_type.order_book cimport OrderBook
from hummingbot.market.market_base import (
    MarketBase,
    OrderType,
    TradeType
)
from hummingbot.strategy.market_symbol_pair import MarketSymbolPair
from hummingbot.strategy.strategy_base import StrategyBase

from hummingbot.strategy.price_target_market_making.ptmm_volume_coordinator import (
    VolumeCoordinator
)
from hummingbot.market.dolomite.dolomite_util import DolomiteExchangeRates

s_decimal_zero = Decimal(0)
ds_logger = None

UPDATE_FAILURE_COUNT_TO_EXIT = 3
HTTP_CALL_TIMEOUT = 10
EXCHANGE_RATES_ROUTE = "https://exchange-api.dolomite.io/v1/tokens/rates/latest"


cdef class PriceTargetMarketMakingStrategy(StrategyBase):
    
    @classmethod
    def logger(cls) -> HummingbotLogger:
        global ds_logger
        if ds_logger is None:
            ds_logger = logging.getLogger(__name__)
        return ds_logger


    def __init__(self,
                 market_info: MarketSymbolPair,
                 target_volume_usd: float,
                 target_spread_percentage: float,
                 target_num_orders: int,
                 price_step_increment: float,
                 poll_interval: float = 10):
        super().__init__()

        self._last_timestamp = 0
        self._poll_interval = poll_interval
        self._poll_notifier = asyncio.Event()
        self._polling_update_task = None
        self._update_failure_count = 0
        self._is_strategy_active = True
        self._all_markets_ready = False
        self._shared_client = None

        self.market_info = market_info
        self.market = market_info.market
        self.market_rates = None
        self.target_volume_usd = Decimal(target_volume_usd)
        self.target_spread_percentage = Decimal(target_spread_percentage)
        self.target_num_orders = target_num_orders
        self.price_step_increment = Decimal(price_step_increment)
        self.volume_coordinator = VolumeCoordinator(self.target_volume_usd, self.target_num_orders)
        

    # ----------------------------------------
    # Status Output

    def format_status(self) -> str:
        return "TODO: format a status output"


    # ----------------------------------------
    # Placement Engine

    async def rebalance_books(self):
        target_price = self.market_rates.convert(1, self.market_info.base_asset, self.market_info.quote_asset)
        self.logger().info(target_price)


    # ----------------------------------------
    # Polling

    async def _polling_update(self):
        while True:
            try:
                self._poll_notifier = asyncio.Event()
                await self._poll_notifier.wait()

                if not self._all_markets_ready:
                    self._all_markets_ready = all([market.ready for market in self._sb_markets])

                if self._all_markets_ready and self._is_strategy_active:
                    await self.update_market_data()

                    if self.market_rates is not None:
                        await self.rebalance_books()

            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._update_failure_count += 1
                remaining_attempts = UPDATE_FAILURE_COUNT_TO_EXIT - self._update_failure_count

                if remaining_attempts <= 0:
                    self.logger().warn(f"There have been {UPDATE_FAILURE_COUNT_TO_EXIT} failed attempts to update. "
                                        "Cancelling all open orders and halting strategy.")
                    await self.market.cancel_all(20)
                    self._is_strategy_active = False
                else:
                    self.logger().warn(f"Polling update failed. Attempting {remaining_attempts} more time(s) before quitting")
                    self.logger().info(e)


    async def update_market_data(self):
        exchange_rates_response = await self.http_request("GET", EXCHANGE_RATES_ROUTE)
        self.market_rates = DolomiteExchangeRates(exchange_rates_response["data"])


    # ==========================================================

    cdef c_start(self, Clock clock, double timestamp):
        StrategyBase.c_start(self, clock, timestamp)
        self._last_timestamp = timestamp
        self._polling_update_task = asyncio.ensure_future(self._polling_update())
    

    cdef c_stop(self, Clock clock):
        StrategyBase.c_stop(self, clock)
        if self._polling_update_task is not None:
            self._polling_update_task.cancel()
        self._polling_update_task = None


    cdef c_tick(self, double timestamp):
        cdef:
            int64_t last_tick = <int64_t>(self._last_timestamp / self._poll_interval)
            int64_t current_tick = <int64_t>(timestamp / self._poll_interval)

        StrategyBase.c_tick(self, timestamp)
        if current_tick > last_tick:
            if not self._poll_notifier.is_set():
                self._poll_notifier.set()
        self._last_timestamp = timestamp


    async def http_request(self, http_method: str, url: str, data: Optional[Dict[str, Any]] = None,
                           params: Optional[Dict[str, Any]] = None, 
                           headers: Optional[Dict[str, str]] = None) -> Dict[str, Any]:

        if self._shared_client is None:
            self._shared_client = aiohttp.ClientSession()

        if data is not None and http_method == "POST":
            data = json.dumps(data).encode('utf8')
            headers = {"Content-Type": "application/json"}

        async with self._shared_client.request(http_method, url=f"{url}", timeout=HTTP_CALL_TIMEOUT, 
                                               data=data, params=params, headers=headers) as response:
            if response.status != 200:
                raise IOError(f"Error fetching data from {url}. HTTP status is {response.status}.")
            data = await response.json()
            return data




















