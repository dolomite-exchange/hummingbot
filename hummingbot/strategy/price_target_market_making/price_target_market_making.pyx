# distutils: language=c++
import aiohttp
import traceback
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
from hummingbot.strategy.price_target_market_making.ptmm_bucket_order_book import (
    BucketOrderBook, 
    BucketType, 
    OrderBucket
)

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

        self.c_add_markets([market_info.market])

        self.market_info = market_info
        self.market = market_info.market
        self.market_rates = None
        self.target_volume_usd = Decimal(target_volume_usd)
        self.target_spread_percentage = Decimal(target_spread_percentage)
        self.target_num_orders = target_num_orders
        self.price_step_increment = Decimal(price_step_increment)
        self.volume_coordinator = VolumeCoordinator(
            self.target_volume_usd / 2, 
            self.target_num_orders, 
            self.price_step_increment,
            self.target_spread_percentage)
        

    # ----------------------------------------
    # Status Output

    def format_status(self) -> str:
        (buckets, target_price, actual_target_price) = self.get_bucket_order_books()

        lines = []
        warning_lines = []
        max_secondary_amount = 0
        
        for bucket in buckets:
            amount = max(bucket.secondary_amounts.target, bucket.secondary_amounts.provided)
            if amount > max_secondary_amount:
                max_secondary_amount = amount

        lines.extend(["", f"  Targeting Price: {actual_target_price}"])
        lines.extend(["", f"  Liquidity Target: ${round(self.target_volume_usd, 2)}"])
        lines.extend(["", "  Price".ljust(8) + "Type".rjust(7) + "Status ".rjust(12), "  " + ("=" * 95)])

        for bucket in buckets:
            lines.extend([bucket.status_row(max_secondary_amount, bucket.price == target_price)])

        if len(warning_lines) > 0:
            lines.extend(["", "  *** WARNINGS ***"] + warning_lines)

        return "\n".join(lines)


    # ----------------------------------------
    # Placement Engine

    def get_bucket_order_books(self) -> (List[OrderBucket], Decimal):
        '''
        return (order_buckets[], current_price)
        '''
        market = self.market_info.market
        actual_target_price = self.market_rates.convert(1, self.market_info.base_asset, self.market_info.quote_asset)
        target_price = BucketOrderBook.to_bucket_price(actual_target_price, self.price_step_increment)

        order_book = BucketOrderBook(
            market_symbol=self.market_info.trading_pair,
            market=market,
            volume_coordinator=self.volume_coordinator,
            active_order_tracker=self._sb_order_tracker,
            market_rates=self.market_rates)
        
        return (order_book.get_buckets(target_price), target_price, round(actual_target_price, 4))


    async def rebalance_books(self):
        (buckets, target_price, __) = self.get_bucket_order_books()

        # self.logger().info(f"============================================")
        
        for bucket in buckets:
            
            # self.logger().info('BUCKET!')

            # self.logger().info(f"Price: {bucket.price}")
            # self.logger().info(f"Target Type: {bucket.target_type}")
            # self.logger().info(f"Current Type: {bucket.type}")
            # self.logger().info(f"Provided: {bucket.primary_amounts.provided}")
            # self.logger().info(f"Target: {bucket.primary_amounts.target}")
            # self.logger().info(f"--------------------------------------------")




            if bucket.primary_amounts.provided <= 0:
                self.logger().info(bucket.primary_amounts.target)
                if bucket.target_type == BucketType.BID:
                    self.c_buy_with_specific_market(
                        self.market_info,
                        bucket.primary_amounts.target,
                        OrderType.LIMIT,
                        bucket.price)
                elif bucket.target_type == BucketType.ASK:
                    self.c_sell_with_specific_market(
                        self.market_info,
                        bucket.primary_amounts.target,
                        OrderType.LIMIT,
                        bucket.price)


                # # Cancel our orders that are in the spread or outside the range
                # if ASK or BID but should be EMPTY:
                #     if we have ask or bid orders here:
                #         cancel them

                # # Cancel our ask orders that should be bid orders
                # # Then (diff run) queue any other maker ask orders for filling
                # if ASK but should be BID:
                #     if we have ask orders here:
                #         cancel them
                #     elif the fillable amount > 0:
                #         # notice: this can only occur on the pass through after we cancel out orders at this bucket!
                #         queue these orders for BUYing (will be checked for profitability later)

                # # Cancel our bid orders that should be ask orders
                # # Then (diff run) queue any other maker bid orders for filling
                # if BID but should be ASK:
                #     if we have bid orders here:
                #         cancel them
                #     elif the fillable amount > 0:
                #         # notice: this can only occur on the pass through after we cancel out orders at this bucket!
                #         queue these orders for SELLing (will be checked for profitability later)

                # if BID and should be BID:
                #     # outside_liquidity_provided = primary.current_amount - primary.provided_amount
                #     # if (primary.provided_liquidity)

                #     if primary.provided_amount <= primary.min_amount:
                #         replace(order)
                #     elif primary.provided_amount >= (primary.target_amount * Decimal(1.10)):







                    # object market_symbol_pair, object amount,
                    #                                         object order_type = OrderType.MARKET,
                    #                                         object price = decimal_nan

            # current
            # provided
            # min
            # target

            # price
            # type
            # target_type
            # fillable_quantity
            # primary_amounts
            # secondary_amounts
            # tracked_orders
            # incorrectly_placed_orders


    # ----------------------------------------
    # Polling

    async def _polling_update(self):
        while True:
            try:
                self._poll_notifier = asyncio.Event()
                await self._poll_notifier.wait()

                if self.market_info.market.ready and self._is_strategy_active:
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

                traceback.print_exc() # TODO: remove


    async def update_market_data(self):
        exchange_rates_response = await self.http_request("GET", EXCHANGE_RATES_ROUTE)
        self.market_rates = DolomiteExchangeRates(exchange_rates_response["data"])


    # ==========================================================

    cdef c_did_fill_order(self, object fill):
        cdef:
            LimitOrder limit_order = self._sb_order_tracker.c_get_limit_order(fill.symbol, fill.order_id)
            LimitOrder replacement = LimitOrder(
                limit_order.client_order_id,
                limit_order.symbol,
                limit_order.is_buy,
                limit_order.base_currency,
                limit_order.quote_currency,
                limit_order.price,
                limit_order.quantity - fill.amount)
            makers = self._sb_order_tracker._tracked_maker_orders

        # Replace tracked maker order with a replacement that has an accurate quantity from the fill event
        if limit_order.order_type is OrderType.LIMIT:
            self._sb_order_tracker._tracked_maker_orders = [
                replacement if o.client_order_id == limit_order.client_order_id else o 
                for o in makers]

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




















