# distutils: language=c++
import aiohttp
import traceback
import json
from enum import Enum
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


class Action(object):
    def __init__(self, action_type, payload):
        self.type = action_type
        self.payload = payload
        pass

    class ActionType(Enum):
        CANCEL = 1
        PLACE = 2
        REPLACE = 3

    class CancelPayload(object):
        def __init__(self, order_id):
            self.order_id = order_id

    class PlacePayload(object):
        def __init__(self, order_type, order_side, quantity, price = None):
            self.order_type = order_type
            self.order_side = order_side
            self.quantity = quantity
            self.price = price

    @classmethod
    def cancel(cls, order_id):
        return Action(Action.ActionType.CANCEL, Action.CancelPayload(order_id))

    @classmethod
    def place(cls, order_type, order_side, quantity, price):
        return Action(Action.ActionType.PLACE, Action.PlacePayload(order_type, order_side, quantity, price))

    @classmethod
    def replace(cls, order_id, order_type, order_side, quantity, price):
        return Action(Action.ActionType.REPLACE, (
                Action.CancelPayload(order_id), 
                Action.PlacePayload(order_type, order_side, quantity, price)))


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
                 poll_interval: float = 15):
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

        self.action_queue = []
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
        (buckets, target_price, actual_target_price) = self._get_bucket_order_books()

        lines = []
        warning_lines = []
        max_secondary_amount = 0

        # Misc Tables
        markets_df = self.market_status_data_frame([self.market_info])
        lines.extend(["", "  Markets:"]  + ["    " + line for line in str(markets_df).split("\n")])

        assets_df = self.wallet_balance_data_frame([self.market_info])
        lines.extend(["", "  Assets:"] + ["    " + line for line in str(assets_df).split("\n")])

        warning_lines.extend(self.balance_warning([self.market_info]))

        # Active order table
        active_orders = self._sb_order_tracker.market_pair_to_active_orders[self.market_info]

        if len(active_orders) > 0:
            df = LimitOrder.to_pandas(active_orders)
            df_lines = str(df).split("\n")
            lines.extend(["", "  Active orders:"] +
                         ["    " + line for line in df_lines])
        else:
            lines.extend(["", "  No active maker orders."])

        # Bucket order book table
        for bucket in buckets:
            amount = max(bucket.secondary_amounts.target, bucket.secondary_amounts.provided)
            if amount > max_secondary_amount:
                max_secondary_amount = amount

        lines.extend(["", f"  Targeting Price: {actual_target_price}"])
        lines.extend([f"  Liquidity Target: ${round(self.target_volume_usd, 2)}"])
        lines.extend(["", "  Price".ljust(12) + "Type".rjust(7) + "Status   ".rjust(12), "  " + ("=" * 95)])

        for bucket in buckets:
            lines.extend([bucket.status_row(max_secondary_amount, bucket.price == target_price)])

        if len(warning_lines) > 0:
            lines.extend(["", "  *** WARNINGS ***"] + warning_lines)

        return "\n".join(lines)


    # ----------------------------------------
    # Placement Engine

    async def rebalance_books(self):
        (buckets, target_price, real_target_price) = self._get_bucket_order_books()

        self.logger().info(f"Rebalancing order books at price: {real_target_price}")

        for bucket in buckets:
            
            # Cancel misplaced orders
            for bad_order in bucket.incorrectly_placed_orders:
                self.queue(Action.cancel(bad_order.client_order_id))

            # Check for multiple tracked orders, cancel all but the largest
            if len(bucket.tracked_orders) > 1:
                bucket.tracked_orders.sort(key=lambda o: o.quantity, reverse=True)
                tracked_order = bucket.tracked_orders[0]
                for dup_order in bucket.tracked_orders[0:]:
                    self.queue(Action.cancel(dup_order.client_order_id))
            elif len(bucket.tracked_orders) == 1:
                tracked_order = bucket.tracked_orders[0]
            else:
                tracked_order = None

            # Correct order fill amounts
            if bucket.type == bucket.target_type and bucket.target_type is not BucketType.EMPTY:
                is_below_min = bucket.primary_amounts.provided <= bucket.primary_amounts.min
                is_above_target_threshold = bucket.primary_amounts.provided > (bucket.primary_amounts.target * Decimal(1.10))

                if is_below_min or is_above_target_threshold:
                    order_side = TradeType.BUY if bucket.target_type is BucketType.BID else TradeType.SELL
                    order_type = OrderType.LIMIT
                    order_payload = Action.PlacePayload(order_type, order_side, bucket.primary_amounts.target, bucket.price)

                    if tracked_order is not None:
                        cancellation_payload = Action.CancelPayload(tracked_order.client_order_id)
                        self.queue(Action(Action.ActionType.REPLACE, (cancellation_payload, order_payload)))
                    else:
                        self.queue(Action(Action.ActionType.PLACE, order_payload))

        await self.perform_queued_actions()
            

    def queue(self, action):
        self.action_queue.append(action)


    async def perform_queued_actions(self):
        '''
        Calculate a wait time between each action based on the set polling interval and 
        action count, then perform each action in order and wait the calculated wait time
        between each execution
        '''
        wait_time = min(self._poll_interval / len(self.action_queue), 0.5)
        market_info = self.market_info
        market = self.market

        for action in self.action_queue:
            if action.type is Action.ActionType.CANCEL:
                market.cancel(market_info.trading_pair, action.payload.order_id)
            else:
                order_payload = action.payload

                if action.type is Action.ActionType.REPLACE:
                    (__, order_payload) = action.payload

                if order_payload.order_side is TradeType.BUY:
                    self.c_buy_with_specific_market(
                        market_info,
                        order_payload.quantity,
                        order_payload.order_type,
                        order_payload.price)
                elif order_payload.order_side is TradeType.SELL:
                    self.c_sell_with_specific_market(
                        market_info,
                        order_payload.quantity,
                        order_payload.order_type,
                        order_payload.price)

                await asyncio.sleep(0.15) 

                if action.type is Action.ActionType.REPLACE:
                    (cancellation_payload, __) = action.payload
                    market.cancel(market_info.trading_pair, cancellation_payload.order_id)
                    
            
            await asyncio.sleep(wait_time) 


    def _get_bucket_order_books(self) -> (List[OrderBucket], Decimal):
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


    # self._place_order(
    #                     order_payload.order_type,
    #                     order_payload.order_side,
    #                     order_payload.quantity,
    #                     order_payload.price)
    # def _place_order(self, order_type, order_side, amount, price):
    #         cdef:
    #             MarketBase market = self.market

    #         kwargs = { "expiration_ts": self._current_timestamp + self._sb_limit_order_min_expiration }
            
    #         if self.market not in self._sb_markets:
    #             raise ValueError(f"Market object for buy order is not in the whitelisted markets set.")

    #         if order_side is TradeType.BUY:
    #             order_id = market.c_buy(self.market_info.trading_pair, amount,
    #                                     order_type=order_type, price=price, kwargs=kwargs)
    #         elif order_side is TradeType.SELL:
    #             order_id = market.c_sell(self.market_info.trading_pair, amount,
    #                                     order_type=order_type, price=price, kwargs=kwargs)

    #         # Start order tracking
    #         if order_type == OrderType.LIMIT:
    #             self.c_start_tracking_limit_order(self.market_info, order_id, True, price, amount)
    #         elif order_type == OrderType.MARKET:
    #             self.c_start_tracking_market_order(self.market_info, order_id, True, amount)

    #         return order_id


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




















