import math
from enum import Enum
from decimal import Decimal
from typing import (
    List,
    Tuple,
    Optional,
    Dict,
    Any
)

from hummingbot.core.event.events import (OrderType, TradeType)
from hummingbot.core.data_type.limit_order import LimitOrder
from hummingbot.core.data_type.order_book_row import OrderBookRow
from hummingbot.market.market_base import MarketBase
from hummingbot.market.dolomite.dolomite_util import DolomiteExchangeRates
from hummingbot.strategy.order_tracker import OrderTracker
from .ptmm_volume_coordinator import VolumeCoordinator

s_decimal_zero = Decimal(0)
s_decimal_one = Decimal(1)

class BucketType(Enum):
    EMPTY = 1
    BID = 2
    ASK = 3

class BucketAmounts(object):
    def __init__(self, current, provided, min, target):
        self.current = Decimal(current)
        self.provided = Decimal(provided)
        self.min = Decimal(min)
        self.target = Decimal(target)


class OrderBucket(object):
    def __init__(self,
                 price: Decimal,
                 given_type: BucketType,
                 target_type: BucketType,
                 fillable_quantity: Decimal,
                 primary_amounts: BucketAmounts,
                 secondary_amounts: BucketAmounts,
                 tracked_orders: List[LimitOrder],
                 incorrectly_placed_orders: List[LimitOrder]):
        self.price = price
        self.type = given_type
        self.target_type = target_type
        self.fillable_quantity = fillable_quantity
        self.primary_amounts = primary_amounts
        self.secondary_amounts = secondary_amounts
        self.tracked_orders = tracked_orders
        self.incorrectly_placed_orders = incorrectly_placed_orders

    def __repr__(self) -> str:
        return (f"OrderBucket({self.price}, '{self.type}', '{self.target_type}')")


class BucketOrderBook(object):
    def __init__(self, 
                 market_symbol: str,
                 market: MarketBase, 
                 volume_coordinator: VolumeCoordinator, 
                 active_order_tracker: OrderTracker,
                 market_rates: DolomiteExchangeRates):
        self.market_symbol = market_symbol
        self.market = market
        self.coordinator = volume_coordinator
        self.tracker = active_order_tracker
        self.market_rates = market_rates


    def get_buckets(self, current_price: Decimal) -> List[OrderBucket]:

        def to_bucket_price(num):
            return BucketOrderBook.to_bucket_price(num, self.coordinator.step_increment)

        current_price = to_bucket_price(current_price)
        order_book = self.market.order_books[self.market_symbol]
        (primary_ticker, secondary_ticker) = self.market.split_symbol(self.market_symbol)
        buckets = []

        bid_rows = {}
        ask_rows = {}
        tracked_bids = {}
        tracked_asks = {}
        (bid_prices, spread_prices, ask_prices) = self._generate_target_bucket_prices(current_price)

        for bid in order_book.bid_entries():
            bucket_price = to_bucket_price(bid.price)
            if bucket_price not in bid_rows: bid_rows[bucket_price] = []
            bid_rows[bucket_price].append(bid)

        for ask in order_book.ask_entries():
            bucket_price = to_bucket_price(ask.price)
            if bucket_price not in ask_rows: ask_rows[bucket_price] = []
            ask_rows[bucket_price].append(ask)

        for tracked_bid in self.tracker.active_bids:
            bucket_price = to_bucket_price(tracked_bid.price)
            if bucket_price not in tracked_bids: tracked_bids[bucket_price] = []
            tracked_bids[bucket_price].append(tracked_bid)

        for tracked_ask in self.tracker.active_bids:
            bucket_price = to_bucket_price(tracked_ask.price)
            if bucket_price not in tracked_asks: tracked_asks[bucket_price] = []
            tracked_asks[bucket_price].append(tracked_ask)

        tracked_prices = set(
            list(bid_rows.keys()) +
            list(ask_rows.keys()) +
            list(tracked_bids.keys()) +
            list(tracked_asks.keys()) +
            bid_prices +
            spread_prices +
            ask_prices)

        for price in tracked_prices:
            order_index = s_decimal_zero
            given_type = BucketType.EMPTY
            target_type = BucketType.EMPTY
            tracked_orders = []
            incorrectly_placed_orders = []
            fillable_quantity = s_decimal_zero
            current_primary = s_decimal_zero
            current_secondary = s_decimal_zero
            provided_primary = s_decimal_zero
            provided_secondary = s_decimal_zero

            if price in bid_prices:
                price = Decimal(price)
                target_type = BucketType.BID
                order_index = len(bid_prices) - bid_prices.index(price) - 1

                if price in tracked_asks:
                    incorrectly_placed_orders = tracked_asks[price]

                if price in tracked_bids:
                    tracked_orders = tracked_bids[price]
                    (provided_primary, provided_secondary) = self._sum_tracked_amounts(tracked_orders)
              
                if price in ask_rows:
                    (fillable_quantity, __) = self._sum_row_amounts(ask_rows[price], incorrectly_placed_orders)
                    given_type = BucketType.ASK
                else:
                    given_type = BucketType.BID

                if price in bid_rows:
                    (current_primary, current_secondary) = self._sum_row_amounts(bid_rows[price])

            elif price in ask_prices:
                target_type = BucketType.ASK
                order_index = ask_prices.index(price)

                if price in tracked_bids:
                    incorrectly_placed_orders = tracked_bids[price]

                if price in tracked_asks:
                    tracked_orders = tracked_asks[price]
                    (provided_primary, provided_secondary) = self._sum_tracked_amounts(tracked_orders)
              
                if price in bid_rows:
                    (fillable_quantity, __) = self._sum_row_amounts(bid_rows[price], incorrectly_placed_orders)
                    given_type = BucketType.BID
                else:
                    given_type = BucketType.ASK

                if price in ask_rows:
                    (current_primary, current_secondary) = self._sum_row_amounts(ask_rows[price])

            else:
                if price in tracked_asks:
                    tracked_orders += tracked_asks[price]
                    given_type = BucketType.ASK
                if price in tracked_bids:
                    tracked_orders += tracked_bids[price]
                    given_type = BucketType.BID

                (provided_primary, provided_secondary) = self._sum_tracked_amounts(tracked_orders)

            target_usd = self.coordinator.target_usd_volume_at(order_index, price)
            min_usd = self.coordinator.min_usd_volume_at(order_index, price)

            if target_type is BucketType.EMPTY:
                target_usd = s_decimal_zero
                min_usd = s_decimal_zero

            min_primary = round(self.market_rates.from_base(min_usd, "USD", secondary_ticker), 4)
            min_secondary = round(min_primary / price, 4)
            target_secondary = round(self.market_rates.from_base(target_usd, "USD", secondary_ticker), 4)
            target_primary = round(target_secondary / price, 4)
            
            primary_amounts = BucketAmounts(current=current_primary,
                                            provided=provided_primary,
                                            min=min_primary,
                                            target=target_primary)

            secondary_amounts = BucketAmounts(current=target_secondary,
                                            provided=target_secondary,
                                            min=target_secondary,
                                            target=target_secondary)

            buckets.append(OrderBucket(price=price,
                                       given_type=given_type,
                                       target_type=target_type,
                                       fillable_quantity=fillable_quantity,
                                       primary_amounts=primary_amounts,
                                       secondary_amounts=secondary_amounts,
                                       tracked_orders=tracked_orders,
                                       incorrectly_placed_orders=incorrectly_placed_orders))
        return buckets


    def _generate_target_bucket_prices(self, current_price: Decimal) -> (List[Decimal], List[Decimal], List[Decimal]):
        '''
        returns (bid prices, spread prices, ask prices)
        '''
        spread_p = Decimal(self.coordinator.target_spread_percentage / 2)
        num_orders = Decimal(self.coordinator.target_num_orders)
        step_increment = Decimal(self.coordinator.step_increment)

        lowest_ask_price = BucketOrderBook.to_bucket_price(current_price * (s_decimal_one + spread_p), step_increment)
        highest_bid_price = BucketOrderBook.to_bucket_price(current_price * (s_decimal_one - spread_p), step_increment)

        highest_bucket_price = lowest_ask_price + (num_orders * step_increment)
        lowest_bucket_price = highest_bid_price - (num_orders * step_increment)

        bid_prices = []
        spread_prices = []
        ask_prices = []
        _p = lowest_bucket_price

        while(_p <= highest_bucket_price): 
            if _p < highest_bid_price: bid_prices.append(Decimal(_p))
            elif _p <= lowest_ask_price: spread_prices.append(Decimal(_p)) 
            elif _p <= highest_bucket_price: ask_prices.append(Decimal(_p)) 
            _p += step_increment

        return (bid_prices, spread_prices, ask_prices)


    def _sum_row_amounts(self, rows, tracked_orders = []) -> (Decimal, Decimal):
        amount_primary = s_decimal_zero
        amount_secondary = s_decimal_zero
        (tracked_amount_primary, tracked_amount_secondary) = self._sum_tracked_amounts(tracked_orders)

        for row in rows:
            amount_primary += Decimal(row.amount)
            amount_secondary += Decimal(row.amount) * Decimal(row.price)

        return (max(amount_primary - tracked_amount_primary, s_decimal_zero), 
                max(amount_secondary - tracked_amount_secondary, s_decimal_zero))


    def _sum_tracked_amounts(self, tracked_orders) -> (Decimal, Decimal):
        amount_primary = s_decimal_zero
        amount_secondary = s_decimal_zero

        for tracked in tracked_orders:
            amount_primary += Decimal(tracked.quantity)
            amount_secondary += Decimal(tracked.quantity) * Decimal(tracked.price)

        return (amount_primary, amount_secondary)


    @classmethod
    def to_bucket_price(cls, price, step_increment):
        round_k = Decimal(1) / Decimal(step_increment)
        return Decimal(math.floor(Decimal(price) * round_k) / round_k)
