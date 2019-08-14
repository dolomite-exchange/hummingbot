import aiohttp
import asyncio
from async_timeout import timeout
from cachetools import TTLCache
from collections import (
    deque,
    OrderedDict
)   
import logging
import math
import time
from typing import(
    Any,
    Dict,
    List,
    Optional
)
from decimal import Decimal
from libc.stdint cimport int64_t
from web3 import Web3

from hummingbot.logger import HummingbotLogger
from hummingbot.core.data_type.cancellation_result import CancellationResult

from hummingbot.market.dolomite.dolomite_api_order_book_data_source import DolomiteAPIOrderBookDataSource


from hummingbot.core.event.events import (
    MarketEvent,
    BuyOrderCompletedEvent,
    SellOrderCompletedEvent,
    OrderFilledEvent,
    OrderCancelledEvent,
    MarketOrderFailureEvent,
    BuyOrderCreatedEvent,
    SellOrderCreatedEvent,
    TradeType,
    OrderType,
    TradeFee,
)
from hummingbot.core.data_type.limit_order import LimitOrder
from hummingbot.core.data_type.order_book cimport OrderBook
from hummingbot.core.data_type.order_book_tracker import OrderBookTrackerDataSourceType
from hummingbot.market.market_base cimport MarketBase
from hummingbot.market.dolomite.dolomite_order_book_tracker import DolomiteOrderBookTracker 
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.wallet.ethereum.web3_wallet import Web3Wallet

 

import binascii 
import time 
import json
import urllib.request

s_logger = None
s_decimal_0 = Decimal(0)
NaN = float("nan")


cdef class DolomiteMarketTransactionTracker(TransactionTracker):
    cdef:
        DolomiteMarket _owner

    def __init__(self, owner: DolomiteMarket):
        super().__init__()
        self._owner = owner

    cdef c_did_timeout_tx(self, str tx_id):
        TransactionTracker.c_did_timeout_tx(self, tx_id)
        self._owner.c_did_timeout_tx(tx_id)


cdef class TradingRule:
    cdef:
        public str symbol
        public double min_maker_order_size
        public double min_taker_order_size
        public int price_decimals               # max amount of decimals in a price
        public int amount_decimals              # max amount of decimals in an amount
        public bint supports_limit_orders       # if limit order is allowed for this trading pair
        public bint supports_market_orders      # if market order is allowed for this trading pair

    @classmethod
    def parse_exchange_info(cls, markets: List[Dict[str, Any]], min_maker_order_usd, min_taker_order_usd, weth_price) -> List[TradingRule]:
        cdef:
            list retval = []
        
        for market in markets["data"]:
            try:
                
                symbol = market["market"]
                
                minMakerOrderSize = None
                minTakerOrderSize = None
                
                #Convert min maker order / min taker order size to quantity of base token
                
                if symbol == "BAT-WETH": #quote token ETH
                    min_maker_order = min_maker_order_usd / weth_price
                    min_taker_order = min_taker_order_usd / weth_price
                    
                    minMakerOrderSize = min_maker_order / (float(market["last_price_traded"]["amount"]) / math.pow(10, market["last_price_traded"]["currency"]["precision"]))
                    minTakerOrderSize = min_taker_order / (float(market["last_price_traded"]["amount"]) / math.pow(10, market["last_price_traded"]["currency"]["precision"]))
                    
                else: #quote token DAI
                
                    minMakerOrderSize = min_maker_order_usd / (float(market["last_price_traded"]["amount"]) / math.pow(10, market["last_price_traded"]["currency"]["precision"]))
                    minTakerOrderSize = min_taker_order_usd / (float(market["last_price_traded"]["amount"]) / math.pow(10, market["last_price_traded"]["currency"]["precision"]))
                
                
                retval.append(TradingRule(
                                          symbol,
                                          minMakerOrderSize,
                                          minTakerOrderSize,
                                          market["last_price_traded"]["currency"]["display_precision"],
                                          market["period_amount"]["currency"]["display_precision"],
                                          True, 
                                          True))
                
            except Exception:
                DolomiteMarket.logger().error(f"Error parsing the symbol {symbol}. Skipping.", exc_info=True)
        return retval

    def __init__(self, symbol: str, min_maker_order_size: float, min_taker_order_size: float, price_decimals: int,
                 amount_decimals: int, supports_limit_orders: bool, supports_market_orders: bool):
        self.symbol = symbol
        self.min_maker_order_size = min_maker_order_size
        self.min_taker_order_size = min_taker_order_size
        self.price_decimals = price_decimals
        self.amount_decimals = amount_decimals
        self.supports_limit_orders = supports_limit_orders
        self.supports_market_orders = supports_market_orders

    def __repr__(self) -> str:
        return f"TradingRule(symbol='{self.symbol}', min_maker_order_size={self.min_maker_order_size}, " \
               f"min_taker_order_size={self.min_taker_order_size}, price_decimals={self.price_decimals}, "\
               f"amount_decimals={self.amount_decimals}, supports_limit_orders={self.supports_limit_orders}, " \
               f"supports_market_orders={self.supports_market_orders}"

cdef class InFlightOrder:
    cdef:
        public str client_order_id
        public str exchange_order_id
        public str symbol
        public bint is_buy
        public object order_type
        public object amount
        public object price
        public object executed_amount
        public object available_amount
        public object quote_asset_amount
        public object fee_amount
        public str last_state
        public object exchange_order_id_update_event

    def __init__(self,
                 client_order_id: str,
                 exchange_order_id: str,
                 symbol: str,
                 is_buy: bool,
                 order_type: OrderType,
                 amount: Decimal,
                 price: Decimal):
        self.client_order_id = client_order_id
        self.exchange_order_id = exchange_order_id
        self.symbol = symbol
        self.is_buy = is_buy
        self.order_type = order_type
        self.amount = amount
        self.available_amount = amount
        self.price = price
        self.executed_amount = s_decimal_0
        self.quote_asset_amount = s_decimal_0
        self.fee_amount = s_decimal_0
        self.last_state = "NEW"
        self.exchange_order_id_update_event = asyncio.Event()

    def __repr__(self) -> str:
        return f"InFlightOrder(client_order_id='{self.client_order_id}', exchange_order_id='{self.exchange_order_id}', " \
               f"symbol='{self.symbol}', is_buy={self.is_buy}, order_type={self.order_type}, amount={self.amount}, " \
               f"price={self.price}, executed_amount={self.executed_amount}, available_amount={self.available_amount}, " \
               f"quote_asset_amount={self.quote_asset_amount}, fee_amount={self.fee_amount}, " \
               f"last_state='{self.last_state}')"

    @property
    def is_done(self) -> bool: #Filled order, blockchain confirmed
        return self.available_amount == s_decimal_0 and self.last_state == "FILLED" 

    @property
    def is_cancelled(self) -> bool:
        return self.last_state in {"canceled"}

    @property
    def base_asset(self) -> str:
        return self.symbol.split('-')[0]

    @property
    def quote_asset(self) -> str:
        return self.symbol.split('-')[1]

    def update_exchange_order_id(self, exchange_id: str):
        self.exchange_order_id = exchange_id
        self.exchange_order_id_update_event.set()

    async def get_exchange_order_id(self):
        if self.exchange_order_id is None:
            await self.exchange_order_id_update_event.wait()
        return self.exchange_order_id

    def to_limit_order(self) -> LimitOrder:
        cdef:
            str base_currency
            str quote_currency

        base_currency, quote_currency = self.symbol.split("-")
        
        return LimitOrder(
            self.client_order_id,
            self.symbol,
            self.is_buy,
            base_currency,
            quote_currency,
            Decimal(self.price),
            Decimal(self.amount)
        )

    def to_json(self) -> Dict[str, any]:
        return {
            "client_order_id": self.client_order_id,
            "exchange_order_id": self.exchange_order_id,
            "symbol": self.symbol,
            "is_buy": self.is_buy,
            "order_type": self.order_type.name,
            "amount": str(self.amount),
            "available_amount": str(self.available_amount),
            "price": str(self.price),
            "executed_amount": str(self.executed_amount),
            "quote_asset_amount": str(self.quote_asset_amount),
            "fee_amount": str(self.fee_amount),
            "last_state": self.last_state,
        }

    @classmethod
    def from_json(cls, data: Dict[str, any]) -> "InFlightOrder":
        cdef:
            InFlightOrder retval = InFlightOrder(
                data["client_order_id"],
                data["exchange_order_id"],
                data["symbol"],
                data["is_buy"],
                getattr(OrderType, data["order_type"]),
                Decimal(data["amount"]),
                Decimal(data["price"]),
            )
        retval.available_amount = Decimal(data["available_amount"])
        retval.executed_amount = Decimal(data["executed_amount"])
        retval.quote_asset_amount = Decimal(data["quote_asset_amount"])
        retval.fee_amount = Decimal(data["fee_amount"])
        retval.last_state = data["last_state"]
        return retval


ZERO_EX_MAINNET_PROXY = "0xb258f5C190faDAB30B5fF0D6ab7E32a646A4BaAe" #Loopring protocol delegate address


cdef class DolomiteMarket(MarketBase):
    
    MARKET_RECEIVED_ASSET_EVENT_TAG = MarketEvent.ReceivedAsset.value
    MARKET_BUY_ORDER_COMPLETED_EVENT_TAG = MarketEvent.BuyOrderCompleted.value
    MARKET_SELL_ORDER_COMPLETED_EVENT_TAG = MarketEvent.SellOrderCompleted.value
    MARKET_WITHDRAW_ASSET_EVENT_TAG = MarketEvent.WithdrawAsset.value
    MARKET_ORDER_CANCELLED_EVENT_TAG = MarketEvent.OrderCancelled.value
    MARKET_ORDER_FILLED_EVENT_TAG = MarketEvent.OrderFilled.value
    MARKET_ORDER_FAILURE_EVENT_TAG = MarketEvent.OrderFailure.value
    MARKET_BUY_ORDER_CREATED_EVENT_TAG = MarketEvent.BuyOrderCreated.value
    MARKET_SELL_ORDER_CREATED_EVENT_TAG = MarketEvent.SellOrderCreated.value

    API_CALL_TIMEOUT = 10.0
    UPDATE_TRADE_FEES_INTERVAL = 60 * 60
    ORDER_EXPIRY_TIME = 15 * 60.0
    CANCEL_EXPIRY_TIME = 60.0
    
    DOLOMITE_REST_ENDPOINT = "https://exchange-api.dolomite.io"
    

    @classmethod
    def logger(cls) -> HummingbotLogger:
        global s_logger
        if s_logger is None:
            s_logger = logging.getLogger(__name__)
        return s_logger
    
    

    def __init__(self,
                 wallet: Web3Wallet,
                 ethereum_rpc_url: str,
                 poll_interval: float = 5.0,
                 order_book_tracker_data_source_type: OrderBookTrackerDataSourceType =
                    OrderBookTrackerDataSourceType.EXCHANGE_API,
                 wallet_spender_address: str = ZERO_EX_MAINNET_PROXY, 
                 symbols: Optional[List[str]] = None,
                 trading_required: bool = True):
                 
        self.logger().info(f"CREATING DOLOMITE...") #
        
        
        super().__init__()
        
        self._order_book_tracker = DolomiteOrderBookTracker(data_source_type=order_book_tracker_data_source_type,
                                                        symbols=symbols)
        
        self._trading_required = trading_required
        self._account_balances = {}
        self._ev_loop = asyncio.get_event_loop()
        self._poll_notifier = asyncio.Event()
        self._last_timestamp = 0
        self._last_update_order_timestamp = 0
        self._last_update_trading_rules_timestamp = 0
        self._last_update_trade_fees_timestamp = 0
        self._poll_interval = poll_interval
        self._in_flight_orders = {}
        self._in_flight_cancels = OrderedDict()
        self._order_expiry_queue = deque()
        self._tx_tracker = DolomiteMarketTransactionTracker(self)
        self._w3 = Web3(Web3.HTTPProvider(ethereum_rpc_url))
        self._withdraw_rules = {}
        self._trading_rules = {}
        self._pending_approval_tx_hashes = set()
        self._status_polling_task = None
        self._order_tracker_task = None
        self._approval_tx_polling_task = None
        self._wallet = wallet
        self._wallet_spender_address = wallet_spender_address
        self._shared_client = None
        self._maker_trade_fee = NaN
        self._taker_trade_fee = NaN
        self._gas_fee_weth = NaN
        self._gas_fee_usd = NaN
        self._api_response_records = TTLCache(60000, ttl=600.0)
        
        self.logger().info(f"DOLOMITE INITIALIZED!!!") #
        

    @property
    def name(self) -> str:
        return "dolomite"

    @property
    def status_dict(self):
        return {
            "account_balance": len(self._account_balances) > 0 if self._trading_required else True,
            "trading_rule_initialized": len(self._trading_rules) > 0,
            "order_books_initialized": len(self._order_book_tracker.order_books) > 0,
            "token_approval": len(self._pending_approval_tx_hashes) == 0 if self._trading_required else True,
            "maker_trade_fee_initialized": not math.isnan(self._maker_trade_fee),
            "taker_trade_fee_initialized": not math.isnan(self._taker_trade_fee),
            "gas_fee_weth_initialized": not math.isnan(self._gas_fee_weth),
            "gas_fee_usd_initilaized": not math.isnan(self._gas_fee_usd)
        }

    @property
    def ready(self) -> bool:
        return all(self.status_dict.values())

    @property
    def name(self) -> str:
        return "dolomite"

    @property
    def order_books(self) -> Dict[str, OrderBook]:
        return self._order_book_tracker.order_books

    @property
    def wallet(self) -> Web3Wallet:
        return self._wallet

    @property
    def trading_rules(self) -> Dict[str, TradingRule]:
        return self._trading_rules

    @property
    def in_flight_orders(self) -> Dict[str, InFlightOrder]:
        return self._in_flight_orders

    @property
    def limit_orders(self) -> List[LimitOrder]:
        cdef:
            list retval = []
            InFlightOrder typed_in_flight_order

        for in_flight_order in self._in_flight_orders.values():
            typed_in_flight_order = in_flight_order
            if ((typed_in_flight_order.order_type is not OrderType.LIMIT) or
                    typed_in_flight_order.is_done):
                continue
            retval.append(typed_in_flight_order.to_limit_order())

        return retval

    @property
    def expiring_orders(self) -> List[LimitOrder]:
        return [self._in_flight_orders[order_id].to_limit_order()
                for _, order_id
                in self._order_expiry_queue]

    @property
    def tracking_states(self) -> Dict[str, any]:
        return {
            key: value.to_json()
            for key, value in self._in_flight_orders.items()
        }

    def restore_tracking_states(self, saved_states: Dict[str, any]):
        self._in_flight_orders.update({
            key: InFlightOrder.from_json(value)
            for key, value in saved_states.items()
        })

    async def get_active_exchange_markets(self):
        return await DolomiteAPIOrderBookDataSource.get_active_exchange_markets()

    async def _status_polling_loop(self):
        while True:
            try:
                self._poll_notifier = asyncio.Event()
                await self._poll_notifier.wait()

                self._update_balances()
                await asyncio.gather(
                    self._update_trading_rules(),
                    self._update_order_status(),
                    self._update_trade_fees()
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network(
                    "Unexpected error while fetching account and status updates.",
                    exc_info=True,
                    app_warning_msg=f"Failed to fetch account updates on Dolomite. Check network connection."
                )

    def _update_balances(self):
        self._account_balances = self.wallet.get_all_balances()

        
    async def _update_trading_rules(self):
        cdef:
            double current_timestamp = self._current_timestamp

        if current_timestamp - self._last_update_trading_rules_timestamp > 60.0 or len(self._trading_rules) < 1:
            
            markets = await self.list_market()
            
            info_url = f"{self.DOLOMITE_REST_ENDPOINT}/v1/info"
            res = await self._api_request(http_method="get", url=info_url)
            
            min_maker_order = float(res["data"]["min_usd_maker_trade_amount"]["amount"]) / math.pow(10, res["data"]["min_usd_maker_trade_amount"]["currency"]["precision"])
            min_taker_order = float(res["data"]["min_usd_taker_trade_amount"]["amount"]) / math.pow(10, res["data"]["min_usd_taker_trade_amount"]["currency"]["precision"])
            
            
            market = await self.get_market("WETH-DAI")
            
            weth_price = float(market["data"]["last_price_traded"]["amount"]) / math.pow(10, market["data"]["last_price_traded"]["currency"]["precision"])
            
            trading_rules_list = TradingRule.parse_exchange_info(markets, min_maker_order, min_taker_order, weth_price)
            self._trading_rules.clear()
            
            for trading_rule in trading_rules_list:
                self._trading_rules[trading_rule.symbol] = trading_rule
            self._last_update_trading_rules_timestamp = current_timestamp
            
            

            
    
    async def _update_order_status(self):
        cdef:
            double current_timestamp = self._current_timestamp

        if not (current_timestamp - self._last_update_order_timestamp > 10.0 and len(self._in_flight_orders) > 0):
            return

        tracked_orders = list(self._in_flight_orders.values())
        tasks = [self.get_order(o.exchange_order_id)
                 for o in tracked_orders
                 if o.exchange_order_id is not None]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for order_update, tracked_order in zip(results["data"], tracked_orders):
            
            if isinstance(order_update, Exception):
                self.logger().network(
                    f"Error fetching status update for the order {tracked_order.client_order_id}: "
                    f"{order_update}.",
                    app_warning_msg=f"Failed to fetch status update for the order {tracked_order.client_order_id}. "
                                    f"Check Ethereum wallet and network connection."
                )
                continue

            # Check the exchange order ID against the expected value.
            exchange_order_id = order_update["order_hash"]
            if exchange_order_id != tracked_order.exchange_order_id:
                self.logger().network(f"Incorrect exchange order id '{exchange_order_id}' returned from get order "
                                      f"request for '{tracked_order.exchange_order_id}'. Ignoring.")
                
                

                # Capture the incorrect request / response conversation for submitting to Dolomite.
                
                request_url = "%s/v1/orders/addresses/" + Web3Wallet.address % (self.DOLOMITE_REST_ENDPOINT,)
                
                response = self._api_response_records.get(request_url)
                
                found = False
                
                for order in response["data"]:
                    if order["order_hash"] == tracked_order.exchange_order_id:
                        found = True
                        self.logger().network(f"Captured erroneous order update request/response. "
                                              f"Request URL={response.real_url}, "
                                              f"Request headers={response.request_info.headers}, "
                                              f"Response headers={response.headers}, "
                                              f"Response data={repr(response._body)}, "
                                              f"Decoded order update={order_update}.")
                if found == False:
                    self.logger().network(f"Failed to capture the erroneous request/response for getting the order update "
                                          f"of the order {tracked_order.exchange_order_id}.")

                continue
                
                
                

            # Calculate the newly executed amount for this update.
            previous_is_done = tracked_order.is_done
            
            
            #Not blockchain confirmed!
            new_filled_amount = float(order_update["dealt_amount_primary"]["amount"]) / math.pow(10, order_update["dealt_amount_primary"]["currency"]["precision"])
            
            
            #Equals how much more of (primary ticker) amount has been filled 
            execute_amount_diff = new_filled_amount - float(tracked_order.executed_amount)


            
            
            execute_price = 0.0 
            
            if tracked_order.order_type == OrderType.LIMIT:
                execute_price = float(order_update["exchange_rate"])
                
            elif tracked_order.order_type == OrderType.MARKET:
                execute_price = float(order_update["market_order_effective_price"]["amount"]) / math.pow(10, order_update["market_order_effective_price"]["currency"]["precision"])
                
            
            client_order_id = tracked_order.client_order_id
            
            order_type_description = (("market" if tracked_order.order_type == OrderType.MARKET else "limit") +
                                      " " +
                                      ("buy" if tracked_order.is_buy else "sell"))
            
            
            order_type = OrderType.MARKET if tracked_order.order_type == OrderType.MARKET else OrderType.LIMIT
            
            
            # Emit event if executed amount is greater than 0.
            if execute_amount_diff > 0:
                
                fill_size = execute_amount_diff

                order_filled_event = OrderFilledEvent(
                    self._current_timestamp,
                    tracked_order.client_order_id,
                    tracked_order.symbol,
                    TradeType.BUY if tracked_order.is_buy else TradeType.SELL,
                    order_type,
                    execute_price,
                    fill_size,
                    self.c_get_fee(
                        tracked_order.base_asset,
                        tracked_order.quote_asset,
                        order_type,
                        TradeType.BUY if tracked_order.is_buy else TradeType.SELL,
                        fill_size,
                        execute_price)
                )
                
                self.logger().info(f"Filled {fill_size} out of {tracked_order.amount} of the "
                                   f"{order_type_description} order {client_order_id}.")
                self.c_trigger_event(self.MARKET_ORDER_FILLED_EVENT_TAG, order_filled_event)

                
                
            # Update the tracked order
            tracked_order.last_state = order_update["order_status"]
            tracked_order.executed_amount = Decimal(order_update["dealt_amount_primary"]["amount"]) / math.pow(10, order_update["dealt_amount_primary"]["currency"]["precision"])
            tracked_order.available_amount = Decimal(order_update["primary_amount"]["amount"]) / math.pow(10, order_update["primary_amount"]["currency"]["precision"]) - Decimal(order_update["dealt_amount_primary"]["amount"]) / math.pow(10, order_update["dealt_amount_primary"]["currency"]["precision"])
            tracked_order.quote_asset_amount = Decimal(order_update["dealt_amount_secondary"]["amount"]) / math.pow(10, order_update["dealt_amount_secondary"]["currency"]["precision"])      
            tracked_order.fee_amount = Decimal(order_update["fee_usd_at_close"]["amount"]) / math.pow(10, order_update["fee_usd_at_close"]["currency"]["precision"]) #null if not filled 
            
            
            
            if not previous_is_done and tracked_order.is_done:
                executed_amount = float(tracked_order.executed_amount)
                quote_asset_amount = float(tracked_order.quote_asset_amount)
                if not tracked_order.is_cancelled:
                    if tracked_order.is_buy:
                        self.logger().info(f"The {order_type_description} order {client_order_id} has "
                                           f"completed according to order status API.")

                        self.c_trigger_event(self.MARKET_BUY_ORDER_COMPLETED_EVENT_TAG,
                                             BuyOrderCompletedEvent(self._current_timestamp,
                                                                    tracked_order.client_order_id,
                                                                    tracked_order.base_asset,
                                                                    tracked_order.quote_asset,
                                                                    tracked_order.quote_asset,
                                                                    executed_amount,
                                                                    quote_asset_amount,
                                                                    float(tracked_order.fee_amount),
                                                                    order_type))
                    else:
                        self.logger().info(f"The {order_type_description} order {client_order_id} has "
                                           f"completed according to order status API.")
                        self.c_trigger_event(self.MARKET_SELL_ORDER_COMPLETED_EVENT_TAG,
                                             SellOrderCompletedEvent(self._current_timestamp,
                                                                     tracked_order.client_order_id,
                                                                     tracked_order.base_asset,
                                                                     tracked_order.quote_asset,
                                                                     tracked_order.quote_asset,
                                                                     executed_amount,
                                                                     quote_asset_amount,
                                                                     float(tracked_order.fee_amount),
                                                                     order_type))
               
            
            else:
                    if (self._in_flight_cancels.get(client_order_id, 0) >
                            self._current_timestamp - self.CANCEL_EXPIRY_TIME):
                        # This cancel was originated from this connector, and the cancel event should have been
                        # emitted in the cancel_order() call already.
                        del self._in_flight_cancels[client_order_id]
                    else:
                        # This cancel was originated externally.
                        self.logger().info(f"The {order_type_description} order {client_order_id} has been cancelled.")
                        self.c_trigger_event(self.MARKET_ORDER_CANCELLED_EVENT_TAG,
                                     OrderCancelledEvent(self._current_timestamp, client_order_id))
            self.c_expire_order(tracked_order.client_order_id)

        self._last_update_order_timestamp = current_timestamp
        
        
        

    async def _approval_tx_polling_loop(self):
        while len(self._pending_approval_tx_hashes) > 0:
            try:
                if len(self._pending_approval_tx_hashes) > 0:
                    for tx_hash in list(self._pending_approval_tx_hashes):
                        receipt = self._w3.eth.getTransactionReceipt(tx_hash)
                        if receipt is not None:
                            self._pending_approval_tx_hashes.remove(tx_hash)
            except Exception:
                self.logger().network(
                    "Unexpected error while fetching approval transactions.",
                    exc_info=True,
                    app_warning_msg="Could not get token approval status. "
                                    "Check Ethereum wallet and network connection."
                )
            finally:
                await asyncio.sleep(1.0)
                
                

    def _generate_auth_headers(self) -> Dict:
        message = "LOOPRING-AUTHENTICATION@%s" % (int(time.time() * 1000),)
        signature = self.wallet.current_backend.sign_hash(text=message)
        auth = "%s#%s#%s" % (self.wallet.address.lower(), message, signature)
        headers = {"Loopring-Authentication": auth}
        return headers
    

    async def _http_client(self) -> aiohttp.ClientSession:
        if self._shared_client is None:
            self._shared_client = aiohttp.ClientSession()
        return self._shared_client
    

    async def _api_request(self,
                           http_method: str,
                           url: str,
                           data: Optional[Dict[str, Any]] = None,
                           params: Optional[Dict[str, Any]] = None,
                           headers: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        client = await self._http_client()
        async with client.request(http_method, url=url, timeout=self.API_CALL_TIMEOUT, data=data, params=params,
                                  headers=headers) as response:
            if response.status != 200:
                raise IOError(f"Error fetching data from {url}. HTTP status is {response.status}.")
            data = await response.json()

            # Keep an auto-expired record of the response and the request URL for debugging and logging purpose.
            self._api_response_records[url] = response

            return data
            
            
            
            
    async def calculateGasFees(self, quoteToken):
        
        fee_url = f"{self.DOLOMITE_REST_ENDPOINT}/v1/info"
        
        res = await self._api_request(http_method="get", url=fee_url)
        
        
        feeData = res["data"]["base_spot_trading_fee_amounts"] #find by quoteToken
        
        baseAmount = float(feeData[quoteToken]["amount"]) / math.pow(10, feeData[quoteToken]["currency"]["precision"])
        
        maxMatches = res["data"]["max_number_of_taker_matches_per_order"]
        
        takerGasFees = baseAmount * maxMatches
        
        return takerGasFees
        
        

    async def _update_trade_fees(self):
        cdef:
            double current_timestamp = self._current_timestamp

        if current_timestamp - self._last_update_trade_fees_timestamp > self.UPDATE_TRADE_FEES_INTERVAL or self._gas_fee_usd == NaN:
            
            
            calc_fee_url = f"{self.DOLOMITE_REST_ENDPOINT}/v1/info"
            
            
            res = await self._api_request(http_method="get", url=calc_fee_url)
            
            self._maker_trade_fee = float(res["data"]["maker_fee_percentage"])
            self._taker_trade_fee = float(res["data"]["taker_fee_percentage"])
            
            
            self._gas_fee_weth = await self.calculateGasFees("WETH")
            self._gas_fee_usd = await self.calculateGasFees("DAI")
            
            
            self._last_update_trade_fees_timestamp = current_timestamp
            

    cdef object c_get_fee(self,
                          str base_currency,
                          str quote_currency,
                          object order_type,
                          object order_side,
                          double amount,
                          double price):
        cdef:
            double gas_fee = 0.0 #Upper bound? 
            double percent
            
        if quote_currency == "WETH":
            gas_fee = self._gas_fee_weth
        elif quote_currency == "DAI":
            gas_fee = self._gas_fee_usd
        else:
            self.logger().warning(
                f"Unrecognized quote token symbol - {quote_currency}. Assuming gas fee is in stable coin units."
            )
            gas_fee = NaN 
            
            
        percent = self._maker_trade_fee if order_type is OrderType.LIMIT else self._taker_trade_fee 
        
        if order_type is OrderType.LIMIT:
            return TradeFee(percent) #maker
        
        else:
            return TradeFee(percent, flat_fees = [(quote_currency, gas_fee)]) #taker 
        
    
    
    
    async def fetch_token_addresses(self, ticker):
    
        fetch_url = "%s/v1/tokens" % (self.DOLOMITE_REST_ENDPOINT,)
        response = await self._api_request(http_method="get", url=fetch_url)
        
        tokens = response.get("data")
                    
        for token in tokens:
                    	
            symbol = token["ticker"]

            if symbol == ticker:
                
               return token["identifier"]
               
        return None 
                    
                     
        

    async def build_unsigned_order(self, amount: str, price: str, side: str, symbol: str, order_type: OrderType,
                                   expires: int) -> Dict[str, Any]:
        
        
        url = "%s/v1/orders/hash" % (self.DOLOMITE_REST_ENDPOINT,)
        
        newAccount = self._w3.eth.account.create() #new auth address / private key 
        
        
        baseToken = symbol.split('-')[0]
        quoteToken = symbol.split('-')[1]
        
        
        marketData = await self.get_market(symbol)
        
        primaryPrecision = marketData["data"]["period_amount"]["currency"]["precision"]
        secondaryPrecision = marketData["data"]["period_volume"]["currency"]["precision"]
        
        
        secondaryAmount = 0
        
        if order_type is OrderType.MARKET and side == "buy":
            secondaryAmount = 1000000000000 * math.pow(10, secondaryPrecision)
            
        elif order_type is OrderType.MARKET and side == "sell":
            secondaryAmount = 1e-18 * math.pow(10, secondaryPrecision)  
        else:
            secondaryAmount = int((float(amount) * float(price)) * math.pow(10, secondaryPrecision))
            
            
            
        #Set fee amount for takers 
        
        fee_url = f"{self.DOLOMITE_REST_ENDPOINT}/v1/info"
        
        
        res = await self._api_request(http_method="get", url=fee_url)
        
        
        #Gas fees 
        
        feeData = res["data"]["base_spot_trading_fee_amounts"] #find by quoteToken
        
        baseAmount = float(feeData[quoteToken]["amount"]) / math.pow(10, feeData[quoteToken]["currency"]["precision"])
        maxMatches = res["data"]["max_number_of_taker_matches_per_order"]
        
        takerGasFees = baseAmount * maxMatches
        
        
        #Commission fees
        
        market_url = f"{self.DOLOMITE_REST_ENDPOINT}/v1/markets/{symbol}"
        
        res2 = await self._api_request(http_method="get", url=market_url)
        
        marketPrice = float(res2["data"]["last_price_traded"]["amount"]) / math.pow(10, res2["data"]["last_price_traded"]["currency"]["precision"])
        
        takerRate = float(res["data"]["taker_fee_percentage"])
        
        
        #if marketPrice == 0.0 
        
        commissionAmount = marketPrice * takerRate * float(amount)
        
        #Total 
        totalTakerFee = takerGasFees + commissionAmount #max amount - set upperbound?

        
        #fee address
        feeAddress = res["data"]["fee_collecting_wallet_address"]
        
        data = {
            
            "fee_collecting_wallet_address": feeAddress, 
            "wallet_split_percentage": 0,
            "owner_address": "0xaFF638fE88eE221e41cB27E7753C855AB4047aEd", #Web3Wallet.address?
            "auth_address": str(newAccount.address),
            "order_side": side.upper(),
            "order_type": "MARKET" if order_type is OrderType.MARKET else "LIMIT",
            "auth_private_key": str(binascii.hexlify(newAccount.privateKey).decode("utf-8")),
            "market": await self.fetch_token_addresses(baseToken) + "-" + await self.fetch_token_addresses(quoteToken),
            "primary_padded_amount": str(int(float(amount) * math.pow(10, primaryPrecision))),
            "secondary_padded_amount": str(secondaryAmount),
            "creation_timestamp": (int(time.time())-5) * 1000,
            "expiration_timestamp": (int(time.time()-5) * 1000) + (expires*1000),
            "fee_padded_amount": str((totalTakerFee * math.pow(10, secondaryPrecision))) if order_type is OrderType.MARKET else "0000000000000000000",
            "fee_token_address": await self.fetch_token_addresses(quoteToken),
            "dependent_transaction_hash": None,
            "max_number_of_taker_matches": 16 if order_type is OrderType.MARKET else 0,
            "base_taker_gas_fee_padded_amount": str(feeData[quoteToken]["amount"]) if order_type is OrderType.MARKET else "0000000000000000000",
            "order_recipient_address": None,
            "extra_data": None,
            "order_hash": None,
            "ecdsa_multi_hash_signature": None
  
        }
        
        
        self.logger().info(f"{data}")
        
        jData = json.dumps(data).encode('utf8')
        headers = {"Content-Type": "application/json"}

        response_data = await self._api_request('post', url=url, data=jData, headers=headers)
        
        self.logger().info(f"FINAL RESPONSE DATA: {response_data}")
        
        values = [data, response_data]
        
        return values
    
    
    

    async def place_order(self, amount: str, price: str, side: str, symbol: str, order_type: OrderType,
                          expires: int = 0) -> Dict[str, Any]:
        
        
        unsigned_order = await self.build_unsigned_order(symbol=symbol, amount=amount, price=price, side=side,
                                                         order_type=order_type, expires=expires)
        
        
        
        
        order_hash = unsigned_order[1]["data"]["order_hash"]
        
        signature = self.wallet.current_backend.sign_hash(hexstr=order_hash)
        

        url = "%s/v1/orders/create" % (self.DOLOMITE_REST_ENDPOINT,)

        
        response_data = {
            
            "ecdsa_multi_hash_signature": signature,
            "order_hash": order_hash
            
        }
        
        data = {**response_data, **unsigned_order[0]}
        
        
        jData = json.dumps(data).encode('utf8')
        headers = {"Content-Type": "application/json"}

        response_data = await self._api_request('post', url=url, data=jData, headers=headers)
        
        return response_data
    

    

    async def cancel_order(self, client_order_id: str) -> Dict[str, Any]:
        cdef:
            InFlightOrder order = self.in_flight_orders.get(client_order_id)

        if not order:
            self.logger().info(f"Failed to cancel order {client_order_id}. Order not found in tracked orders.")
            if client_order_id in self._in_flight_cancels:
                del self._in_flight_cancels[client_order_id]
            return {}

        
        exchange_order_id = await order.get_exchange_order_id() 
        
        orderInfo = await self.get_order(exchange_order_id)
        
        dolomite_id = orderInfo["data"]["dolomite_order_id"]
        
        
        url = "%s/v1/orders/%s/cancel" % (self.DOLOMITE_REST_ENDPOINT, dolomite_id)
        
        
        message = str(int(time.time() * 1000))
        signature = self.wallet.current_backend.sign_hash(text=message) 
        
        data = {
            
            "owner_address": Web3Wallet.address,
            "ecdsa_signature": signature, 
            "cancellation_timestamp": int(time.time()) * 1000
        }
        
        
        jData = json.dumps(data).encode('utf8')
        

        headers = {"Content-Type": "application/json"}

        response_data = await self._api_request('post', url=url, data=jData, headers=headers)
        
        
        if isinstance(response_data, dict):
            self.logger().info(f"Successfully cancelled order {exchange_order_id}.")

            # Simulate cancelled state earlier.
            order.available_amount = s_decimal_0

            # Notify listeners.
            self.c_trigger_event(self.MARKET_ORDER_CANCELLED_EVENT_TAG,
                                 OrderCancelledEvent(self._current_timestamp, client_order_id))

        response_data["client_order_id"] = client_order_id
        return response_data
    
    
    
    
    

    async def list_orders(self) -> Dict[str, Any]:
        url = "%s/v1/orders/addresses/" + Web3Wallet.address % (self.DOLOMITE_REST_ENDPOINT,)
        response_data = await self._api_request('get', url=url, headers=self._generate_auth_headers())
        return response_data
    
    

    async def get_order(self, order_id: str) -> Dict[str, Any]:
    
        orderList = await self.list_orders() 
        
        for order in orderList["data"]:
            if order["order_hash"] == order_id:
                return order
            
        raise IOError(f"Error fetching data from url.") 
    

    async def get_market(self, symbol: str) -> Dict[str, Any]:
        
        url = "%s/v1/markets/%s" % (self.DOLOMITE_REST_ENDPOINT, symbol)
        response_data = await self._api_request('get', url=url)
        return response_data
    
        

    async def list_market(self) -> Dict[str, Any]:
        
        url = "%s/v1/markets" % (self.DOLOMITE_REST_ENDPOINT,)
        response_data = await self._api_request('get', url=url)
        return response_data
    
    
    
    
    cdef str c_buy(self, str symbol, double amount, object order_type = OrderType.MARKET, double price = 0,
                   dict kwargs = {}):
        cdef:
            int64_t tracking_nonce = <int64_t>(time.time() * 1e6)
            str order_id = str(f"buy-{symbol}-{tracking_nonce}")

        asyncio.ensure_future(self.execute_buy(order_id, symbol, amount, order_type, price))
        return order_id

    async def execute_buy(self, order_id: str, symbol: str, amount: float, order_type: OrderType, price: float) -> str:
        cdef:
            str q_price = str(self.c_quantize_order_price(symbol, price))
            str q_amt = str(self.c_quantize_order_amount(symbol, amount))
            TradingRule trading_rule = self._trading_rules[symbol]
            double quote_amount 

        try:
            if order_type is OrderType.LIMIT:
                if float(q_amt) < trading_rule.min_maker_order_size or amount < trading_rule.min_maker_order_size:
                    raise ValueError(f"Buy order amount {amount} is lower than the minimum order size")
            else:
                if amount < trading_rule.min_taker_order_size or amount < trading_rule.min_taker_order_size:
                    raise ValueError(f"Buy order amount {amount} is lower than the minimum order size")

            if order_type is OrderType.LIMIT and trading_rule.supports_limit_orders is False:
                raise ValueError(f"Limit order is not supported for trading pair {symbol}")
            if order_type is OrderType.MARKET and trading_rule.supports_market_orders is False:
                raise ValueError(f"Market order is not supported for trading pair {symbol}")

            self.c_start_tracking_order(order_id, symbol, True, order_type, Decimal(q_amt), Decimal(q_price))
            
            order_result = await self.place_order(amount=q_amt, price=q_price, side="buy", symbol=symbol,
                                                  order_type=order_type)
            
            
            exchange_order_id = order_result["data"]["order_hash"]
            tracked_order = self._in_flight_orders.get(order_id)
            if tracked_order is not None:
                self.logger().info(f"Created {order_type} buy order {exchange_order_id} for "
                                   f"{q_amt} {symbol}.")
                tracked_order.update_exchange_order_id(exchange_order_id)
            self.c_trigger_event(self.MARKET_BUY_ORDER_CREATED_EVENT_TAG,
                                 BuyOrderCreatedEvent(
                                     self._current_timestamp,
                                     order_type,
                                     symbol,
                                     float(q_amt),
                                     float(q_price),
                                     order_id
                                 ))
            return order_id
        
        
        except Exception:
            self.c_stop_tracking_order(order_id)
            self.logger().network(
                f"Error submitting buy order to Dolomite for {amount} {symbol}.",
                exc_info=True,
                app_warning_msg=f"Failed to submit buy order to Dolomite. "
                                f"Check Ethereum wallet and network connection."
            )
            self.c_trigger_event(self.MARKET_ORDER_FAILURE_EVENT_TAG,
                                 MarketOrderFailureEvent(self._current_timestamp,
                                                         order_id,
                                                         order_type)
                                 )
            
            

    cdef str c_sell(self, str symbol, double amount, object order_type = OrderType.MARKET, double price = 0,
                    dict kwargs = {}):
        cdef:
            int64_t tracking_nonce = <int64_t>(time.time() * 1e6)
            str order_id = str(f"sell-{symbol}-{tracking_nonce}")

        asyncio.ensure_future(self.execute_sell(order_id, symbol, amount, order_type, price))
        return order_id

    async def execute_sell(self, order_id: str, symbol: str, amount: float, order_type: OrderType, price: float) -> str:
        cdef:
            str q_price = str(self.c_quantize_order_price(symbol, price))
            str q_amt = str(self.c_quantize_order_amount(symbol, amount))
            TradingRule trading_rule = self._trading_rules[symbol]

        try:
                
            if order_type is OrderType.LIMIT:
                if float(q_amt) < trading_rule.min_maker_order_size or amount < trading_rule.min_maker_order_size:
                    raise ValueError(f"Sell order amount {amount} is lower than the minimum order size ")
            else:
                if amount < trading_rule.min_taker_order_size or amount < trading_rule.min_taker_order_size:
                    raise ValueError(f"Sell order amount {amount} is lower than the minimum order size ")
                
                
            if order_type is OrderType.LIMIT and trading_rule.supports_limit_orders is False:
                raise ValueError(f"Limit order is not supported for trading pair {symbol}")
            if order_type is OrderType.MARKET and trading_rule.supports_market_orders is False:
                raise ValueError(f"Market order is not supported for trading pair {symbol}")

            self.c_start_tracking_order(order_id, symbol, False, order_type, Decimal(q_amt), Decimal(q_price))
            order_result = await self.place_order(amount=q_amt, price=q_price, side="sell", symbol=symbol,
                                                  order_type=order_type)
            
            
            exchange_order_id = order_result["data"]["order_hash"]
            tracked_order = self._in_flight_orders.get(order_id)
            if tracked_order is not None:
                self.logger().info(f"Created {order_type} sell order {exchange_order_id} for "
                                   f"{q_amt} {symbol}.")
                tracked_order.update_exchange_order_id(exchange_order_id)
            self.c_trigger_event(self.MARKET_SELL_ORDER_CREATED_EVENT_TAG,
                                 SellOrderCreatedEvent(
                                     self._current_timestamp,
                                     order_type,
                                     symbol,
                                     float(q_amt),
                                     float(q_price),
                                     order_id
                                 ))
            return order_id
        except Exception:
            self.c_stop_tracking_order(order_id)
            self.logger().network(
                f"Error submitting sell order to Dolomite for {amount} {symbol}.",
                exc_info=True,
                app_warning_msg=f"Failed to submit sell order to Dolomite. "
                                f"Check Ethereum wallet and network connection."
            )
            self.c_trigger_event(self.MARKET_ORDER_FAILURE_EVENT_TAG,
                                 MarketOrderFailureEvent(self._current_timestamp,
                                                         order_id,
                                                         order_type)
                                 )
            
            

    cdef c_cancel(self, str symbol, str client_order_id):
        # If there's an ongoing cancel on this order within the expiry time, don't do it again.
        if self._in_flight_cancels.get(client_order_id, 0) > self._current_timestamp - self.CANCEL_EXPIRY_TIME:
            return

        # Maintain the in flight orders list vs. expiry invariant.
        cdef:
            list keys_to_delete = []

        for k, cancel_timestamp in self._in_flight_cancels.items():
            if cancel_timestamp < self._current_timestamp - self.CANCEL_EXPIRY_TIME:
                keys_to_delete.append(k)
            else:
                break
        for k in keys_to_delete:
            del self._in_flight_cancels[k]

        # Record the in-flight cancellation.
        self._in_flight_cancels[client_order_id] = self._current_timestamp

        # Execute the cancel asynchronously.
        asyncio.ensure_future(self.cancel_order(client_order_id))

    async def cancel_all(self, timeout_seconds: float) -> List[CancellationResult]:
        incomplete_orders = [o for o in self.in_flight_orders.values() if not o.is_done]
        tasks = [self.cancel_order(o.client_order_id) for o in incomplete_orders]
        order_id_set = set([o.client_order_id for o in incomplete_orders])
        successful_cancellations = []

        try:
            async with timeout(timeout_seconds):
                cancellation_results = await asyncio.gather(*tasks, return_exceptions=True)
                for cr in cancellation_results:
                    if isinstance(cr, Exception):
                        continue
                    if isinstance(cr, dict):
                        client_order_id = cr.get("client_order_id")
                        order_id_set.remove(client_order_id)
                        successful_cancellations.append(CancellationResult(client_order_id, True))
        except Exception:
            self.logger().network(
                f"Unexpected error cancelling orders.",
                exc_info=True,
                app_warning_msg=f"Failed to cancel orders on Dolomite. Check Ethereum wallet and network connection."
            )

        failed_cancellations = [CancellationResult(oid, False) for oid in order_id_set]
        return successful_cancellations + failed_cancellations
    
    

    def get_all_balances(self) -> Dict[str, float]:
        return self._account_balances.copy()

    def get_balance(self, currency: str) -> float:
        return self.c_get_balance(currency)

    def get_price(self, symbol: str, is_buy: bool) -> float:
        return self.c_get_price(symbol, is_buy)

    def wrap_eth(self, amount: float) -> str:
        return self._wallet.wrap_eth(amount)

    def unwrap_eth(self, amount: float) -> str:
        return self._wallet.unwrap_eth(amount)

    cdef double c_get_balance(self, str currency) except? -1:
        return float(self._account_balances.get(currency, 0.0))

    cdef OrderBook c_get_order_book(self, str symbol):
        cdef:
            dict order_books = self._order_book_tracker.order_books

        if symbol not in order_books:
            raise ValueError(f"No order book exists for '{symbol}'.")
        return order_books[symbol]

    cdef double c_get_price(self, str symbol, bint is_buy) except? -1:
        cdef:
            OrderBook order_book = self.c_get_order_book(symbol)

        return order_book.c_get_price(is_buy)

    async def start_network(self):
        if self._order_tracker_task is not None:
            self._stop_network()

        self._order_tracker_task = asyncio.ensure_future(self._order_book_tracker.start())
        self._status_polling_task = asyncio.ensure_future(self._status_polling_loop())
        if self._trading_required:
            tx_hashes = await self.wallet.current_backend.check_and_fix_approval_amounts(
                spender=self._wallet_spender_address
            )
            self._pending_approval_tx_hashes.update(tx_hashes)
            self._approval_tx_polling_task = asyncio.ensure_future(self._approval_tx_polling_loop())

    def _stop_network(self):
        if self._order_tracker_task is not None:
            self._order_tracker_task.cancel()
            self._status_polling_task.cancel()
            self._pending_approval_tx_hashes.clear()
            self._approval_tx_polling_task.cancel()
        self._order_tracker_task = self._status_polling_task = self._approval_tx_polling_task = None

    async def stop_network(self):
        self._stop_network()
        if self._shared_client is not None:
            await self._shared_client.close()
            self._shared_client = None

    async def check_network(self) -> NetworkStatus:
        if self._wallet.network_status is not NetworkStatus.CONNECTED:
            return NetworkStatus.NOT_CONNECTED

        url = f"{self.DOLOMITE_REST_ENDPOINT}/v1/markets"
        try:
            await self._api_request("GET", url)
        except asyncio.CancelledError:
            raise
        except Exception:
            return NetworkStatus.NOT_CONNECTED
        return NetworkStatus.CONNECTED

    cdef c_tick(self, double timestamp):
        cdef:
            int64_t last_tick = <int64_t>(self._last_timestamp / self._poll_interval)
            int64_t current_tick = <int64_t>(timestamp / self._poll_interval)

        self._tx_tracker.c_tick(timestamp)
        MarketBase.c_tick(self, timestamp)
        if current_tick > last_tick:
            if not self._poll_notifier.is_set():
                self._poll_notifier.set()
        self.c_check_and_remove_expired_orders()
        self._last_timestamp = timestamp

    cdef c_start_tracking_order(self,
                                str client_order_id,
                                str symbol,
                                bint is_buy,
                                object order_type,
                                object amount,
                                object price):
        self._in_flight_orders[client_order_id] = InFlightOrder(client_order_id, None, symbol, is_buy,
                                                                order_type, amount, price)

    cdef c_expire_order(self, str order_id):
        self._order_expiry_queue.append((self._current_timestamp + self.ORDER_EXPIRY_TIME, order_id))

    cdef c_check_and_remove_expired_orders(self):
        cdef:
            double current_timestamp = self._current_timestamp
            str order_id

        while len(self._order_expiry_queue) > 0 and self._order_expiry_queue[0][0] < current_timestamp:
            _, order_id = self._order_expiry_queue.popleft()
            self.c_stop_tracking_order(order_id)

    cdef c_stop_tracking_order(self, str order_id):
        if order_id in self._in_flight_orders:
            del self._in_flight_orders[order_id]

            
    cdef object c_get_order_price_quantum(self, str symbol, double price):
        cdef:
            TradingRule trading_rule = self._trading_rules[symbol]
        decimals_quantum = Decimal(f"1e-{trading_rule.price_decimals}")
        
        return decimals_quantum
    

    cdef object c_get_order_size_quantum(self, str symbol, double amount):
        cdef:
            TradingRule trading_rule = self._trading_rules[symbol]
        decimals_quantum = Decimal(f"1e-{trading_rule.amount_decimals}")
        return decimals_quantum

    
    cdef object c_quantize_order_amount(self, str symbol, double amount, double price=0):
        cdef:
            TradingRule trading_rule = self._trading_rules[symbol]
        global s_decimal_0


        quantized_amount = MarketBase.c_quantize_order_amount(self, symbol, amount)

        return quantized_amount