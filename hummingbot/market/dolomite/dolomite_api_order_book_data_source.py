#!/usr/bin/env python

import asyncio
import aiohttp
import logging
import pandas as pd
import math 

from typing import (
    AsyncIterable,
    Dict,
    List,
    Optional
    
    
)


import re
import time
import ujson 
import json
import urllib.request
import time
import websockets
from websockets.exceptions import ConnectionClosed


from hummingbot.core.utils import async_ttl_cache
from hummingbot.market.dolomite.dolomite_active_order_tracker import DolomiteActiveOrderTracker 
from hummingbot.market.dolomite.dolomite_order_book import DolomiteOrderBook 
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.logger import HummingbotLogger

from hummingbot.core.data_type.order_book_tracker_entry import (
    DolomiteOrderBookTrackerEntry,
    OrderBookTrackerEntry
)

from hummingbot.core.data_type.order_book_message import DolomiteOrderBookMessage 


REST_URL = "https://exchange-api.dolomite.io"

WS_URL = "wss://exchange-api.dolomite.io/ws-connect"


TICKERS_URL = f"{REST_URL}/v1/markets"
MARKETS_URL = f"{REST_URL}/v1/markets"



class DolomiteAPIOrderBookDataSource(OrderBookTrackerDataSource):

    MESSAGE_TIMEOUT = 30.0
    PING_TIMEOUT = 10.0

    _raobds_logger: Optional[HummingbotLogger] = None

    
    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._raobds_logger is None:
            cls._raobds_logger = logging.getLogger(__name__)
        return cls._raobds_logger
    

    def __init__(self, symbols: Optional[List[str]] = None):
        super().__init__()
        self._symbols: Optional[List[str]] = symbols
        self._get_tracking_pair_done_event: asyncio.Event = asyncio.Event()
            
            

    @classmethod
    @async_ttl_cache(ttl=60 * 30, maxsize=1)
    async def get_active_exchange_markets(cls) -> pd.DataFrame:
        """
        Returned data frame should have symbol as index and include usd volume, baseAsset and quoteAsset
        """
        async with aiohttp.ClientSession() as client:
            market_response, ticker_response = await asyncio.gather(
                #identical
                client.get(MARKETS_URL), 
                client.get(TICKERS_URL)
            )
            
            market_response: aiohttp.ClientResponse = market_response
            ticker_response: aiohttp.ClientResponse = ticker_response

            if market_response.status != 200:
                raise IOError(f"Error fetching active Dolomite markets. HTTP status is {market_response.status}.")
            if ticker_response.status != 200:
                raise IOError(f"Error fetching active Dolomite Ticker. HTTP status is {ticker_response.status}.")

            ticker_data = await ticker_response.json()
            market_data = await market_response.json()

            attr_name_map = {"baseToken": "baseAsset", "quoteToken": "quoteAsset"}  

            market_data: Dict[str, any] = {
                item["market"]: {attr_name_map[k]: item[k] for k in ["baseToken", "quoteToken"]}
                for item in market_data["data"]
            }
                

            ticker_data: List[Dict[str, any]] = [
                                                {**ticker_item, **market_data[ticker_item["market"]]}
                                                 for ticker_item in ticker_data["data"]
                                                 if ticker_item["market"] in market_data]
                

            all_markets: pd.DataFrame = pd.DataFrame.from_records(data=ticker_data,
                                                                  index="market") 


            weth_to_usd_price: float = float(all_markets.loc["WETH-DAI"].last_price_traded["amount"]) / math.pow(10, all_markets.loc["WETH-DAI"].last_price_traded["currency"]["precision"])
                
                
            usd_volume: float = [
                (
                    quoteVolume * weth_to_usd_price if symbol.endswith("WETH") else quoteVolume
                )
                
                for symbol, quoteVolume in zip(all_markets.index,
                                               float(all_markets.period_amount["amount"] / all_markets.period_amount["currency"]["precision"]))]
                
                
            all_markets["USDVolume"] = usd_volume
            return all_markets.sort_values("USDVolume", ascending=False)
 
   
    @property
    def order_book_class(self) -> DolomiteOrderBook: 
        return DolomiteOrderBook

    
    async def get_trading_pairs(self) -> List[str]:
        if self._symbols is None:
            active_markets: pd.DataFrame = await self.get_active_exchange_markets() #
            trading_pairs: List[str] = active_markets.index.tolist()
            self._symbols = trading_pairs
        else:
            trading_pairs: List[str] = self._symbols
        return trading_pairs
    
    

    async def get_snapshot(self, client: aiohttp.ClientSession, trading_pair: str, level: int = 3) -> Dict[str, any]:
            params: Dict = {"level": level}
            retry: int = 3
            while retry > 0:

                try:
                    async with client.get(f"{REST_URL}/v1/orders/markets/{trading_pair}/depth/unmerged") as response:
                        response: aiohttp.ClientResponse = response
                        if response.status != 200:
                            raise IOError(f"Error fetching Dolomite market snapshot for {trading_pair}. "
                                          f"HTTP status is {response.status}.")
                        data: Dict[str, any] = await response.json()
                        return data
                except Exception:
                    self.logger().error(f"Error getting snapshot for {trading_pair}. Retrying {retry} more times.",
                                        exc_info=True)
                    await asyncio.sleep(10)
                    retry -= 1
                    if retry == 0:
                        raise
                        

    async def get_tracking_pairs(self) -> Dict[str, OrderBookTrackerEntry]:
        # Get the currently active markets
        async with aiohttp.ClientSession() as client:
            trading_pairs: List[str] = await self.get_trading_pairs()
            retval: Dict[str, DolomiteOrderBookTrackerEntry] = {}
            number_of_pairs: int = len(trading_pairs)
            for index, trading_pair in enumerate(trading_pairs):
                try:
                    snapshot: Dict[str, any] = await self.get_snapshot(client, trading_pair, 3)
                    snapshot_timestamp: float = time.time()
                        
                    snapshot_msg: DolomiteOrderBookMessage = self.order_book_class.snapshot_message_from_exchange(
                        snapshot,
                        snapshot_timestamp,
                        {"market": trading_pair}
                    )

                    dolomite_order_book: DolomiteOrderBook = DolomiteOrderBook()
                    dolomite_active_order_tracker: DolomiteActiveOrderTracker = DolomiteActiveOrderTracker()
                    bids, asks = dolomite_active_order_tracker.convert_snapshot_message_to_order_book_row(snapshot_msg)
                    dolomite_order_book.apply_snapshot(bids, asks, snapshot_msg.update_id) 

                    
                    retval[trading_pair] = DolomiteOrderBookTrackerEntry(
                        trading_pair,
                        snapshot_timestamp,
                        dolomite_order_book,
                        dolomite_active_order_tracker
                    )
                    

                    self.logger().info(f"Initialized order book for {trading_pair}. "
                                       f"{index+1}/{number_of_pairs} completed.")

                    await asyncio.sleep(5.0) 

                except Exception:
                    self.logger().error(f"Error getting snapshot for {trading_pair} in get_tracking_pairs.",
                                        exc_info=True)
                    await asyncio.sleep(5)

            self._get_tracking_pair_done_event.set()
            return retval
        
        

    async def _inner_messages(self,
                              ws: websockets.WebSocketClientProtocol) -> AsyncIterable[str]:
        # Terminate the recv() loop as soon as the next message timed out, so the outer loop can reconnect.
        try:
            while True:
                try:
                    msg: str = await asyncio.wait_for(ws.recv(), timeout=self.MESSAGE_TIMEOUT)
                    yield msg
                except asyncio.TimeoutError:
                    try:
                        pong_waiter = await ws.ping()
                        await asyncio.wait_for(pong_waiter, timeout=self.PING_TIMEOUT)
                    except asyncio.TimeoutError:
                        raise
        except asyncio.TimeoutError:
            self.logger().warning("WebSocket ping timed out. Going to reconnect...")
            return
        except ConnectionClosed:
            return
        finally:
            await ws.close()
            
            

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        pass

                

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):

        await self._get_tracking_pair_done_event.wait()

        try:
            trading_pairs: List[str] = await self.get_trading_pairs()

            async with websockets.connect(WS_URL) as ws:
                ws: websockets.WebSocketClientProtocol = ws

                for trading_pair in trading_pairs:

                    self.logger().info(f"PAIR: {trading_pair}")

                    request = { #Subscribe to each market's order book...
                        "action": "subscribe",
                        "data": { "market": trading_pair },
                        "route": f"/v1/orders/markets/-market-/depth/unmerged" 
                    }
              
                    await ws.send(ujson.dumps(request))

                    async for raw_msg in self._inner_messages(ws):                
                        msg = ujson.loads(raw_msg)

                        #self.logger().info(f"DATA: {msg}")

                        if msg["route"] == "/v1/orders/markets/-market-/depth/unmerged" and msg["action"] == "update":

                            snapshot_timestamp: float = time.time()

                            snapshot_msg: DolomiteOrderBookMessage = self.order_book_class.snapshot_message_from_exchange(
                                msg,
                                snapshot_timestamp,
                                {"market": trading_pair}
                            )

                            output.put_nowait(snapshot_msg)
                            self.logger().debug(f"Saved order book snapshot for {trading_pair} at {snapshot_timestamp}")


        except asyncio.CancelledError:
            raise
        except Exception:
                self.logger().error("Unexpected error.", exc_info=True)
                await asyncio.sleep(5.0)