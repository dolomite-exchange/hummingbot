from decimal import Decimal
from typing import (
    List,
    Tuple,
)

from hummingbot.strategy.market_symbol_pair import MarketSymbolPair
from hummingbot.strategy.price_target_market_making import (
    PriceTargetMarketMakingStrategy
)
from hummingbot.strategy.price_target_market_making.price_target_market_making_config_map import (
    price_target_market_making_config_map
)


def start(self):
    try:
        market = price_target_market_making_config_map.get("market").value.lower()
        raw_market_symbol = price_target_market_making_config_map.get("market_symbol_pair").value.upper()
        target_volume_usd = Decimal(price_target_market_making_config_map.get("target_volume_usd").value)
        target_spread_percentage = Decimal(price_target_market_making_config_map.get("target_spread_percentage").value)
        target_num_orders = price_target_market_making_config_map.get("target_num_orders").value
        price_step_increment = Decimal(price_target_market_making_config_map.get("price_step_increment").value)

        try:
            assets: Tuple[str, str] = self._initialize_market_assets(market, [raw_market_symbol])[0]
        except ValueError as e:
            self._notify(str(e))
            return

        market_names: List[Tuple[str, List[str]]] = [(market, [raw_market_symbol])]

        self._initialize_wallet(token_symbols=list(set(assets)))
        self._initialize_markets(market_names)
        self.assets = set(assets)

        maker_data = [self.markets[market], raw_market_symbol] + list(assets)

        self.strategy = PriceTargetMarketMakingStrategy(market_info=MarketSymbolPair(*maker_data),
                                                        target_volume_usd=target_volume_usd,
                                                        target_spread_percentage=target_spread_percentage,
                                                        target_num_orders=target_num_orders,
                                                        price_step_increment=price_step_increment)
        
    except Exception as e:
        self._notify(str(e))
        self.logger().error("Unknown error during initialization.", exc_info=True)
