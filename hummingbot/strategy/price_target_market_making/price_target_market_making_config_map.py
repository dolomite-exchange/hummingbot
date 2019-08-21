from hummingbot.client.config.config_var import ConfigVar
from hummingbot.client.config.config_validators import (
    is_exchange,
    is_valid_market_symbol,
)
from hummingbot.client.settings import (
    required_exchanges,
    EXAMPLE_PAIRS,
)


def symbol_prompt():
    market = price_target_market_making_config_map.get("market").value
    example = EXAMPLE_PAIRS.get(market)
    return "Enter the token symbol you would like to trade on %s%s >>> " \
           % (market, f" (e.g. {example})" if example else "")

# checks if the symbol pair is valid
def is_valid_market_symbol_pair(value: str) -> bool:
    market = price_target_market_making_config_map.get("market").value
    return is_valid_market_symbol(market, value)


price_target_market_making_config_map = {
    "market":                           ConfigVar(key="market",
                                                  prompt="Enter the name of the exchange >>> ",
                                                  validator=is_exchange,
                                                  on_validated=lambda value: required_exchanges.append(value)),
    "market_symbol_pair":               ConfigVar(key="market_symbol_pair",
                                                  prompt=symbol_prompt,
                                                  validator=is_valid_market_symbol_pair),
    "target_volume_usd":                ConfigVar(key="target_volume_usd",
                                                  prompt="Enter the USD value you would like to put towards market "
                                                         "making on this pair (Enter 20000 to signify $20,000) >>> ",
                                                  type_str="float"),
    "target_spread_percentage":         ConfigVar(key="target_spread_percentage",
                                                  prompt="Enter the spread percentage you would like to target "
                                                         "(Enter 0.01 to signify 1%, default is 0.02) >>> ",
                                                  type_str="float",
                                                  default=0.02),
    "target_num_orders":                ConfigVar(key="target_num_orders",
                                                  prompt="How many orders would you like on each side of the market? "
                                                         "(default is 10) >>> ",
                                                  type_str="int",
                                                  default=10),
    "price_step_increment":             ConfigVar(key="price_step_increment",
                                                  prompt="Enter the price step increment orders will be submitted at "
                                                         "(e.g. 0.25 -> 200.00, 200.25, 200.50, etc.) >>>",
                                                  type_str="float")
}
