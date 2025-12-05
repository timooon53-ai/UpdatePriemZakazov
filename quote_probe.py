# -*- coding: utf-8 -*-
"""CLI helper to fetch taxi quotes via launch → persuggest → routestats.

This tool mirrors the bot's pricing flow so you can test fare retrieval
without running the Telegram bot. Tokens are loaded the same way as in
``main.py`` (from ``token.txt`` or environment variables).
"""

from __future__ import annotations

import argparse
import json
import sys
from types import SimpleNamespace
from typing import Iterable

import main


def _parse_position(value: str | None):
    if not value:
        return None
    try:
        parsed = json.loads(value)
        if isinstance(parsed, list):
            return parsed
    except json.JSONDecodeError:
        pass
    return None


def _format_json(value) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, indent=2)
    except Exception:
        return str(value)


def _guess_user_id(cli_user_id: str | None) -> str:
    if cli_user_id:
        return cli_user_id
    for key in ("X_YATAXI_USERID", "YA_USER_ID", "USER_ID"):
        env_value = main.os.getenv(key)
        if env_value:
            return env_value
    # Fall back to a predictable placeholder.
    return "debug-user"


def fetch_quote(address_from: str, address_to: str, *, city: str, tariff_class: str, user_id: str,
                address_from_position=None, address_to_position=None):
    order_data = {
        "city": city,
        "address_from": address_from,
        "address_to": address_to,
        "tariff_class": tariff_class,
        "user_id": user_id,
    }
    if address_from_position:
        order_data["address_from_position"] = address_from_position
    if address_to_position:
        order_data["address_to_position"] = address_to_position

    context = SimpleNamespace(user_data={"order_data": order_data})
    offer, price = main.populate_price_from_routestats(context, force=True)
    return offer, price, context.user_data.get("routestats_response"), context.user_data.get("order_data")


def main_cli(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from", dest="address_from", required=True, help="Origin address")
    parser.add_argument("--to", dest="address_to", required=True, help="Destination address")
    parser.add_argument("--city", default="moscow", help="City/zone name (default: moscow)")
    parser.add_argument("--tariff", default="vip", help="Tariff class key (default: vip)")
    parser.add_argument("--user-id", dest="user_id", help="User identifier used in routestats payload")
    parser.add_argument("--from-pos", dest="from_pos", help="Optional JSON array with [lon, lat] for origin")
    parser.add_argument("--to-pos", dest="to_pos", help="Optional JSON array with [lon, lat] for destination")
    parser.add_argument("--dump-response", action="store_true", help="Print full routestats response")
    args = parser.parse_args(argv)

    user_id = _guess_user_id(args.user_id)
    from_position = _parse_position(args.from_pos)
    to_position = _parse_position(args.to_pos)

    offer, price, response, populated_order = fetch_quote(
        args.address_from,
        args.address_to,
        city=args.city,
        tariff_class=args.tariff,
        user_id=user_id,
        address_from_position=from_position,
        address_to_position=to_position,
    )

    print(f"Tariff class: {populated_order.get('tariff_class')}")
    print(f"Offer: {offer or '<none>'}")
    print(f"Price: {price if price is not None else '<not found>'}")
    if args.dump_response:
        print("Routestats response:\n" + _format_json(response))

    return 0 if price is not None else 1


if __name__ == "__main__":
    sys.exit(main_cli())
