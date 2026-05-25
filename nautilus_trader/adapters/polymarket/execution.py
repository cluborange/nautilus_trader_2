# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------

import asyncio
import json
import re
from collections import OrderedDict
from collections import defaultdict
from dataclasses import dataclass
from dataclasses import field
from decimal import Decimal
from typing import Any

import msgspec
from py_clob_client_v2.client import BalanceAllowanceParams
from py_clob_client_v2.client import ClobClient
from py_clob_client_v2.client import MarketOrderArgsV2
from py_clob_client_v2.client import OpenOrderParams
from py_clob_client_v2.client import OrderArgsV2
from py_clob_client_v2.client import OrderPayload
from py_clob_client_v2.client import PartialCreateOrderOptions
from py_clob_client_v2.client import TradeParams
from py_clob_client_v2.clob_types import AssetType
from py_clob_client_v2.clob_types import OrderMarketCancelParams
from py_clob_client_v2.clob_types import OrderType as PolyOrderType
from py_clob_client_v2.clob_types import PostOrdersV2Args
from py_clob_client_v2.config import get_contract_config
from py_clob_client_v2.exceptions import PolyApiException
from py_clob_client_v2.order_utils import ExchangeOrderBuilderV1
from py_clob_client_v2.order_utils import ExchangeOrderBuilderV2
from py_clob_client_v2.order_utils.model.order_data_v2 import SignedOrderV2
from py_clob_client_v2.order_utils.model.side import Side
from py_clob_client_v2.order_utils.model.signature_type_v2 import SignatureTypeV2

from nautilus_trader.adapters.polymarket.common.cache import get_polymarket_trades_key
from nautilus_trader.adapters.polymarket.common.constants import DUST_POSITION_THRESHOLD
from nautilus_trader.adapters.polymarket.common.constants import DUST_SNAP_THRESHOLD_DEC
from nautilus_trader.adapters.polymarket.common.constants import POLYMARKET_CANCEL_ALREADY_DONE
from nautilus_trader.adapters.polymarket.common.constants import POLYMARKET_FINALIZED_TRADE_STATUSES
from nautilus_trader.adapters.polymarket.common.constants import POLYMARKET_INVALID_API_KEY
from nautilus_trader.adapters.polymarket.common.constants import POLYMARKET_MIN_MAX_PRICES
from nautilus_trader.adapters.polymarket.common.constants import POLYMARKET_NAUTILUS_BUILDER_CODE
from nautilus_trader.adapters.polymarket.common.constants import POLYMARKET_VENUE
from nautilus_trader.adapters.polymarket.common.constants import (
    VALID_POLYMARKET_MARKET_TIME_IN_FORCE,
)
from nautilus_trader.adapters.polymarket.common.constants import VALID_POLYMARKET_TIME_IN_FORCE
from nautilus_trader.adapters.polymarket.common.conversion import pusd_from_units
from nautilus_trader.adapters.polymarket.common.credentials import PolymarketWebSocketAuth
from nautilus_trader.adapters.polymarket.common.enums import PolymarketEventType
from nautilus_trader.adapters.polymarket.common.enums import PolymarketOrderStatus
from nautilus_trader.adapters.polymarket.common.enums import PolymarketTradeStatus
from nautilus_trader.adapters.polymarket.common.parsing import calculate_commission
from nautilus_trader.adapters.polymarket.common.parsing import make_composite_trade_id
from nautilus_trader.adapters.polymarket.common.parsing import validate_ethereum_address
from nautilus_trader.adapters.polymarket.common.symbol import get_polymarket_condition_id
from nautilus_trader.adapters.polymarket.common.symbol import get_polymarket_instrument_id
from nautilus_trader.adapters.polymarket.common.symbol import get_polymarket_token_id
from nautilus_trader.adapters.polymarket.common.types import JSON
from nautilus_trader.adapters.polymarket.config import PolymarketExecClientConfig
from nautilus_trader.adapters.polymarket.http.conversion import convert_tif_to_polymarket_order_type
from nautilus_trader.adapters.polymarket.http.errors import should_retry
from nautilus_trader.adapters.polymarket.order_fill_tracker import OrderFillTracker
from nautilus_trader.adapters.polymarket.providers import PolymarketInstrumentProvider
from nautilus_trader.adapters.polymarket.schemas.trade import PolymarketTradeReport
from nautilus_trader.adapters.polymarket.schemas.user import PolymarketOpenOrder
from nautilus_trader.adapters.polymarket.schemas.user import PolymarketUserOrder
from nautilus_trader.adapters.polymarket.schemas.user import PolymarketUserTrade
from nautilus_trader.adapters.polymarket.schemas.user import _snap_filled_qty_to_quantity
from nautilus_trader.adapters.polymarket.schemas.user import _sum_filled_quantity
from nautilus_trader.adapters.polymarket.schemas.user import _weighted_average_price
from nautilus_trader.adapters.polymarket.websocket.client import PolymarketWebSocketChannel
from nautilus_trader.adapters.polymarket.websocket.client import PolymarketWebSocketClient
from nautilus_trader.adapters.polymarket.websocket.types import USER_WS_MESSAGE
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.common.enums import LogColor
from nautilus_trader.common.enums import LogLevel
from nautilus_trader.core.datetime import millis_to_nanos
from nautilus_trader.core.datetime import nanos_to_secs
from nautilus_trader.core.datetime import secs_to_nanos
from nautilus_trader.core.nautilus_pyo3 import HttpClient
from nautilus_trader.core.nautilus_pyo3 import HttpResponse
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.messages import BatchCancelOrders
from nautilus_trader.execution.messages import CancelAllOrders
from nautilus_trader.execution.messages import CancelOrder
from nautilus_trader.execution.messages import GenerateFillReports
from nautilus_trader.execution.messages import GenerateOrderStatusReport
from nautilus_trader.execution.messages import GenerateOrderStatusReports
from nautilus_trader.execution.messages import GeneratePositionStatusReports
from nautilus_trader.execution.messages import QueryAccount
from nautilus_trader.execution.messages import SubmitOrder
from nautilus_trader.execution.messages import SubmitOrderList
from nautilus_trader.execution.reports import FillReport
from nautilus_trader.execution.reports import OrderStatusReport
from nautilus_trader.execution.reports import PositionStatusReport
from nautilus_trader.live.execution_client import LiveExecutionClient
from nautilus_trader.live.retry import RetryManagerPool
from nautilus_trader.model.currencies import pUSD
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.enums import ContingencyType
from nautilus_trader.model.enums import LiquiditySide
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.enums import OrderType
from nautilus_trader.model.enums import PositionSide
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.enums import order_side_to_str
from nautilus_trader.model.events import OrderUpdated
from nautilus_trader.model.identifiers import AccountId
from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.identifiers import VenueOrderId
from nautilus_trader.model.objects import AccountBalance
from nautilus_trader.model.objects import Money
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.model.orders import Order


# Polymarket GTD orders need roughly 60s of security buffer. A 360s raw
# expiration gives Rust-signed marketable limits about 5 minutes of effective
# usable life.
POLYMARKET_RUST_MARKET_ORDER_EXPIRATION_SECS = 360
POLYMARKET_PRESIGN_ENDPOINT = "PolymarketExecutionClient.presign_arb"
POLYMARKET_GTD_SECURITY_BUFFER_SECS = 60

# [fern2 local patch] Arb-completion batch (PR 2 — tag-coalescing path).
#
# Strategy tags every leg of an arb-completion burst with
# ``arb_batch:<burst_id>:<n_legs>`` and submits each via the normal
# ``submit_order`` API. The exec adapter buffers tagged orders by burst_id
# and, once the buffer reaches ``n_legs`` (or a short watchdog fires),
# signs them all in parallel via the Rust signer and posts them as a
# single ``post_orders`` HTTP request — bypassing Nautilus's
# ``OrderList``/``submit_order_list`` validator (which still enforces
# same-instrument_id across the list, no good for cross-market neg-risk
# bursts).
ARB_BATCH_TAG_RE = re.compile(r"^arb_batch:(?P<burst_id>[a-f0-9]+):(?P<n_legs>\d+)$")
PRESIGNED_ARB_TAG_RE = re.compile(
    r"^arb_presigned_(?P<mode>batch|individual):(?P<plan_key>[a-f0-9]+):(?P<n_legs>\d+)$",
)
ARB_COMPLETION_PRICE_CAP_TAG_RE = re.compile(
    r"^arb_completion_price_cap:(?P<price>\d+(?:\.\d+)?)$",
)
ARB_BATCH_DEFAULT_WATCHDOG_MS = 25
# Polymarket ``post_orders`` accepts at most 15 orders per request.
ARB_BATCH_MAX_ORDERS_PER_POST = 15


@dataclass
class _PendingArbBatch:
    """Adapter-side coalescing buffer for one in-flight arb-completion batch."""

    burst_id: str
    n_legs_expected: int
    pending: list[tuple[SubmitOrder, Order, float]] = field(default_factory=list)
    created_ts: float = 0.0
    watchdog_task: "asyncio.Task | None" = None
    flushed: bool = False


@dataclass
class _PresignedArbLegSpec:
    instrument_id: InstrumentId
    side: str
    size: float
    tick_size: str
    neg_risk: bool
    boundary_price: float


@dataclass
class _LivePresignedArbBatch:
    plan_key: str
    leg_specs: list[_PresignedArbLegSpec]
    signed_orders_args: list[PostOrdersV2Args]
    expected_venue_order_ids: list[VenueOrderId | None]
    signed_at: float
    effective_expires_at: float
    stale_after_seconds: float
    size_tolerance: float
    generation: int


@dataclass
class _PresignedArbPlan:
    plan_key: str
    live: _LivePresignedArbBatch | None = None
    refresh_in_progress: bool = False
    last_sign_failed: bool = False
    generation: int = 0
    last_used: float = 0.0
    posting_count: int = 0


@dataclass
class _PendingPresignedArbBatch:
    mode: str
    plan_key: str
    n_legs_expected: int
    pending: list[tuple[SubmitOrder, Order, float]] = field(default_factory=list)
    created_ts: float = 0.0
    watchdog_task: "asyncio.Task | None" = None
    flushed: bool = False


def _parse_arb_batch_tag(tags) -> "tuple[str, int] | None":
    """Return ``(burst_id, n_legs)`` if any tag matches the arb-batch scheme.

    Tags are arbitrary order metadata; we only recognise strings of the form
    ``arb_batch:<8hex>:<n_legs>``. Returns None when no such tag is present
    so non-batch orders flow through the standard per-leg path.
    """
    if not tags:
        return None
    for tag in tags:
        if isinstance(tag, str):
            match = ARB_BATCH_TAG_RE.match(tag)
            if match is not None:
                return match.group("burst_id"), int(match.group("n_legs"))
    return None


def _parse_presigned_arb_tag(tags) -> "tuple[str, str, int] | None":
    """Return ``(mode, plan_key, n_legs)`` for presigned arb tags."""
    if not tags:
        return None
    for tag in tags:
        if isinstance(tag, str):
            match = PRESIGNED_ARB_TAG_RE.match(tag)
            if match is not None:
                return match.group("mode"), match.group("plan_key"), int(match.group("n_legs"))
    return None


def _parse_arb_completion_price_cap(tags) -> float | None:
    """Return the bounded marketable limit price for arb-completion orders."""
    if not tags:
        return None
    for tag in tags:
        if isinstance(tag, str):
            match = ARB_COMPLETION_PRICE_CAP_TAG_RE.match(tag)
            if match is not None:
                return float(match.group("price"))
    return None


class PolymarketExecutionClient(LiveExecutionClient):
    """
    Provides an execution client for Polymarket, a decentralized predication market.

    Parameters
    ----------
    loop : asyncio.AbstractEventLoop
        The event loop for the client.
    http_client : py_clob_client_v2.client.ClobClient
        The Polymarket HTTP client.
    msgbus : MessageBus
        The message bus for the client.
    cache : Cache
        The cache for the client.
    clock : LiveClock
        The clock for the client.
    instrument_provider : PolymarketInstrumentProvider
        The instrument provider.
    config : PolymarketExecClientConfig
        The configuration for the client.
    name : str, optional
        The custom client ID.
    rust_client : PyClobClient, optional
        Optional Rust CLOB client used to sign orders. Posting always uses the
        Python ``ClobClient``. When ``None``, signing falls back to the Python
        client.

    """

    PROCESSED_TRADES_LIMIT = 10_000

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        http_client: ClobClient,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
        instrument_provider: PolymarketInstrumentProvider,
        ws_auth: PolymarketWebSocketAuth,
        config: PolymarketExecClientConfig,
        name: str | None,
        rust_client: Any | None = None,
    ) -> None:
        super().__init__(
            loop=loop,
            client_id=ClientId(name or POLYMARKET_VENUE.value),
            venue=POLYMARKET_VENUE,
            oms_type=OmsType.NETTING,
            instrument_provider=instrument_provider,
            account_type=AccountType.CASH,
            base_currency=pUSD,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
        )

        # Configuration
        self._config = config
        self._log.info(f"{config.signature_type=}", LogColor.BLUE)
        self._log.info(f"{config.funder=}", LogColor.BLUE)
        self._log.info(f"{config.max_retries=}", LogColor.BLUE)
        self._log.info(f"{config.retry_delay_initial_ms=}", LogColor.BLUE)
        self._log.info(f"{config.retry_delay_max_ms=}", LogColor.BLUE)
        self._log.info(f"{config.generate_order_history_from_trades=}", LogColor.BLUE)
        self._log.info(f"{config.log_raw_ws_messages=}", LogColor.BLUE)
        self._log.info(f"{config.ack_timeout_secs=}", LogColor.BLUE)
        self._log.info(f"{config.use_rust=}", LogColor.BLUE)

        # Optional Rust CLOB client for order signing (posting always uses Python).
        self._rust_client = rust_client
        if self._rust_client is not None:
            self._log.info(
                "Using Rust client for order signing (Python client posts orders)",
                LogColor.GREEN,
            )

        account_id = AccountId(f"{name or POLYMARKET_VENUE.value}-001")
        self._set_account_id(account_id)
        self._log.info(f"account_id={account_id.value}", LogColor.BLUE)

        wallet_address = http_client.get_address()
        if wallet_address is None:
            raise RuntimeError("Auth error: could not determine `wallet_address`")

        validate_ethereum_address(wallet_address)
        self._wallet_address = wallet_address

        # Get the user address (funder) - this is the address that holds positions
        # For proxy wallets, this differs from the signer address
        user_address = (
            http_client.builder.funder
            if hasattr(http_client, "builder")
            else config.funder or wallet_address
        )
        validate_ethereum_address(user_address)
        self._user_address = user_address

        self._api_key = http_client.creds.api_key
        self._log.info(f"{wallet_address=}", LogColor.BLUE)
        self._log.info(f"{user_address=}", LogColor.BLUE)

        # HTTP API
        self._http_client = http_client
        self._http_client_async = HttpClient(timeout_secs=15)
        self._retry_manager_pool = RetryManagerPool[None](
            pool_size=100,
            max_retries=config.max_retries or 0,
            delay_initial_ms=config.retry_delay_initial_ms or 1_000,
            delay_max_ms=config.retry_delay_max_ms or 10_000,
            backoff_factor=2,
            logger=self._log,
            exc_types=(PolyApiException,),
            retry_check=should_retry,
        )
        self._decoder_order_report = msgspec.json.Decoder(PolymarketOpenOrder)
        self._decoder_trade_report = msgspec.json.Decoder(PolymarketTradeReport)

        # WebSocket API
        self._ws_auth = ws_auth
        self._ws_client: PolymarketWebSocketClient = PolymarketWebSocketClient(
            self._clock,
            base_url=self._config.base_url_ws,
            channel=PolymarketWebSocketChannel.USER,
            handler=self._handle_ws_message,
            handler_reconnect=None,
            loop=self._loop,
            auth=self._ws_auth,
            max_subscriptions_per_connection=self._config.ws_max_subscriptions_per_connection,
            proxy_url=self._config.proxy_url,
        )
        self._decoder_user_msg = msgspec.json.Decoder(USER_WS_MESSAGE)

        # Fill tracker for dust detection
        self._fill_tracker = OrderFillTracker()

        # Hot caches
        self._processed_fills: OrderedDict[tuple[TradeId, VenueOrderId], None] = OrderedDict()
        self._processed_trades: OrderedDict[TradeId, PolymarketTradeStatus] = OrderedDict()
        self._finalized_trades: OrderedDict[TradeId, None] = OrderedDict()
        self._ack_events_order: dict[VenueOrderId, asyncio.Event] = {}
        self._ack_events_trade: dict[VenueOrderId, asyncio.Event] = {}
        self._order_timing_starts: dict[VenueOrderId, float] = {}
        self._order_ack_seen_at: dict[VenueOrderId, float] = {}
        self._collateral_balance_pusd: float | None = None
        # [fern2 local patch] Arb-completion batch coalescing buffer keyed by
        # burst_id (the ``arb_batch:<burst_id>:<n>`` tag on each leg). See
        # ``_queue_arb_batch_order`` / ``_process_arb_batch``.
        self._arb_batch_buffer: dict[str, _PendingArbBatch] = {}
        self._arb_batch_watchdog_ms: int = ARB_BATCH_DEFAULT_WATCHDOG_MS
        # [fern2 local patch] B2 presigned arb-completion cache. Strategy
        # sends ensure/refresh requests via a lightweight message-bus endpoint;
        # fill-time orders carry ``arb_presigned_*`` tags and hit this cache.
        self._presigned_arb_plans: OrderedDict[str, _PresignedArbPlan] = OrderedDict()
        self._presigned_arb_buffer: dict[str, _PendingPresignedArbBatch] = {}
        self._presigned_arb_watchdog_ms: int = ARB_BATCH_DEFAULT_WATCHDOG_MS
        try:
            self._msgbus.register(POLYMARKET_PRESIGN_ENDPOINT, self._handle_presign_request)
        except KeyError:
            self._log.warning(
                f"Presign endpoint already registered: {POLYMARKET_PRESIGN_ENDPOINT}",
                LogColor.YELLOW,
            )

    def calculate_commission(self, instrument, last_qty, last_px, liquidity_side):
        commission = calculate_commission(
            quantity=last_qty.as_decimal(),
            price=last_px.as_decimal(),
            fee_rate=instrument.taker_fee,
            liquidity_side=liquidity_side,
        )

        return Money(commission, pUSD)

    async def _connect(self) -> None:
        await self._instrument_provider.initialize()

        # Add initial market subscriptions
        instruments = self._cache.instruments(venue=POLYMARKET_VENUE)
        for instrument in instruments:
            condition_id = get_polymarket_condition_id(instrument.id)
            self._ws_client.add_subscription(condition_id)

        try:
            # Only connect if we have subscriptions (avoids empty = all behavior)
            if self._ws_client.has_subscriptions:
                await self._ws_client.connect()

            await self._update_account_state()
            await self._await_account_registered()
        except PolyApiException as e:
            self._log.error(repr(e))
            if e.error_msg["error"] == POLYMARKET_INVALID_API_KEY:
                await self._ws_client.disconnect()
            raise e

    async def _disconnect(self) -> None:
        await self._ws_client.disconnect()

    def _stop(self) -> None:
        try:
            self._msgbus.deregister(POLYMARKET_PRESIGN_ENDPOINT, self._handle_presign_request)
        except Exception:
            pass
        self._retry_manager_pool.shutdown()

    async def _maintain_active_market(self, instrument_id: InstrumentId) -> None:
        condition_id = get_polymarket_condition_id(instrument_id)
        # [fern2 local patch] Hot-path gate. This runs at the top of every
        # order submit/cancel. When the condition_id already has a live
        # subscription, skip the async, asyncio.Lock-taking, refcount-
        # incrementing subscribe() path entirely: it would only take the
        # lock, bump the (never-decremented) refcount and return. Avoiding
        # the event-loop hop + lock contention here is critical for fast
        # arb-completion market orders. Fall through to subscribe() (which
        # owns the reconnect/retry path) when the subscription is missing
        # or its client connection is not active.
        if self._ws_client.is_subscription_active(condition_id):
            return
        await self._ws_client.subscribe(condition_id)

    async def _update_account_state(self) -> None:
        self._log.info("Checking account balance")

        params = BalanceAllowanceParams(
            asset_type=AssetType.COLLATERAL,
            signature_type=self._config.signature_type,
        )
        response: dict[str, Any] = await asyncio.to_thread(
            self._http_client.get_balance_allowance,
            params,
        )
        self._collateral_balance_pusd = int(response["balance"]) / 1_000_000
        total = pusd_from_units(int(response["balance"]))
        account_balance = AccountBalance(
            total=total,
            locked=Money.from_raw(0, pUSD),
            free=total,
        )

        self.generate_account_state(
            balances=[account_balance],
            margins=[],  # N/A
            reported=True,
            ts_event=self._clock.timestamp_ns(),
        )

    async def _fetch_user_positions(
        self,
        *,
        limit: int = 100,
        size_threshold: int = 0,
    ) -> list[dict[str, Any]]:
        """
        Fetch all current positions for the configured user using the Polymarket Data
        API.

        Implements pagination as the endpoint returns a maximum of 100 entries per
        request.

        """
        base_url = (self._config.base_url_data_api or "https://data-api.polymarket.com").rstrip("/")
        base_url = f"{base_url}/positions"
        results: list[dict[str, Any]] = []
        offset = 0

        while True:
            params = {
                "user": self._user_address,
                "limit": str(limit),
                "offset": str(offset),
                "sizeThreshold": str(size_threshold),
                "sortBy": "TOKENS",
                "sortDirection": "DESC",
            }
            response: HttpResponse = await self._http_client_async.get(
                url=base_url,
                params=params,
            )

            if response.status >= 400:
                raise RuntimeError(f"HTTP {response.status}: Failed to fetch positions")

            data = msgspec.json.decode(response.body)
            if not data:
                break
            if isinstance(data, list):
                results.extend(data)
                if len(data) < limit:
                    break
            else:
                # Unexpected shape; stop to avoid loop
                break
            offset += limit
            if offset > 10000:
                self._log.warning("Offset exceeded 10000; stopping")
                break

        return results

    # -- EXECUTION REPORTS ------------------------------------------------------------------------

    async def generate_order_status_reports(  # noqa: C901
        self,
        command: GenerateOrderStatusReports,
    ) -> list[OrderStatusReport]:
        self._log.debug("Requesting OrderStatusReports...")
        reports: list[OrderStatusReport] = []

        if command.instrument_id is not None:
            condition_id = get_polymarket_condition_id(command.instrument_id)
            asset_id = get_polymarket_token_id(command.instrument_id)
            params = OpenOrderParams(market=condition_id, asset_id=asset_id)
        else:
            params = None

        # Check active orders with venue
        # Note: py_clob_client_v2.get_open_orders() handles pagination internally
        retry_manager = await self._retry_manager_pool.acquire()
        try:
            response: list[JSON] | None = await retry_manager.run(
                "generate_order_status_reports",
                [command.instrument_id],
                asyncio.to_thread,
                self._http_client.get_open_orders,
                params=params,
            )

            if response:
                # Uncomment for development
                # self._log.info(f"Processing {len(response)} orders", LogColor.MAGENTA)
                for json_obj in response:
                    raw = msgspec.json.encode(json_obj)
                    polymarket_order = self._decoder_order_report.decode(raw)

                    instrument_id = get_polymarket_instrument_id(
                        polymarket_order.market,
                        polymarket_order.asset_id,
                    )
                    instrument = self._cache.instrument(instrument_id)
                    if instrument is None:
                        self._log.warning(
                            f"Cannot handle order report: instrument {instrument_id} not found "
                            f"(market={polymarket_order.market}, asset_id={polymarket_order.asset_id})",
                        )
                        continue

                    venue_order_id = polymarket_order.get_venue_order_id()
                    client_order_id = self._cache.client_order_id(venue_order_id)
                    if client_order_id is None:
                        client_order_id = ClientOrderId(str(UUID4()))

                    report = polymarket_order.parse_to_order_status_report(
                        account_id=self.account_id,
                        instrument=instrument,
                        client_order_id=client_order_id,
                        ts_init=self._clock.timestamp_ns(),
                    )
                    reports.append(report)
        finally:
            await self._retry_manager_pool.release(retry_manager)

        if self._config.generate_order_history_from_trades:
            self._log.warning(
                "Experimental feature not currently recommended: generating order history from trades",
            )
            reported_client_order_ids: set[ClientOrderId] = {r.client_order_id for r in reports}
            for order in self._cache.orders_open(venue=POLYMARKET_VENUE):
                if order.client_order_id in reported_client_order_ids:
                    continue  # Already reported

                command = GenerateOrderStatusReport(
                    instrument_id=order.instrument_id,
                    client_order_id=order.client_order_id,
                    venue_order_id=order.venue_order_id,
                    command_id=UUID4(),
                    ts_init=self._clock.timestamp_ns(),
                )
                maybe_report = await self.generate_order_status_report(command)
                if maybe_report:
                    reports.append(maybe_report)

            known_venue_order_ids: set[VenueOrderId] = {
                o.venue_order_id for o in self._cache.orders()
            }
            known_venue_order_ids.update({r.venue_order_id for r in reports})

            # Check fills to generate order reports
            fill_command = GenerateFillReports(
                instrument_id=command.instrument_id,
                venue_order_id=None,
                start=None,
                end=None,
                command_id=UUID4(),
                ts_init=self._clock.timestamp_ns(),
            )
            fill_reports = await self.generate_fill_reports(fill_command)
            if fill_reports and not known_venue_order_ids:
                self._log.warning(
                    "No previously known venue order IDs found in cache or from active orders",
                )

            venue_order_id_fill_reports: dict[VenueOrderId, list[FillReport]] = defaultdict(list)

            for fill in fill_reports:
                if fill.venue_order_id in known_venue_order_ids:
                    continue  # Already reported
                venue_order_id_fill_reports[fill.venue_order_id].append(fill)

            for venue_order_id, fill_reports in venue_order_id_fill_reports.items():
                first_fill = fill_reports[0]
                instrument = self._cache.instrument(first_fill.instrument_id)
                if instrument is None:
                    self._log.warning(
                        f"Cannot handle order report: instrument {first_fill.instrument_id} not found "
                        f"(venue_order_id={venue_order_id})",
                    )
                    continue

                order_type = (
                    OrderType.MARKET
                    if first_fill.liquidity_side == LiquiditySide.TAKER
                    else OrderType.LIMIT
                )

                if order_type == OrderType.LIMIT:
                    price = first_fill.last_px
                else:
                    price = None

                order_side = first_fill.order_side

                avg_px: float = 0.0
                filled_qty: float = 0.0
                ts_last: int = first_fill.ts_event

                for fill_report in fill_reports:
                    avg_px += float(fill_report.last_px) * float(fill_report.last_qty)
                    filled_qty += float(fill_report.last_qty)
                    ts_last = fill_report.ts_event

                if filled_qty > 0:
                    avg_px /= filled_qty
                else:
                    avg_px = 0.0

                self._log.warning(f"{venue_order_id=}")
                self._log.warning(f"{avg_px=}")
                self._log.warning(f"{filled_qty=}")

                report = OrderStatusReport(
                    account_id=first_fill.account_id,
                    instrument_id=first_fill.instrument_id,
                    client_order_id=ClientOrderId(str(UUID4())),
                    order_list_id=None,
                    venue_order_id=venue_order_id,
                    order_side=order_side,
                    order_type=order_type,
                    contingency_type=ContingencyType.NO_CONTINGENCY,
                    time_in_force=TimeInForce.GTC,
                    order_status=OrderStatus.FILLED,
                    price=price,
                    avg_px=instrument.make_price(avg_px),
                    quantity=instrument.make_qty(filled_qty),
                    filled_qty=instrument.make_qty(filled_qty),
                    ts_accepted=ts_last,
                    ts_last=ts_last,
                    report_id=UUID4(),
                    ts_init=self._clock.timestamp_ns(),
                )
                self._log.warning(f"Generated from fill report: {report}")
                reports.append(report)

        self._log_report_receipt(
            len(reports),
            "OrderStatusReport",
            command.log_receipt_level,
        )

        return reports

    async def generate_order_status_report(
        self,
        command: GenerateOrderStatusReport,
    ) -> OrderStatusReport | None:
        await self._maintain_active_market(command.instrument_id)

        venue_order_id = command.venue_order_id
        if venue_order_id is None:
            venue_order_id = self._cache.venue_order_id(command.client_order_id)
            if venue_order_id is None:
                self._log.error(
                    "Cannot generate an order status report for Polymarket without the venue order ID",
                )
                return None  # Failed

        self._log.info(
            f"Generating OrderStatusReport for "
            f"{repr(command.client_order_id) if command.client_order_id else ''} "
            f"{repr(command.venue_order_id) if command.venue_order_id else ''}",
        )

        retry_manager = await self._retry_manager_pool.acquire()
        try:
            response: JSON | None = await retry_manager.run(
                "generate_order_status_report",
                [command.client_order_id, venue_order_id],
                asyncio.to_thread,
                self._http_client.get_order,
                order_id=venue_order_id.value,
            )

            if not response:
                return await self._recover_terminal_status_from_trades(
                    command=command,
                    venue_order_id=venue_order_id,
                )
            # Uncomment for development
            # self._log.info(str(response), LogColor.MAGENTA)
            raw_response = msgspec.json.encode(response)
            polymarket_order = self._decoder_order_report.decode(raw_response)
            instrument_id = get_polymarket_instrument_id(
                polymarket_order.market,
                polymarket_order.asset_id,
            )
            instrument = self._cache.instrument(instrument_id)
            if instrument is None:
                self._log.warning(
                    f"Cannot handle order report: instrument {instrument_id} not found "
                    f"(market={polymarket_order.market}, asset_id={polymarket_order.asset_id})",
                )
                return None

            return polymarket_order.parse_to_order_status_report(
                account_id=self.account_id,
                instrument=instrument,
                client_order_id=command.client_order_id,
                ts_init=self._clock.timestamp_ns(),
            )
        finally:
            await self._retry_manager_pool.release(retry_manager)

    async def _recover_terminal_status_from_trades(
        self,
        command: GenerateOrderStatusReport,
        venue_order_id: VenueOrderId,
    ) -> OrderStatusReport | None:
        # See `docs/integrations/polymarket.md` (Single-order recovery from trades).
        instrument_id = command.instrument_id
        if instrument_id is None and command.client_order_id is not None:
            cached_order_for_instrument = self._cache.order(command.client_order_id)
            if cached_order_for_instrument is not None:
                instrument_id = cached_order_for_instrument.instrument_id

        if instrument_id is None:
            self._log.warning(
                f"Cannot recover terminal status for {venue_order_id!r}: instrument_id unknown",
            )
            return None

        instrument = self._cache.instrument(instrument_id)
        if instrument is None:
            self._log.warning(
                f"Cannot recover terminal status for {venue_order_id!r}: "
                f"instrument {instrument_id} not found",
            )
            return None

        fill_command = GenerateFillReports(
            instrument_id=instrument_id,
            venue_order_id=venue_order_id,
            start=None,
            end=None,
            command_id=UUID4(),
            ts_init=self._clock.timestamp_ns(),
        )
        fills = await self.generate_fill_reports(fill_command)
        fills = [f for f in fills if f.venue_order_id == venue_order_id]

        # Fall back to the venue_order_id index when client_order_id is absent.
        resolved_client_order_id = command.client_order_id
        if resolved_client_order_id is None:
            resolved_client_order_id = self._cache.client_order_id(venue_order_id)
        cached_order: Order | None = (
            self._cache.order(resolved_client_order_id)
            if resolved_client_order_id is not None
            else None
        )

        if cached_order is None:
            # Don't synthesize an external order from trades alone.
            self._log.info(
                f"Order {venue_order_id!r} not active at venue and no cached order; "
                f"deferring to engine",
            )
            return None

        ts_now = self._clock.timestamp_ns()
        cached_price: Price | None = cached_order.price if cached_order.has_price else None

        if not fills:
            order_status = OrderStatus.CANCELED
            quantity = cached_order.quantity
            filled_qty = cached_order.filled_qty
            order_side = cached_order.side
            order_type = cached_order.order_type
            time_in_force = cached_order.time_in_force
            price = cached_price
            avg_px: Decimal | None = None
            cancel_reason: str | None = "ORDER_NOT_FOUND_AT_VENUE"
            ts_event = ts_now
            self._log.info(
                f"Order {venue_order_id!r} not active at venue and no trades found; "
                f"recovering as Canceled",
            )
        else:
            total_filled = _sum_filled_quantity(fills)
            avg_px = _weighted_average_price(fills, total_filled)
            ts_event = max(f.ts_event for f in fills)
            raw_filled_qty = instrument.make_qty(total_filled)
            quantity = cached_order.quantity
            order_side = cached_order.side
            order_type = cached_order.order_type
            time_in_force = cached_order.time_in_force
            price = cached_price
            # Mirror live-parser dust handling (see `docs/integrations/polymarket.md`).
            dust_diff = abs(quantity.as_decimal() - raw_filled_qty.as_decimal())
            order_status = (
                OrderStatus.FILLED
                if raw_filled_qty >= quantity or dust_diff < DUST_SNAP_THRESHOLD_DEC
                else OrderStatus.CANCELED
            )
            filled_qty = _snap_filled_qty_to_quantity(quantity, raw_filled_qty, order_status)
            cancel_reason = None
            self._log.info(
                f"Recovered {order_status.name} status for {venue_order_id!r} from "
                f"{len(fills)} trade(s) (filled_qty={filled_qty}, quantity={quantity})",
            )

        return OrderStatusReport(
            account_id=self.account_id,
            instrument_id=instrument_id,
            client_order_id=resolved_client_order_id,
            order_list_id=None,
            venue_order_id=venue_order_id,
            order_side=order_side,
            order_type=order_type,
            contingency_type=ContingencyType.NO_CONTINGENCY,
            time_in_force=time_in_force,
            order_status=order_status,
            price=price,
            quantity=quantity,
            filled_qty=filled_qty,
            avg_px=avg_px,
            cancel_reason=cancel_reason,
            ts_accepted=ts_event,
            ts_last=ts_event,
            report_id=UUID4(),
            ts_init=ts_now,
        )

    async def generate_fill_reports(
        self,
        command: GenerateFillReports,
    ) -> list[FillReport]:
        self._log.debug("Requesting FillReports...")
        reports: list[FillReport] = []

        params = TradeParams()

        if command.instrument_id:
            condition_id = get_polymarket_condition_id(command.instrument_id)
            params.market = condition_id
            # Note: We intentionally don't filter by asset_id here because the API
            # filters by TAKER's asset_id, which would miss maker fills from cross-asset
            # matches (e.g., YES maker matched against NO taker)

        if command.start is not None:
            params.after = int(command.start.timestamp())
        if command.end is not None:
            params.before = int(command.end.timestamp())

        details = []

        if command.instrument_id:
            details.append(command.instrument_id)

        # Note: py_clob_client_v2.get_trades() handles pagination internally
        retry_manager = await self._retry_manager_pool.acquire()
        try:
            response: list[JSON] | None = await retry_manager.run(
                "generate_fill_reports",
                details,
                asyncio.to_thread,
                self._http_client.get_trades,
                params=params,
            )

            if response:
                # Uncomment for development
                # self._log.info(f"Processing {len(response)} trades", LogColor.MAGENTA)
                parsed_fill_keys: set[tuple[TradeId, VenueOrderId]] = set()

                for json_obj in response:
                    self._parse_trades_response_object(
                        command=command,
                        json_obj=json_obj,
                        parsed_fill_keys=parsed_fill_keys,
                        reports=reports,
                    )
        finally:
            await self._retry_manager_pool.release(retry_manager)

        self._log_report_receipt(len(reports), "FillReport", LogLevel.INFO)

        return reports

    async def generate_position_status_reports(
        self,
        command: GeneratePositionStatusReports,
    ) -> list[PositionStatusReport]:
        reports: list[PositionStatusReport] = []

        if command.instrument_id is not None:
            instrument_ids = [command.instrument_id]
        else:
            instrument_ids = [inst.id for inst in self._cache.instruments(venue=POLYMARKET_VENUE)]

        quantities_by_instrument = await self._fetch_quantities_from_gamma_api(instrument_ids)

        # Generate reports from quantities (filter dust positions)
        for instrument_id, quantity in quantities_by_instrument.items():
            size = float(quantity)
            if 0.0 < size < DUST_POSITION_THRESHOLD:
                self._log.debug(f"Filtering dust position: {instrument_id}, size={size}")
            if size < DUST_POSITION_THRESHOLD:
                continue
            position_side = PositionSide.LONG
            self._log.info(f"Long position for {instrument_id} of {quantity} shares")

            now = self._clock.timestamp_ns()
            report = PositionStatusReport(
                account_id=self.account_id,
                instrument_id=instrument_id,
                position_side=position_side,
                quantity=quantity,
                report_id=UUID4(),
                ts_last=now,
                ts_init=now,
            )
            reports.append(report)

        self._log_report_receipt(
            len(reports),
            "PositionReport",
            command.log_receipt_level,
        )

        return reports

    def _parse_trades_response_object(
        self,
        command: GenerateFillReports,
        json_obj: JSON,
        parsed_fill_keys: set[tuple[TradeId, VenueOrderId]],
        reports: list[FillReport],
    ) -> None:
        raw = msgspec.json.encode(json_obj)
        polymarket_trade = self._decoder_trade_report.decode(raw)

        filled_user_order_ids = polymarket_trade.get_filled_user_order_ids(
            self._wallet_address,
            self._api_key,
        )

        for order_id in filled_user_order_ids:
            asset_id = polymarket_trade.get_asset_id(order_id)
            instrument_id = get_polymarket_instrument_id(polymarket_trade.market, asset_id)

            # Filter by instrument_id if specified in command
            if command.instrument_id is not None and instrument_id != command.instrument_id:
                continue

            instrument = self._cache.instrument(instrument_id)
            if instrument is None:
                self._log.warning(
                    f"Cannot handle trade report: instrument {instrument_id} not found "
                    f"(market={polymarket_trade.market}, asset_id={asset_id})",
                )
                continue

            venue_order_id = polymarket_trade.venue_order_id(order_id)

            if command.venue_order_id is not None and venue_order_id != command.venue_order_id:
                continue

            client_order_id = self._cache.client_order_id(venue_order_id)
            if client_order_id is None:
                client_order_id = ClientOrderId(str(UUID4()))

            report = polymarket_trade.parse_to_fill_report(
                account_id=self.account_id,
                instrument=instrument,
                client_order_id=client_order_id,
                ts_init=self._clock.timestamp_ns(),
                filled_user_order_id=order_id,
            )

            # Apply the same dust snap as the WS path so the engine sees a
            # consistent fill quantity. Commission stays as the venue computed
            # it from the on-chain fill: the snap is only an internal
            # accommodation for engine-side overfill checks.
            report.last_qty = self._fill_tracker.snap_fill_qty(venue_order_id, report.last_qty)

            fill_key = (report.trade_id, report.venue_order_id)
            if fill_key in parsed_fill_keys:
                self._log.warning(f"Duplicate fill key {fill_key}, skipping")
                continue

            parsed_fill_keys.add(fill_key)
            reports.append(report)

    async def _fetch_quantities_from_gamma_api(
        self,
        instrument_ids: list[InstrumentId],
    ) -> dict[InstrumentId, Quantity]:
        """
        Fetch position quantities using Gamma API (bulk fetch).
        """
        self._log.debug("Fetching positions from Gamma API")

        # Fetch all user positions once (paginated)
        positions: list[dict[str, Any]] = await self._fetch_user_positions(
            limit=100,
            size_threshold=0,
        )

        # Map asset (token id) -> size (shares)
        size_by_asset: dict[str, float] = {}

        for p in positions:
            instrument_id = InstrumentId.from_str(
                p.get("conditionId", "") + "-" + str(p.get("asset", "")) + ".POLYMARKET",
            )
            size_val = p.get("size", 0) or 0
            try:
                size_by_asset[instrument_id] = float(size_val)
            except Exception as e:
                self._log.warning(
                    f"Failed to parse position size for instrument {instrument_id}: {e}",
                )
                continue

        # Convert to quantities by instrument ID
        quantities: dict[InstrumentId, Quantity] = {}

        for instrument_id in instrument_ids:
            size = size_by_asset.get(instrument_id, 0.0)
            # Gamma API returns size as decimal float (e.g., 1.5 shares)
            quantities[instrument_id] = Quantity(float(size), precision=pUSD.precision)

        return quantities

    # -- COMMAND HANDLERS -------------------------------------------------------------------------

    def _generate_cancel_event(
        self,
        strategy_id,
        instrument_id,
        client_order_id,
        venue_order_id,
        reason: str,
        ts_event: int,
    ) -> None:
        # Venue says order is gone (canceled or matched) - suppress event
        # and let WS deliver the correct terminal event (CANCELLATION or TRADE)
        if POLYMARKET_CANCEL_ALREADY_DONE in reason:
            self._log.info(
                f"Cancel rejected for {client_order_id!r}: {reason} "
                "- awaiting WS event for terminal state",
            )
            return

        self.generate_order_cancel_rejected(
            strategy_id=strategy_id,
            instrument_id=instrument_id,
            client_order_id=client_order_id,
            venue_order_id=venue_order_id,
            reason=reason,
            ts_event=ts_event,
        )

    def _get_neg_risk_for_instrument(self, instrument) -> bool:
        if instrument is None or instrument.info is None:
            return False
        return instrument.info.get("neg_risk", False)

    def _create_order_options(self, instrument) -> PartialCreateOrderOptions:
        """
        Build CLOB signing options from cached instrument metadata.

        Passing both tick_size and neg_risk avoids SDK-side market metadata
        lookups on the order signing path.
        """
        return PartialCreateOrderOptions(
            tick_size=format(instrument.price_increment.as_decimal(), "f"),
            neg_risk=self._get_neg_risk_for_instrument(instrument),
        )

    @staticmethod
    def _build_signed_order_v2_from_rust_json(signed_order_json: str) -> SignedOrderV2:
        """
        Build a ``SignedOrderV2`` from JSON returned by ``PyClobClient`` signing methods.

        The Rust adapter emits a camelCase payload matching the V2 wire schema
        (``timestamp`` / ``metadata`` / ``builder`` populated), so we just need
        to convert ``side`` and ``signatureType`` to their typed enums for the
        Python V2 client to recognise it as a V2 order via ``hasattr(order,
        "timestamp")``.
        """
        data = json.loads(signed_order_json)
        side = Side.BUY if str(data["side"]).upper() == "BUY" else Side.SELL
        return SignedOrderV2(
            salt=str(data["salt"]),
            maker=data["maker"],
            signer=data["signer"],
            tokenId=data["tokenId"],
            makerAmount=data["makerAmount"],
            takerAmount=data["takerAmount"],
            side=side,
            signatureType=SignatureTypeV2(int(data["signatureType"])),
            timestamp=str(data["timestamp"]),
            metadata=data["metadata"],
            builder=data["builder"],
            expiration=str(data["expiration"]),
            signature=data["signature"],
        )

    async def _sign_limit_order_rust(self, order: Order, instrument) -> SignedOrderV2:
        """
        Sign a single limit order via the Rust CLOB client.

        Returns a ``SignedOrderV2`` ready to be posted by the Python client.
        """
        tick_size = float(instrument.price_increment.as_decimal())
        neg_risk = self._get_neg_risk_for_instrument(instrument)
        signed_order_json = await self._rust_client.create_order(
            get_polymarket_token_id(order.instrument_id),
            order_side_to_str(order.side),
            float(order.quantity),
            float(order.price),
            self._get_expiration_secs(order),
            tick_size,
            neg_risk,
        )
        return self._build_signed_order_v2_from_rust_json(signed_order_json)

    @staticmethod
    def _get_min_max_prices(instrument) -> tuple[float, float]:
        """
        Return the (min, max) marketable limit prices for the instrument's tick size.
        """
        tick_size_str = format(instrument.price_increment.as_decimal(), "f")
        return POLYMARKET_MIN_MAX_PRICES.get(
            tick_size_str,
            POLYMARKET_MIN_MAX_PRICES[None],
        )

    @staticmethod
    def _get_expiration_secs(order: Order) -> int:
        """
        Return the Polymarket order expiration in seconds, or 0 for no expiry.
        """
        expire_time_ns = getattr(order, "expire_time_ns", None)
        return int(nanos_to_secs(expire_time_ns)) if expire_time_ns else 0

    def _get_rust_market_order_expiration_secs(self) -> int:
        """
        Return a short GTD expiration for Rust-signed market-as-limit orders.
        """
        return int(self._clock.timestamp()) + POLYMARKET_RUST_MARKET_ORDER_EXPIRATION_SECS

    async def _sign_market_order_rust_as_limit(
        self,
        order: Order,
        instrument,
    ) -> SignedOrderV2:
        """
        Sign a market order as an aggressive limit via the Rust CLOB client.

        Untagged market orders use the per-tick-size boundary (max for BUY,
        min for SELL). Fern arb-completion orders can carry an explicit
        ``arb_completion_price_cap`` tag, which is used as the marketable limit
        price. ``order.quantity`` is passed straight through as base-share size
        for both sides so fills cannot exceed the requested share quantity.
        """
        min_price, max_price = self._get_min_max_prices(instrument)
        tick_size = float(instrument.price_increment.as_decimal())
        neg_risk = self._get_neg_risk_for_instrument(instrument)
        completion_price_cap = _parse_arb_completion_price_cap(order.tags)
        if completion_price_cap is not None:
            price = min(max_price, max(min_price, completion_price_cap))
        else:
            price = max_price if order.side == OrderSide.BUY else min_price
        size = float(order.quantity)

        self._log.info(
            f"Signing market order as limit via Rust: "
            f"side={order_side_to_str(order.side)} price={price} "
            f"size={size} tick_size={tick_size} "
            f"arb_completion_cap={completion_price_cap is not None}",
            LogColor.CYAN,
        )

        signed_order_json = await self._rust_client.create_order(
            get_polymarket_token_id(order.instrument_id),
            order_side_to_str(order.side),
            size,
            price,
            self._get_rust_market_order_expiration_secs(),
            tick_size,
            neg_risk,
        )
        return self._build_signed_order_v2_from_rust_json(signed_order_json)

    async def _query_account(self, _command: QueryAccount) -> None:
        # Specific account ID (sub account) not yet supported
        await self._update_account_state()

    async def _cancel_order(self, command: CancelOrder) -> None:
        # https://docs.polymarket.com/#cancel-an-order
        await self._maintain_active_market(command.instrument_id)

        order: Order | None = self._cache.order(command.client_order_id)

        if order is None:
            self._log.error(f"Cannot cancel order: {command.client_order_id!r} not found in cache")
            return

        if order.is_closed:
            self._log.warning(
                f"`CancelOrder` command for {command.client_order_id!r} when order already {order.status_string()} "
                "(will not send to exchange)",
            )
            return

        venue_order_id = order.venue_order_id
        if venue_order_id is None:
            # Check cache index: submit may have cached it before OrderAccepted was applied
            venue_order_id = self._cache.venue_order_id(order.client_order_id)

        if venue_order_id is None:
            self._log.info(
                f"Cancel for {command.client_order_id!r} deferred, "
                "venue_order_id not yet available",
            )
            return

        retry_manager = await self._retry_manager_pool.acquire()
        try:
            response: JSON | None = await retry_manager.run(
                "cancel_order",
                [order.client_order_id, venue_order_id],
                asyncio.to_thread,
                self._http_client.cancel_order,
                OrderPayload(orderID=venue_order_id.value),
            )

            if not response or not retry_manager.result:
                reason = retry_manager.message
            else:
                reason = response.get("not_canceled")

            if reason:
                self._generate_cancel_event(
                    strategy_id=order.strategy_id,
                    instrument_id=order.instrument_id,
                    client_order_id=order.client_order_id,
                    venue_order_id=venue_order_id,
                    reason=str(reason),
                    ts_event=self._clock.timestamp_ns(),
                )
        finally:
            await self._retry_manager_pool.release(retry_manager)

    async def _execute_deferred_cancel(
        self,
        order: Order,
        venue_order_id: VenueOrderId,
    ) -> None:
        retry_manager = await self._retry_manager_pool.acquire()
        try:
            response: JSON | None = await retry_manager.run(
                "cancel_order",
                [order.client_order_id, venue_order_id],
                asyncio.to_thread,
                self._http_client.cancel_order,
                OrderPayload(orderID=venue_order_id.value),
            )

            if not response or not retry_manager.result:
                reason = retry_manager.message
            else:
                reason = response.get("not_canceled")

            if reason:
                self._generate_cancel_event(
                    strategy_id=order.strategy_id,
                    instrument_id=order.instrument_id,
                    client_order_id=order.client_order_id,
                    venue_order_id=venue_order_id,
                    reason=str(reason),
                    ts_event=self._clock.timestamp_ns(),
                )
        finally:
            await self._retry_manager_pool.release(retry_manager)

    async def _batch_cancel_orders(self, command: BatchCancelOrders) -> None:
        # https://docs.polymarket.com/#cancel-orders
        await self._maintain_active_market(command.instrument_id)

        # Check open orders for instrument
        open_order_ids = self._cache.client_order_ids_open(instrument_id=command.instrument_id)

        # Filter orders that are actually open
        valid_cancels: list[CancelOrder] = []

        for cancel in command.cancels:
            if cancel.client_order_id in open_order_ids:
                valid_cancels.append(cancel)
                continue
            self._log.warning(f"{cancel.client_order_id!r} not open for cancel")

        if not valid_cancels:
            self._log.warning(f"No orders open for {command.instrument_id} batch cancel")
            return

        retry_manager = await self._retry_manager_pool.acquire()
        try:
            order_ids = []

            for cancel in valid_cancels:
                order = self._cache.order(cancel.client_order_id)
                if order and order.venue_order_id:
                    order_ids.append(order.venue_order_id.value)
            response: JSON | None = await retry_manager.run(
                "batch_cancel_orders",
                [command.instrument_id],
                asyncio.to_thread,
                self._http_client.cancel_orders,
                order_ids,
            )

            if not response or not retry_manager.result:
                reason_map = dict.fromkeys(order_ids, retry_manager.message)
            else:
                reason_map = response.get("not_canceled", {})

            for order_id, reason in reason_map.items():
                venue_order_id = VenueOrderId(order_id)
                client_order_id = self._cache.client_order_id(venue_order_id)
                if client_order_id:
                    self._generate_cancel_event(
                        strategy_id=command.strategy_id,
                        instrument_id=command.instrument_id,
                        client_order_id=client_order_id,
                        venue_order_id=venue_order_id,
                        reason=str(reason),
                        ts_event=self._clock.timestamp_ns(),
                    )
        finally:
            await self._retry_manager_pool.release(retry_manager)

    async def _cancel_all_orders(self, command: CancelAllOrders) -> None:
        # https://docs.polymarket.com/#cancel-orders
        await self._maintain_active_market(command.instrument_id)

        # Polymarket API does not support side-specific cancellation
        if command.order_side != OrderSide.NO_ORDER_SIDE:
            self._log.warning(
                f"Polymarket does not support order_side filtering for cancel all orders; "
                f"ignoring order_side={order_side_to_str(command.order_side)} and canceling all orders",
            )

        open_orders_strategy: list[Order] = self._cache.orders_open(
            instrument_id=command.instrument_id,
            strategy_id=command.strategy_id,
        )

        if not open_orders_strategy:
            self._log.warning(f"No open orders to cancel for strategy {command.strategy_id}")
            return

        retry_manager = await self._retry_manager_pool.acquire()
        try:
            order_ids = [o.venue_order_id.value for o in open_orders_strategy if o.venue_order_id]
            response: JSON | None = await retry_manager.run(
                "cancel_all_orders",
                [command.instrument_id],
                asyncio.to_thread,
                self._http_client.cancel_orders,
                order_ids,
            )

            if not response or not retry_manager.result:
                reason_map = dict.fromkeys(order_ids, retry_manager.message)
            else:
                reason_map = response.get("not_canceled", {})

            for order_id, reason in reason_map.items():
                venue_order_id = VenueOrderId(order_id)
                client_order_id = self._cache.client_order_id(venue_order_id)
                if client_order_id:
                    self._generate_cancel_event(
                        strategy_id=command.strategy_id,
                        instrument_id=command.instrument_id,
                        client_order_id=client_order_id,
                        venue_order_id=venue_order_id,
                        reason=str(reason),
                        ts_event=self._clock.timestamp_ns(),
                    )
        finally:
            await self._retry_manager_pool.release(retry_manager)

    async def _cancel_all_global(self) -> None:
        """
        Cancel all orders for this API key using Polymarket's cancel_all endpoint.

        This cancels ALL orders across all markets and strategies. Use with caution as
        it cannot be filtered by instrument or strategy.

        Notes
        -----
        This is a "fire-and-forget" method. Order state updates are handled via WebSocket
        events. Local order state will be updated when the WebSocket receives cancel
        confirmations from Polymarket.

        """
        self._log.info("Canceling ALL orders globally via Polymarket cancel_all endpoint")

        retry_manager = await self._retry_manager_pool.acquire()
        try:
            response: JSON | None = await retry_manager.run(
                "cancel_all_global",
                [],
                asyncio.to_thread,
                self._http_client.cancel_all,
            )

            if not response or not retry_manager.result:
                self._log.error(f"Failed to cancel all orders: {retry_manager.message}")
            else:
                canceled = response.get("canceled", [])
                not_canceled = response.get("not_canceled", {})
                self._log.info(
                    f"Cancel all result: {len(canceled)} canceled, "
                    f"{len(not_canceled)} not canceled",
                )

                for order_id, reason in not_canceled.items():
                    self._log.warning(f"Order {order_id} not canceled: {reason}")
        finally:
            await self._retry_manager_pool.release(retry_manager)

    async def _cancel_market_orders(
        self,
        instrument_id: InstrumentId | None = None,
        asset_id: str = "",
    ) -> None:
        """
        Cancel orders for a specific market using Polymarket's cancel_market_orders
        endpoint.

        Parameters
        ----------
        instrument_id : InstrumentId, optional
            The instrument ID to derive the market (condition_id).
        asset_id : str, optional
            The specific asset ID (token_id) to cancel orders for.

        Notes
        -----
        This is a "fire-and-forget" method. Order state updates are handled via WebSocket
        events. Local order state will be updated when the WebSocket receives cancel
        confirmations from Polymarket.

        """
        market = ""

        if instrument_id is not None:
            market = get_polymarket_condition_id(instrument_id)

            if not asset_id:
                asset_id = get_polymarket_token_id(instrument_id)

        self._log.info(
            f"Canceling orders for market={market or 'ALL'}, asset_id={asset_id or 'ALL'}",
        )

        retry_manager = await self._retry_manager_pool.acquire()
        try:
            payload = OrderMarketCancelParams(
                market=market or None,
                asset_id=asset_id or None,
            )
            response: JSON | None = await retry_manager.run(
                "cancel_market_orders",
                [instrument_id] if instrument_id else [],
                asyncio.to_thread,
                self._http_client.cancel_market_orders,
                payload,
            )

            if not response or not retry_manager.result:
                self._log.error(f"Failed to cancel market orders: {retry_manager.message}")
            else:
                canceled = response.get("canceled", [])
                not_canceled = response.get("not_canceled", {})
                self._log.info(
                    f"Cancel market orders result: {len(canceled)} canceled, "
                    f"{len(not_canceled)} not canceled",
                )

                for order_id, reason in not_canceled.items():
                    self._log.warning(f"Order {order_id} not canceled: {reason}")
        finally:
            await self._retry_manager_pool.release(retry_manager)

    async def _submit_order(self, command: SubmitOrder) -> None:
        # [fern2 local patch] End-to-end pipeline timing anchor. Paired
        # with the strategy's ``NAUTILUS_PIPELINE strategy_submit`` log so
        # the summarizer can measure the strategy->adapter gap (Nautilus
        # risk + exec engines + message bus). Only emitted for orders that
        # carry an arb-completion tag (``arb_presigned_*`` / ``arb_batch:``)
        # to avoid the per-line cost on every normal limit re-quote.
        tags = command.order.tags
        if tags is not None and any(
            isinstance(tag, str) and (tag.startswith("arb_presigned_") or tag.startswith("arb_batch:"))
            for tag in tags
        ):
            self._log.info(
                f"NAUTILUS_PIPELINE adapter_submit client_order_id={command.order.client_order_id} "
                f"ts_ns={self._clock.timestamp_ns()}",
                LogColor.CYAN,
            )

        await self._maintain_active_market(command.instrument_id)

        order = command.order
        if order.is_closed:
            self._log.warning(f"Order {order} is already closed")
            return

        # [fern2 local patch] B2 presigned arb-completion entrypoint. When a
        # completion leg carries ``arb_presigned_*`` we try to post the
        # already-signed payloads from the adapter cache. Misses fall back to
        # the existing cold sign path below.
        presigned_info = _parse_presigned_arb_tag(order.tags)
        if presigned_info is not None:
            mode, plan_key, n_legs = presigned_info
            await self._queue_presigned_arb_order(command, order, mode, plan_key, n_legs)
            return

        # [fern2 local patch] Arb-batch coalescing entrypoint. When the order
        # carries an ``arb_batch:<burst_id>:<n_legs>`` tag we buffer it by
        # burst_id and submit all N legs together once the buffer is full
        # (or a short watchdog fires). Bypasses the per-leg sign+HTTP path
        # entirely — the dominant arb-completion latency win in PR 2.
        arb_batch_info = _parse_arb_batch_tag(order.tags)
        if arb_batch_info is not None:
            burst_id, n_legs = arb_batch_info
            await self._queue_arb_batch_order(command, order, burst_id, n_legs)
            return

        if order.is_reduce_only:
            self._log.error(
                f"Cannot submit order {order.client_order_id}: "
                "Reduce-only orders not supported on Polymarket",
                LogColor.RED,
            )
            self.generate_order_denied(
                strategy_id=order.strategy_id,
                instrument_id=order.instrument_id,
                client_order_id=order.client_order_id,
                reason="REDUCE_ONLY_NOT_SUPPORTED",
                ts_event=self._clock.timestamp_ns(),
            )
            return

        # post_only orders only supported with GTC or GTD time_in_force
        if order.is_post_only and order.time_in_force not in (TimeInForce.GTC, TimeInForce.GTD):
            self._log.error(
                f"Cannot submit order {order.client_order_id}: "
                "Post-only orders require GTC or GTD time in force",
                LogColor.RED,
            )
            self.generate_order_denied(
                strategy_id=order.strategy_id,
                instrument_id=order.instrument_id,
                client_order_id=order.client_order_id,
                reason="POST_ONLY_REQUIRES_GTC_OR_GTD",
                ts_event=self._clock.timestamp_ns(),
            )
            return

        if order.time_in_force not in VALID_POLYMARKET_TIME_IN_FORCE:
            self._log.error(
                f"Cannot submit order {order.client_order_id}: "
                f"Order time in force {order.tif_string()} not supported on Polymarket; "
                "use any of FOK, GTC, GTD, IOC",
                LogColor.RED,
            )
            self.generate_order_denied(
                strategy_id=order.strategy_id,
                instrument_id=order.instrument_id,
                client_order_id=order.client_order_id,
                reason="UNSUPPORTED_TIME_IN_FORCE",
                ts_event=self._clock.timestamp_ns(),
            )
            return

        instrument = self._cache.instrument(order.instrument_id)

        if order.order_type == OrderType.MARKET:
            await self._submit_market_order(command, instrument)
        elif order.order_type == OrderType.LIMIT:
            await self._submit_limit_order(command, instrument)
        else:
            self._log.error(
                f"Cannot submit order {order.client_order_id}: "
                f"Order type {order.type_string()} not supported on Polymarket; "
                "use either MARKET or LIMIT",
                LogColor.RED,
            )
            self.generate_order_denied(
                strategy_id=order.strategy_id,
                instrument_id=order.instrument_id,
                client_order_id=order.client_order_id,
                reason="UNSUPPORTED_ORDER_TYPE",
                ts_event=self._clock.timestamp_ns(),
            )

    # ------------------------------------------------------------------ #
    # [fern2 local patch] Arb-completion tag-coalescing batch path.      #
    # ------------------------------------------------------------------ #

    def _validate_arb_batch_order(self, order: Order) -> str | None:
        """Validate an arb-batch leg. Accepts both MARKET and LIMIT (single-
        instrument constraint that Nautilus's ``OrderList`` enforces is
        deliberately skipped — neg-risk arbs are cross-market by design)."""
        if order.is_reduce_only:
            return "REDUCE_ONLY_NOT_SUPPORTED"
        if order.is_post_only and order.time_in_force not in (TimeInForce.GTC, TimeInForce.GTD):
            return "POST_ONLY_REQUIRES_GTC_OR_GTD"
        if order.time_in_force not in VALID_POLYMARKET_TIME_IN_FORCE:
            return "UNSUPPORTED_TIME_IN_FORCE"
        if order.is_quote_quantity:
            # MARKET legs are signed as marketable limits via Rust, which
            # requires base-share quantities (matches ``_submit_market_order``).
            return "ARB_BATCH_REQUIRES_BASE_QUANTITIES"
        if self._rust_client is None:
            return "ARB_BATCH_REQUIRES_RUST_SIGNER"
        return None

    async def _queue_arb_batch_order(
        self,
        command: SubmitOrder,
        order: Order,
        burst_id: str,
        n_legs_expected: int,
    ) -> None:
        """Buffer one tagged leg; fire the batch when N have arrived."""
        denial_reason = self._validate_arb_batch_order(order)
        if denial_reason is not None:
            self._log.error(
                f"Cannot arb-batch order {order.client_order_id}: {denial_reason}",
                LogColor.RED,
            )
            self.generate_order_denied(
                strategy_id=order.strategy_id,
                instrument_id=order.instrument_id,
                client_order_id=order.client_order_id,
                reason=denial_reason,
                ts_event=self._clock.timestamp_ns(),
            )
            return

        now_ts = self._clock.timestamp()
        batch = self._arb_batch_buffer.get(burst_id)
        if batch is None:
            batch = _PendingArbBatch(
                burst_id=burst_id,
                n_legs_expected=n_legs_expected,
                created_ts=now_ts,
            )
            self._arb_batch_buffer[burst_id] = batch
        batch.pending.append((command, order, now_ts))

        if len(batch.pending) >= batch.n_legs_expected:
            # Full batch — fire immediately and cancel any pending watchdog.
            if batch.watchdog_task is not None and not batch.watchdog_task.done():
                batch.watchdog_task.cancel()
            if batch.flushed:
                return
            batch.flushed = True
            self._arb_batch_buffer.pop(burst_id, None)
            self.create_task(self._process_arb_batch(batch))
        elif batch.watchdog_task is None:
            # First leg of an incomplete batch — start the safety watchdog so
            # a missing/denied leg can't strand the buffer indefinitely.
            batch.watchdog_task = self.create_task(self._arb_batch_watchdog(burst_id))

    async def _arb_batch_watchdog(self, burst_id: str) -> None:
        await asyncio.sleep(self._arb_batch_watchdog_ms / 1000.0)
        batch = self._arb_batch_buffer.pop(burst_id, None)
        if batch is None or batch.flushed:
            return
        batch.flushed = True
        self._log.warning(
            f"Arb-batch watchdog flushing burst_id={burst_id} with "
            f"{len(batch.pending)}/{batch.n_legs_expected} legs after "
            f"{self._arb_batch_watchdog_ms}ms — submitting partial batch.",
            LogColor.YELLOW,
        )
        await self._process_arb_batch(batch)

    async def _process_arb_batch(self, batch: _PendingArbBatch) -> None:
        """Sign all buffered legs in parallel; post in ≤15-order chunks."""
        if not batch.pending:
            return

        leg_orders: list[Order] = [order for _, order, _ in batch.pending]
        n = len(leg_orders)

        # Anchor for both the per-leg ``init_to_sign_start`` and the batch
        # ``sign_order_batch`` / ``sign_and_post_order_batch`` timings.
        signing_start = self._clock.timestamp()
        for _command, order, _enq_ts in batch.pending:
            self._log_order_init_to_sign_start(order, "arb_batch", signing_start)

        signed_orders, signed_orders_args, expected_venue_order_ids = (
            await self._sign_orders_for_arb_batch_rust(leg_orders)
        )
        sign_elapsed = self._clock.timestamp() - signing_start

        # Mirror per-leg ``sign_order`` lines so the existing summarizer's
        # per-order rollup AND the burst-latency anchors keep working for
        # batch-submitted legs (all legs share the same elapsed = batch sign
        # duration; their timestamps are identical, so ``posted_spread``
        # collapses to ~0 — the visible signal that batching is in effect).
        for order in signed_orders:
            order_type_str = "market" if order.order_type == OrderType.MARKET else "limit"
            self._log.info(
                f"POLYMARKET_ORDER_TIMING sign_order client_order_id={order.client_order_id} "
                f"order_type={order_type_str} signer=rust elapsed={sign_elapsed:.3f}s "
                f"burst_id={batch.burst_id}",
                LogColor.BLUE,
            )
        self._log.info(
            f"POLYMARKET_ORDER_TIMING sign_order_batch burst_id={batch.burst_id} "
            f"signed_count={len(signed_orders)} order_count={n} signer=rust "
            f"elapsed={sign_elapsed:.3f}s",
            LogColor.BLUE,
        )

        if not signed_orders:
            self._log.warning(
                f"Arb-batch burst_id={batch.burst_id}: no orders signed; nothing to post",
            )
            return

        now_ns = self._clock.timestamp_ns()
        for order in signed_orders:
            self.generate_order_submitted(
                strategy_id=order.strategy_id,
                instrument_id=order.instrument_id,
                client_order_id=order.client_order_id,
                ts_event=now_ns,
            )

        # Chunk into ≤15-order sub-batches (Polymarket post_orders cap). For
        # typical neg-risk events N ≤ ~12 so there's always exactly one chunk;
        # the chunked path is a defensive fallback for outlier groups.
        chunks: list[tuple[list[Order], list[PostOrdersV2Args], list[VenueOrderId | None]]] = []
        for start in range(0, len(signed_orders), ARB_BATCH_MAX_ORDERS_PER_POST):
            end = start + ARB_BATCH_MAX_ORDERS_PER_POST
            chunks.append(
                (
                    signed_orders[start:end],
                    signed_orders_args[start:end],
                    expected_venue_order_ids[start:end],
                ),
            )

        # Chunks post concurrently so they all share roughly the same HTTP RTT
        # — keeps fill→last_posted close to a single round trip even when N>15.
        await asyncio.gather(
            *[
                self._post_signed_orders_batch(
                    chunk_orders,
                    chunk_args,
                    chunk_expected,
                    post_only=False,
                    timing_start=signing_start,
                )
                for chunk_orders, chunk_args, chunk_expected in chunks
            ],
        )

        # Mirror per-leg ``sign_and_post_order`` lines (one elapsed value for
        # the whole batch — same caveat as ``sign_order`` above).
        post_elapsed = self._clock.timestamp() - signing_start
        for order in signed_orders:
            venue_order_id = self._cache.venue_order_id(order.client_order_id)
            venue_oid_str = venue_order_id.value if venue_order_id is not None else "unknown"
            self._log.info(
                f"POLYMARKET_ORDER_TIMING sign_and_post_order client_order_id={order.client_order_id} "
                f"venue_order_id={venue_oid_str} elapsed={post_elapsed:.3f}s "
                f"burst_id={batch.burst_id}",
                LogColor.BLUE,
            )

    async def _sign_orders_for_arb_batch_rust(
        self,
        orders: list[Order],
    ) -> tuple[list[Order], list[PostOrdersV2Args], list[VenueOrderId | None]]:
        """Mirror of ``_sign_orders_for_batch_rust`` that dispatches MARKET
        legs to ``_sign_market_order_rust_as_limit`` (the same recipe the
        single-order MARKET path uses)."""
        async def sign_one(order: Order) -> SignedOrderV2:
            instrument = self._cache.instrument(order.instrument_id)
            if order.order_type == OrderType.MARKET:
                return await self._sign_market_order_rust_as_limit(order, instrument)
            return await self._sign_limit_order_rust(order, instrument)

        results = await asyncio.gather(
            *[sign_one(order) for order in orders],
            return_exceptions=True,
        )

        signed_orders_args: list[PostOrdersV2Args] = []
        successfully_signed_orders: list[Order] = []
        expected_venue_order_ids: list[VenueOrderId | None] = []
        for order, result in zip(orders, results, strict=True):
            if isinstance(result, BaseException):
                self._log.error(
                    f"Failed to sign arb-batch leg {order.client_order_id} via Rust: {result}",
                    LogColor.RED,
                )
                self.generate_order_rejected(
                    strategy_id=order.strategy_id,
                    instrument_id=order.instrument_id,
                    client_order_id=order.client_order_id,
                    reason=f"Order signing failed: {result}",
                    ts_event=self._clock.timestamp_ns(),
                )
                continue
            if order.order_type == OrderType.MARKET:
                # Bounded arb-completion MARKET orders must not rest; post as FAK.
                poly_order_type = (
                    PolyOrderType.FAK
                    if _parse_arb_completion_price_cap(order.tags) is not None
                    else PolyOrderType.GTD
                )
            else:
                poly_order_type = convert_tif_to_polymarket_order_type(order.time_in_force)
            signed_orders_args.append(
                PostOrdersV2Args(order=result, orderType=poly_order_type),
            )
            successfully_signed_orders.append(order)
            instrument = self._cache.instrument(order.instrument_id)
            expected_venue_order_ids.append(
                self._expected_venue_order_id(
                    result,
                    neg_risk=self._get_neg_risk_for_instrument(instrument),
                ),
            )
        return successfully_signed_orders, signed_orders_args, expected_venue_order_ids

    # ------------------------------------------------------------------ #
    # [fern2 local patch] B2 presigned arb-completion path.              #
    # ------------------------------------------------------------------ #

    def _handle_presign_request(self, request: dict) -> None:
        """Message-bus endpoint used by the strategy to ensure a plan is signed.

        The message bus may call this from a strategy/executor thread, so the
        actual signing work is scheduled onto the execution client's asyncio
        loop and never blocks ``process_main``.
        """
        try:
            self._loop.call_soon_threadsafe(
                lambda: self.create_task(self._ensure_presigned_arb_batch(request)),
            )
        except Exception as e:
            self._log.warning(f"Failed to schedule presign request: {e}", LogColor.YELLOW)

    @staticmethod
    def _presign_leg_spec_from_dict(raw: dict) -> _PresignedArbLegSpec:
        return _PresignedArbLegSpec(
            instrument_id=InstrumentId.from_str(str(raw["instrument_id"])),
            side=str(raw.get("side", "BUY")).upper(),
            size=float(raw["size"]),
            tick_size=str(raw["tick_size"]),
            neg_risk=bool(raw.get("neg_risk", False)),
            boundary_price=float(raw["boundary_price"]),
        )

    @staticmethod
    def _presign_specs_by_instrument(
        specs: list[_PresignedArbLegSpec],
    ) -> dict[InstrumentId, _PresignedArbLegSpec]:
        return {spec.instrument_id: spec for spec in specs}

    @staticmethod
    def _presign_live_matches_specs(
        live: _LivePresignedArbBatch,
        specs: list[_PresignedArbLegSpec],
        tolerance: float,
    ) -> bool:
        if len(live.leg_specs) != len(specs):
            return False
        current_by_iid = PolymarketExecutionClient._presign_specs_by_instrument(specs)
        for signed_spec in live.leg_specs:
            current = current_by_iid.get(signed_spec.instrument_id)
            if current is None:
                return False
            if (
                current.side != signed_spec.side
                or current.tick_size != signed_spec.tick_size
                or current.neg_risk != signed_spec.neg_risk
                or abs(current.boundary_price - signed_spec.boundary_price) > 1e-12
            ):
                return False
            if abs(current.size - signed_spec.size) > tolerance:
                return False
        return True

    async def _ensure_presigned_arb_batch(self, request: dict) -> None:
        if self._rust_client is None:
            self._log.info(
                "POLYMARKET_PRESIGN2 miss plan_key="
                + str(request.get("plan_key", "unknown"))
                + " reason=no_rust_signer submit_path=prepare",
                LogColor.CYAN,
            )
            return

        plan_key = str(request["plan_key"])
        leg_specs = [self._presign_leg_spec_from_dict(raw) for raw in request.get("leg_specs", [])]
        if not leg_specs:
            return

        now = self._clock.timestamp()
        effective_ttl_seconds = float(request.get("effective_ttl_seconds", 300))
        refresh_after_seconds = float(request.get("refresh_after_seconds", 180))
        stale_after_seconds = float(request.get("stale_after_seconds", 285))
        size_tolerance = float(request.get("size_tolerance", 1.0))
        cache_max_plans = int(request.get("cache_max_plans", 256))
        if not (refresh_after_seconds < stale_after_seconds < effective_ttl_seconds):
            self._log.warning(
                f"POLYMARKET_PRESIGN2 miss plan_key={plan_key} "
                f"reason=invalid_ttl_config submit_path=prepare "
                f"refresh_after_seconds={refresh_after_seconds:.3f} "
                f"stale_after_seconds={stale_after_seconds:.3f} "
                f"effective_ttl_seconds={effective_ttl_seconds:.3f}",
                LogColor.YELLOW,
            )
            return

        plan = self._presigned_arb_plans.get(plan_key)
        if plan is None:
            plan = _PresignedArbPlan(plan_key=plan_key, last_used=now)
            self._presigned_arb_plans[plan_key] = plan
        else:
            plan.last_used = now
            self._presigned_arb_plans.move_to_end(plan_key)

        live = plan.live
        if live is not None:
            age = now - live.signed_at
            if (
                age < refresh_after_seconds
                and self._presign_live_matches_specs(live, leg_specs, size_tolerance)
            ):
                self._evict_presign_cache_if_needed(cache_max_plans)
                return

        if plan.refresh_in_progress:
            return

        plan.refresh_in_progress = True
        generation = plan.generation + 1
        start = self._clock.timestamp()
        previous_age_ms = None if live is None else max(0.0, (start - live.signed_at) * 1000)
        self._log.info(
            f"POLYMARKET_PRESIGN2 prepare_start plan_key={plan_key} "
            f"n_legs={len(leg_specs)} generation={generation}",
            LogColor.CYAN,
        )
        try:
            signed_orders_args, expected_venue_order_ids = await self._sign_presigned_arb_legs(
                leg_specs,
                effective_ttl_seconds=effective_ttl_seconds,
            )
            signed_at = self._clock.timestamp()
            live_generation = _LivePresignedArbBatch(
                plan_key=plan_key,
                leg_specs=list(leg_specs),
                signed_orders_args=signed_orders_args,
                expected_venue_order_ids=expected_venue_order_ids,
                signed_at=signed_at,
                effective_expires_at=signed_at + effective_ttl_seconds,
                stale_after_seconds=stale_after_seconds,
                size_tolerance=size_tolerance,
                generation=generation,
            )
            plan.live = live_generation
            plan.generation = generation
            plan.last_sign_failed = False
            plan.last_used = signed_at
            self._presigned_arb_plans.move_to_end(plan_key)
            elapsed_ms = (self._clock.timestamp() - start) * 1000
            age_ms_text = "none" if previous_age_ms is None else f"{previous_age_ms:.2f}"
            self._log.info(
                f"POLYMARKET_PRESIGN2 refresh plan_key={plan_key} "
                f"age_ms={age_ms_text} elapsed_ms={elapsed_ms:.2f} "
                f"result=success n_legs={len(leg_specs)} generation={generation} "
                f"effective_expires_at={live_generation.effective_expires_at:.3f}",
                LogColor.CYAN,
            )
        except Exception as e:
            elapsed_ms = (self._clock.timestamp() - start) * 1000
            age_ms_text = "none" if previous_age_ms is None else f"{previous_age_ms:.2f}"
            self._log.warning(
                f"POLYMARKET_PRESIGN2 refresh plan_key={plan_key} generation={generation} "
                f"age_ms={age_ms_text} elapsed_ms={elapsed_ms:.2f} "
                f"result=failure reason={type(e).__name__}",
                LogColor.YELLOW,
            )
            plan.last_sign_failed = True
        finally:
            plan.refresh_in_progress = False
            self._evict_presign_cache_if_needed(cache_max_plans)

    async def _sign_presigned_arb_legs(
        self,
        leg_specs: list[_PresignedArbLegSpec],
        *,
        effective_ttl_seconds: float,
    ) -> tuple[list[PostOrdersV2Args], list[VenueOrderId | None]]:
        raw_expiration = int(
            self._clock.timestamp() + POLYMARKET_GTD_SECURITY_BUFFER_SECS + effective_ttl_seconds,
        )

        async def sign_one(spec: _PresignedArbLegSpec) -> SignedOrderV2:
            signed_order_json = await self._rust_client.create_order(
                get_polymarket_token_id(spec.instrument_id),
                spec.side,
                spec.size,
                spec.boundary_price,
                raw_expiration,
                float(spec.tick_size),
                spec.neg_risk,
            )
            return self._build_signed_order_v2_from_rust_json(signed_order_json)

        results = await asyncio.gather(
            *[sign_one(spec) for spec in leg_specs],
            return_exceptions=True,
        )
        signed_orders_args: list[PostOrdersV2Args] = []
        expected_venue_order_ids: list[VenueOrderId | None] = []
        errors: list[str] = []
        for spec, result in zip(leg_specs, results, strict=True):
            if isinstance(result, BaseException):
                errors.append(f"{spec.instrument_id}: {result}")
                continue
            signed_orders_args.append(PostOrdersV2Args(order=result, orderType=PolyOrderType.FAK))
            expected_venue_order_ids.append(
                self._expected_venue_order_id(result, neg_risk=spec.neg_risk),
            )

        if errors:
            raise RuntimeError("; ".join(errors))
        return signed_orders_args, expected_venue_order_ids

    def _evict_presign_cache_if_needed(self, max_plans: int) -> None:
        if max_plans <= 0:
            return
        now = self._clock.timestamp()
        while len(self._presigned_arb_plans) > max_plans:
            evict_key = None
            evict_reason = "lru"
            for key, plan in self._presigned_arb_plans.items():
                live = plan.live
                if plan.posting_count > 0 or plan.refresh_in_progress:
                    continue
                if live is None:
                    evict_key = key
                    evict_reason = "empty"
                    break
                if now - live.signed_at > live.stale_after_seconds:
                    evict_key = key
                    evict_reason = "stale"
                    break
            if evict_key is None:
                for key, plan in self._presigned_arb_plans.items():
                    if plan.posting_count == 0 and not plan.refresh_in_progress:
                        evict_key = key
                        break
            if evict_key is None:
                return
            self._presigned_arb_plans.pop(evict_key, None)
            self._log.info(
                f"POLYMARKET_PRESIGN2 evict plan_key={evict_key} reason={evict_reason}",
                LogColor.CYAN,
            )

    async def _queue_presigned_arb_order(
        self,
        command: SubmitOrder,
        order: Order,
        mode: str,
        plan_key: str,
        n_legs_expected: int,
    ) -> None:
        denial_reason = None
        if order.is_reduce_only:
            denial_reason = "REDUCE_ONLY_NOT_SUPPORTED"
        elif order.is_post_only and order.time_in_force not in (TimeInForce.GTC, TimeInForce.GTD):
            denial_reason = "POST_ONLY_REQUIRES_GTC_OR_GTD"
        elif order.time_in_force not in VALID_POLYMARKET_TIME_IN_FORCE:
            denial_reason = "UNSUPPORTED_TIME_IN_FORCE"
        elif order.is_quote_quantity:
            denial_reason = "PRESIGNED_ARB_REQUIRES_BASE_QUANTITIES"
        elif order.order_type not in (OrderType.MARKET, OrderType.LIMIT):
            denial_reason = "UNSUPPORTED_ORDER_TYPE"

        if denial_reason is not None:
            self.generate_order_denied(
                strategy_id=order.strategy_id,
                instrument_id=order.instrument_id,
                client_order_id=order.client_order_id,
                reason=denial_reason,
                ts_event=self._clock.timestamp_ns(),
            )
            return

        # Current strategy gates should prevent concurrent same-plan bursts.
        # If execute_one_arb_only/active_arbs semantics change, include a
        # burst disambiguator in this key to avoid sharing one buffer.
        buffer_key = f"{mode}:{plan_key}"
        now_ts = self._clock.timestamp()
        batch = self._presigned_arb_buffer.get(buffer_key)
        if batch is None:
            batch = _PendingPresignedArbBatch(
                mode=mode,
                plan_key=plan_key,
                n_legs_expected=n_legs_expected,
                created_ts=now_ts,
            )
            self._presigned_arb_buffer[buffer_key] = batch
        batch.pending.append((command, order, now_ts))

        # [fern2 local patch] Hot-path timing anchor 1/5: leg buffered.
        # Carries cid + plan_key so the summarizer can join adapter-side
        # PRESIGN2 stages back to per-burst leg client_order_ids.
        self._log.info(
            f"POLYMARKET_PRESIGN2 buffer_add plan_key={plan_key} "
            f"client_order_id={order.client_order_id} mode={mode} "
            f"buffered_count={len(batch.pending)} n_legs_expected={n_legs_expected} "
            f"ts_ns={self._clock.timestamp_ns()}",
            LogColor.CYAN,
        )

        if len(batch.pending) >= batch.n_legs_expected:
            if batch.watchdog_task is not None and not batch.watchdog_task.done():
                batch.watchdog_task.cancel()
            if batch.flushed:
                return
            batch.flushed = True
            self._presigned_arb_buffer.pop(buffer_key, None)
            # [fern2 local patch] Hot-path timing anchor 2/5: buffer reached
            # n_legs_expected and is about to dispatch _process_presigned_arb_batch.
            self._log.info(
                f"POLYMARKET_PRESIGN2 buffer_full plan_key={plan_key} mode={mode} "
                f"trigger=n_legs n_legs={len(batch.pending)} "
                f"n_legs_expected={n_legs_expected} ts_ns={self._clock.timestamp_ns()}",
                LogColor.CYAN,
            )
            self.create_task(self._process_presigned_arb_batch(batch))
        elif batch.watchdog_task is None:
            batch.watchdog_task = self.create_task(self._presigned_arb_watchdog(buffer_key))

    async def _presigned_arb_watchdog(self, buffer_key: str) -> None:
        await asyncio.sleep(self._presigned_arb_watchdog_ms / 1000.0)
        batch = self._presigned_arb_buffer.pop(buffer_key, None)
        if batch is None or batch.flushed:
            return
        batch.flushed = True
        self._log.warning(
            f"Presigned arb watchdog flushing plan_key={batch.plan_key} "
            f"with {len(batch.pending)}/{batch.n_legs_expected} legs after "
            f"{self._presigned_arb_watchdog_ms}ms.",
            LogColor.YELLOW,
        )
        # [fern2 local patch] Hot-path timing anchor 2/5 (watchdog branch):
        # buffer flushed by watchdog timeout, not by reaching n_legs_expected.
        self._log.info(
            f"POLYMARKET_PRESIGN2 buffer_full plan_key={batch.plan_key} mode={batch.mode} "
            f"trigger=watchdog n_legs={len(batch.pending)} "
            f"n_legs_expected={batch.n_legs_expected} ts_ns={self._clock.timestamp_ns()}",
            LogColor.CYAN,
        )
        await self._process_presigned_arb_batch(batch)

    @staticmethod
    def _presigned_orders_match_live(
        orders: list[Order],
        live: _LivePresignedArbBatch,
    ) -> tuple[bool, str]:
        if len(orders) != len(live.leg_specs):
            return False, "n_legs_mismatch"
        specs_by_iid = PolymarketExecutionClient._presign_specs_by_instrument(live.leg_specs)
        for order in orders:
            spec = specs_by_iid.get(order.instrument_id)
            if spec is None:
                return False, "plan_mismatch"
            if order_side_to_str(order.side).upper() != spec.side:
                return False, "plan_mismatch"
            if abs(float(order.quantity) - spec.size) > live.size_tolerance:
                return False, "size_mismatch"
        return True, "ok"

    def _align_presigned_live_to_orders(
        self,
        orders: list[Order],
        live: _LivePresignedArbBatch,
    ) -> tuple[list[PostOrdersV2Args], list[VenueOrderId | None]]:
        index_by_iid = {spec.instrument_id: idx for idx, spec in enumerate(live.leg_specs)}
        signed_orders_args: list[PostOrdersV2Args] = []
        expected_venue_order_ids: list[VenueOrderId | None] = []
        for order in orders:
            idx = index_by_iid[order.instrument_id]
            signed_orders_args.append(live.signed_orders_args[idx])
            expected_venue_order_ids.append(live.expected_venue_order_ids[idx])
        return signed_orders_args, expected_venue_order_ids

    async def _process_presigned_arb_batch(self, batch: _PendingPresignedArbBatch) -> None:
        # [fern2 local patch] Hot-path timing anchor 3/5: task scheduled by
        # buffer_full or watchdog has now started running. The gap from the
        # preceding buffer_full ts_ns measures asyncio.create_task scheduling
        # latency (often the dominant chunk of the 100+ms hot-path delay).
        self._log.info(
            f"POLYMARKET_PRESIGN2 batch_process_enter plan_key={batch.plan_key} mode={batch.mode} "
            f"n_legs={len(batch.pending)} ts_ns={self._clock.timestamp_ns()}",
            LogColor.CYAN,
        )

        if not batch.pending:
            return

        orders = [order for _command, order, _enq_ts in batch.pending]
        plan = self._presigned_arb_plans.get(batch.plan_key)
        live = plan.live if plan is not None else None
        reason = None
        if plan is None:
            reason = "no_cache"
        elif live is None:
            reason = (
                "sign_in_progress"
                if plan.refresh_in_progress
                else "last_sign_failed"
                if plan.last_sign_failed
                else "no_cache"
            )
        elif self._clock.timestamp() - live.signed_at > live.stale_after_seconds:
            reason = "stale"
        else:
            matched, match_reason = self._presigned_orders_match_live(orders, live)
            if not matched:
                reason = match_reason

        if reason is not None:
            # [fern2 local patch] Hot-path timing anchor 4/5 (miss branch):
            # plan lookup + validation finished, falling back to cold path.
            self._log.info(
                f"POLYMARKET_PRESIGN2 batch_lookup_done plan_key={batch.plan_key} mode={batch.mode} "
                f"decision=miss reason={reason} ts_ns={self._clock.timestamp_ns()}",
                LogColor.CYAN,
            )
            self._log.info(
                f"POLYMARKET_PRESIGN2 miss plan_key={batch.plan_key} "
                f"reason={reason} submit_path=presigned_{batch.mode}",
                LogColor.CYAN,
            )
            await self._fallback_presigned_arb_batch(batch)
            return

        assert plan is not None and live is not None
        now = self._clock.timestamp()
        plan.last_used = now
        plan.posting_count += 1
        self._presigned_arb_plans.move_to_end(batch.plan_key)
        signed_orders_args, expected_venue_order_ids = self._align_presigned_live_to_orders(
            orders,
            live,
        )
        age_ms = (now - live.signed_at) * 1000
        # [fern2 local patch] Hot-path timing anchor 4/5 (hit branch):
        # plan lookup + validation + alignment finished; about to emit
        # OrderSubmitted events and POST.
        self._log.info(
            f"POLYMARKET_PRESIGN2 batch_lookup_done plan_key={batch.plan_key} mode={batch.mode} "
            f"decision=hit ts_ns={self._clock.timestamp_ns()}",
            LogColor.CYAN,
        )
        self._log.info(
            f"POLYMARKET_PRESIGN2 hit plan_key={batch.plan_key} age_ms={age_ms:.2f} "
            f"n_legs={len(orders)} submit_path=presigned_{batch.mode}",
            LogColor.CYAN,
        )

        timing_start = self._clock.timestamp()
        for order in orders:
            order_type_str = "market" if order.order_type == OrderType.MARKET else "limit"
            self._log.info(
                f"POLYMARKET_ORDER_TIMING sign_order client_order_id={order.client_order_id} "
                f"order_type={order_type_str} signer=presigned elapsed=0.000s "
                f"plan_key={batch.plan_key} generation={live.generation}",
                LogColor.BLUE,
            )

        now_ns = self._clock.timestamp_ns()
        for order in orders:
            self.generate_order_submitted(
                strategy_id=order.strategy_id,
                instrument_id=order.instrument_id,
                client_order_id=order.client_order_id,
                ts_event=now_ns,
            )

        # [fern2 local patch] Hot-path timing anchor 5/5: ``post_signed_orders_batch``
        # / ``asyncio.gather(_post_signed_order)`` about to start. Gap from
        # batch_lookup_done measures the per-leg sign_order log loop + the
        # generate_order_submitted loop.
        self._log.info(
            f"POLYMARKET_PRESIGN2 batch_post_start plan_key={batch.plan_key} mode={batch.mode} "
            f"n_legs={len(orders)} ts_ns={self._clock.timestamp_ns()}",
            LogColor.CYAN,
        )

        try:
            if batch.mode == "batch":
                await self._post_signed_orders_batch(
                    orders,
                    signed_orders_args,
                    expected_venue_order_ids,
                    post_only=False,
                    timing_start=timing_start,
                )
                post_elapsed = self._clock.timestamp() - timing_start
                for order in orders:
                    venue_order_id = self._cache.venue_order_id(order.client_order_id)
                    venue_oid_str = venue_order_id.value if venue_order_id is not None else "unknown"
                    self._log.info(
                        "POLYMARKET_ORDER_TIMING sign_and_post_order "
                        f"client_order_id={order.client_order_id} venue_order_id={venue_oid_str} "
                        f"elapsed={post_elapsed:.3f}s plan_key={batch.plan_key} generation={live.generation}",
                        LogColor.BLUE,
                    )
            else:
                await asyncio.gather(
                    *[
                        self._post_signed_order(
                            order,
                            signed_arg.order,
                            order_type_override=PolyOrderType.FAK,
                            expected_venue_order_id=expected_venue_order_id,
                            timing_start=timing_start,
                        )
                        for order, signed_arg, expected_venue_order_id in zip(
                            orders,
                            signed_orders_args,
                            expected_venue_order_ids,
                            strict=True,
                        )
                    ],
                )
        except Exception as e:
            self._log.warning(
                f"POLYMARKET_PRESIGN2 post_failure plan_key={batch.plan_key} "
                f"reason={type(e).__name__} submit_path=presigned_{batch.mode}",
                LogColor.YELLOW,
            )
            raise
        finally:
            # Signed Polymarket orders are single-use: even FAK orders that do
            # not fill may still have been accepted by the API. Force the next
            # matching plan to refresh instead of reusing the same order hashes.
            plan.live = None
            plan.posting_count -= 1

    async def _fallback_presigned_arb_batch(self, batch: _PendingPresignedArbBatch) -> None:
        if batch.mode == "batch":
            burst_info = _parse_arb_batch_tag(batch.pending[0][1].tags)
            burst_id = burst_info[0] if burst_info is not None else batch.plan_key
            await self._process_arb_batch(
                _PendingArbBatch(
                    burst_id=burst_id,
                    n_legs_expected=batch.n_legs_expected,
                    pending=list(batch.pending),
                    created_ts=batch.created_ts,
                    flushed=True,
                ),
            )
            return

        await asyncio.gather(
            *[self._submit_presigned_fallback_individual(command, order) for command, order, _ in batch.pending],
        )

    async def _submit_presigned_fallback_individual(self, command: SubmitOrder, order: Order) -> None:
        instrument = self._cache.instrument(order.instrument_id)
        if order.order_type == OrderType.MARKET:
            await self._submit_market_order(command, instrument)
        elif order.order_type == OrderType.LIMIT:
            await self._submit_limit_order(command, instrument)
        else:
            self.generate_order_denied(
                strategy_id=order.strategy_id,
                instrument_id=order.instrument_id,
                client_order_id=order.client_order_id,
                reason="UNSUPPORTED_ORDER_TYPE",
                ts_event=self._clock.timestamp_ns(),
            )

    def _validate_order_for_batch(self, order: Order) -> str | None:
        """
        Validate an order for batch submission.

        Returns None if valid, or an error reason string if invalid.

        """
        if order.is_reduce_only:
            return "REDUCE_ONLY_NOT_SUPPORTED"

        if order.is_post_only and order.time_in_force not in (TimeInForce.GTC, TimeInForce.GTD):
            return "POST_ONLY_REQUIRES_GTC_OR_GTD"

        if order.time_in_force not in VALID_POLYMARKET_TIME_IN_FORCE:
            return "UNSUPPORTED_TIME_IN_FORCE"

        if order.order_type != OrderType.LIMIT:
            return "BATCH_ONLY_SUPPORTS_LIMIT_ORDERS"

        if order.is_quote_quantity:
            return "UNSUPPORTED_QUOTE_QUANTITY"

        return None

    async def _submit_order_list(self, command: SubmitOrderList) -> None:
        """
        Submit a batch of orders to Polymarket using the post_orders endpoint.

        Parameters
        ----------
        command : SubmitOrderList
            The command containing the list of orders to submit.

        """
        order_list = command.order_list
        orders = order_list.orders

        if not orders:
            self._log.warning("Order list is empty, nothing to submit")
            return

        # Filter out closed orders
        orders = [order for order in orders if not order.is_closed]
        if not orders:
            return

        # Validate all orders before processing
        valid_orders = []

        for order in orders:
            denial_reason = self._validate_order_for_batch(order)
            if denial_reason:
                self._log.error(
                    f"Cannot submit order {order.client_order_id}: {denial_reason}",
                    LogColor.RED,
                )
                self.generate_order_denied(
                    strategy_id=order.strategy_id,
                    instrument_id=order.instrument_id,
                    client_order_id=order.client_order_id,
                    reason=denial_reason,
                    ts_event=self._clock.timestamp_ns(),
                )
                continue
            valid_orders.append(order)

        if not valid_orders:
            self._log.warning("No valid orders to submit after validation")
            return

        self._log.info(f"Submitting batch of {len(valid_orders)} orders to Polymarket")

        for post_only in (False, True):
            batch_orders = [order for order in valid_orders if order.is_post_only == post_only]
            if not batch_orders:
                continue
            await self._submit_valid_orders_batch(batch_orders, post_only=post_only)

    async def _submit_valid_orders_batch(
        self,
        orders: list[Order],
        post_only: bool,
    ) -> None:
        # Maintain active markets for all orders
        for order in orders:
            await self._maintain_active_market(order.instrument_id)

        # Sign all orders (individual failures are rejected during signing)
        timing_start = self._clock.timestamp()
        (
            signed_orders,
            signed_orders_args,
            expected_venue_order_ids,
        ) = await self._sign_orders_for_batch(orders)

        if not signed_orders:
            self._log.warning("No orders successfully signed for batch submission")
            return

        # Generate submitted events only for successfully signed orders
        now_ns = self._clock.timestamp_ns()
        for order in signed_orders:
            self.generate_order_submitted(
                strategy_id=order.strategy_id,
                instrument_id=order.instrument_id,
                client_order_id=order.client_order_id,
                ts_event=now_ns,
            )

        # Submit batch
        await self._post_signed_orders_batch(
            signed_orders,
            signed_orders_args,
            expected_venue_order_ids,
            post_only=post_only,
            timing_start=timing_start,
        )

    async def _sign_orders_for_batch(
        self,
        orders: list[Order],
    ) -> tuple[list[Order], list[PostOrdersV2Args], list[VenueOrderId | None]]:
        """
        Sign multiple orders for batch submission.

        When a Rust client is configured, all orders are signed in parallel via
        ``asyncio.gather``. Otherwise orders are signed sequentially with the
        Python client. In both cases, individual signing failures reject only
        that order and are excluded from the returned lists.

        Returns
        -------
        tuple[list[Order], list[PostOrdersV2Args], list[VenueOrderId | None]]
            Tuple of (successfully signed orders, signed order args, expected venue order IDs).
            Orders that fail to sign are rejected and excluded from the result.

        """
        signing_start = self._clock.timestamp()

        if self._rust_client is not None:
            successfully_signed_orders, signed_orders_args, expected_venue_order_ids = (
                await self._sign_orders_for_batch_rust(orders)
            )
            signing_backend = "rust"
        else:
            successfully_signed_orders, signed_orders_args, expected_venue_order_ids = (
                await self._sign_orders_for_batch_python(orders)
            )
            signing_backend = "python"

        interval = self._clock.timestamp() - signing_start
        self._log.info(
            "POLYMARKET_ORDER_TIMING sign_order_batch "
            f"signed_count={len(successfully_signed_orders)} "
            f"order_count={len(orders)} signer={signing_backend} elapsed={interval:.3f}s",
            LogColor.BLUE,
        )

        return successfully_signed_orders, signed_orders_args, expected_venue_order_ids

    async def _sign_orders_for_batch_python(
        self,
        orders: list[Order],
    ) -> tuple[list[Order], list[PostOrdersV2Args], list[VenueOrderId | None]]:
        signed_orders_args: list[PostOrdersV2Args] = []
        successfully_signed_orders: list[Order] = []
        expected_venue_order_ids: list[VenueOrderId | None] = []

        for order in orders:
            try:
                instrument = self._cache.instrument(order.instrument_id)
                neg_risk = self._get_neg_risk_for_instrument(instrument)

                order_args = OrderArgsV2(
                    price=float(order.price),
                    token_id=get_polymarket_token_id(order.instrument_id),
                    size=float(order.quantity),
                    side=order_side_to_str(order.side),
                    expiration=self._get_expiration_secs(order),
                    builder_code=POLYMARKET_NAUTILUS_BUILDER_CODE,
                )

                signed_order = await asyncio.to_thread(
                    self._http_client.create_order,
                    order_args,
                    options=self._create_order_options(instrument),
                )

                order_type = convert_tif_to_polymarket_order_type(order.time_in_force)
                signed_orders_args.append(
                    PostOrdersV2Args(
                        order=signed_order,
                        orderType=order_type,
                    ),
                )
                successfully_signed_orders.append(order)
                expected_venue_order_ids.append(
                    self._expected_venue_order_id(signed_order, neg_risk=neg_risk),
                )
            except Exception as e:
                self._log.error(
                    f"Failed to sign order {order.client_order_id}: {e}",
                    LogColor.RED,
                )
                self.generate_order_rejected(
                    strategy_id=order.strategy_id,
                    instrument_id=order.instrument_id,
                    client_order_id=order.client_order_id,
                    reason=f"Order signing failed: {e}",
                    ts_event=self._clock.timestamp_ns(),
                )

        return successfully_signed_orders, signed_orders_args, expected_venue_order_ids

    async def _sign_orders_for_batch_rust(
        self,
        orders: list[Order],
    ) -> tuple[list[Order], list[PostOrdersV2Args], list[VenueOrderId | None]]:
        async def sign_one(order: Order) -> SignedOrderV2:
            instrument = self._cache.instrument(order.instrument_id)
            return await self._sign_limit_order_rust(order, instrument)

        results = await asyncio.gather(
            *[sign_one(order) for order in orders],
            return_exceptions=True,
        )

        signed_orders_args: list[PostOrdersV2Args] = []
        successfully_signed_orders: list[Order] = []
        expected_venue_order_ids: list[VenueOrderId | None] = []
        for order, result in zip(orders, results, strict=True):
            if isinstance(result, BaseException):
                self._log.error(
                    f"Failed to sign order {order.client_order_id} via Rust: {result}",
                    LogColor.RED,
                )
                self.generate_order_rejected(
                    strategy_id=order.strategy_id,
                    instrument_id=order.instrument_id,
                    client_order_id=order.client_order_id,
                    reason=f"Order signing failed: {result}",
                    ts_event=self._clock.timestamp_ns(),
                )
                continue

            order_type = convert_tif_to_polymarket_order_type(order.time_in_force)
            signed_orders_args.append(
                PostOrdersV2Args(
                    order=result,
                    orderType=order_type,
                ),
            )
            successfully_signed_orders.append(order)
            instrument = self._cache.instrument(order.instrument_id)
            expected_venue_order_ids.append(
                self._expected_venue_order_id(
                    result,
                    neg_risk=self._get_neg_risk_for_instrument(instrument),
                ),
            )

        return successfully_signed_orders, signed_orders_args, expected_venue_order_ids

    async def _post_signed_orders_batch(
        self,
        orders: list[Order],
        signed_orders_args: list[PostOrdersV2Args],
        expected_venue_order_ids: list[VenueOrderId | None],
        post_only: bool = False,
        timing_start: float | None = None,
    ) -> None:
        """
        Post a batch of signed orders to Polymarket.
        """
        retry_manager = await self._retry_manager_pool.acquire()
        try:
            client_order_ids = [order.client_order_id for order in orders]
            if timing_start is not None:
                elapsed = self._clock.timestamp() - timing_start
                client_order_ids_text = ",".join(str(order.client_order_id) for order in orders)
                self._log.info(
                    "POLYMARKET_ORDER_TIMING post_orders_start "
                    f"order_count={len(orders)} client_order_ids={client_order_ids_text} "
                    f"elapsed={elapsed:.3f}s",
                    LogColor.BLUE,
                )
            response = await retry_manager.run(
                "submit_orders_batch",
                client_order_ids,
                asyncio.to_thread,
                self._http_client.post_orders,
                signed_orders_args,
                post_only=post_only,
            )

            if response is None:
                if self._is_unknown_submit_result(retry_manager.last_exception):
                    self._handle_unknown_batch_submit_result(
                        orders,
                        expected_venue_order_ids,
                        str(retry_manager.message),
                    )
                else:
                    self._reject_all_orders(orders, str(retry_manager.message))
                return

            self._process_batch_response(orders, response)
            if timing_start is not None:
                elapsed = self._clock.timestamp() - timing_start
                self._log.info(
                    "POLYMARKET_ORDER_TIMING sign_and_post_order_batch "
                    f"order_count={len(orders)} elapsed={elapsed:.3f}s",
                    LogColor.BLUE,
                )

        except Exception as e:
            self._log.error(f"Error submitting order batch: {e}")
            self._reject_all_orders(orders, str(e))
        finally:
            await self._retry_manager_pool.release(retry_manager)

    def _handle_unknown_batch_submit_result(
        self,
        orders: list[Order],
        expected_venue_order_ids: list[VenueOrderId | None],
        reason: str,
    ) -> None:
        for order, expected_venue_order_id in zip(
            orders,
            expected_venue_order_ids,
            strict=False,
        ):
            self._handle_unknown_submit_result(order, expected_venue_order_id, reason)

    def _reject_all_orders(self, orders: list[Order], reason: str) -> None:
        """
        Generate rejection events for all orders.
        """
        for order in orders:
            self.generate_order_rejected(
                strategy_id=order.strategy_id,
                instrument_id=order.instrument_id,
                client_order_id=order.client_order_id,
                reason=reason,
                ts_event=self._clock.timestamp_ns(),
            )

    def _process_batch_response(self, orders: list[Order], response: list) -> None:
        """
        Process the response from a batch order submission.

        If response length doesn't match orders, remaining orders are rejected.

        """
        if len(response) != len(orders):
            self._log.warning(
                f"Response length ({len(response)}) != orders length ({len(orders)}). "
                "Some orders may not have been processed.",
            )
            # Reject any orders beyond the response length
            for order in orders[len(response) :]:
                self.generate_order_rejected(
                    strategy_id=order.strategy_id,
                    instrument_id=order.instrument_id,
                    client_order_id=order.client_order_id,
                    reason="Order not included in API response",
                    ts_event=self._clock.timestamp_ns(),
                )

        for order, result in zip(orders, response, strict=False):
            if result.get("success"):
                venue_order_id = VenueOrderId(result["orderID"])
                self._cache.add_venue_order_id(order.client_order_id, venue_order_id)

                # Signal order event
                event = self._ack_events_order.get(venue_order_id)
                if event:
                    event.set()

                # Signal trade event
                trade_event = self._ack_events_trade.get(venue_order_id)
                if trade_event:
                    trade_event.set()

                # Check if cancel was requested during the HTTP round-trip
                if order.is_pending_cancel:
                    self._log.info(
                        f"Order {order.client_order_id!r} is pending cancel, "
                        f"issuing deferred cancel for {venue_order_id!r}",
                    )
                    self.create_task(
                        self._execute_deferred_cancel(order, venue_order_id),
                    )
                else:
                    self._log.debug(
                        f"Order {order.client_order_id} accepted, venue_order_id={venue_order_id}",
                    )
            else:
                reason = result.get("errorMsg", "Unknown error")
                self.generate_order_rejected(
                    strategy_id=order.strategy_id,
                    instrument_id=order.instrument_id,
                    client_order_id=order.client_order_id,
                    reason=reason,
                    ts_event=self._clock.timestamp_ns(),
                )
                self._log.warning(f"Order {order.client_order_id} rejected: {reason}")

    def _deny_market_order_quantity(self, order: Order, reason: str) -> None:
        self._log.error(
            f"Cannot submit market order {order.client_order_id}: {reason}",
            LogColor.RED,
        )
        self.generate_order_denied(
            strategy_id=order.strategy_id,
            instrument_id=order.instrument_id,
            client_order_id=order.client_order_id,
            reason=reason,
            ts_event=self._clock.timestamp_ns(),
        )

    def _cached_collateral_balance_pusd(self) -> float:
        return self._collateral_balance_pusd if self._collateral_balance_pusd is not None else 0.0

    async def _submit_market_order(self, command: SubmitOrder, instrument) -> None:
        self._log.debug("Creating Polymarket order", LogColor.MAGENTA)

        order = command.order

        # The Rust path signs as a base-share marketable limit, so the V2 quote
        # vs base requirements only apply to the Python ``create_market_order``
        # branch.
        if self._rust_client is None:
            if order.side == OrderSide.BUY:
                if not order.is_quote_quantity:
                    self._deny_market_order_quantity(
                        order,
                        "Polymarket market BUY orders require quote-denominated quantities; "
                        "resubmit with `quote_quantity=True`",
                    )
                    return
            else:
                if order.is_quote_quantity:
                    self._deny_market_order_quantity(
                        order,
                        "Polymarket market SELL orders require base-denominated quantities; "
                        "resubmit with `quote_quantity=False`",
                    )
                    return
        elif order.is_quote_quantity:
            self._deny_market_order_quantity(
                order,
                "Polymarket Rust-signed market orders require base-denominated "
                "quantities; resubmit with `quote_quantity=False`",
            )
            return

        if order.time_in_force not in VALID_POLYMARKET_MARKET_TIME_IN_FORCE:
            self._log.error(
                f"Cannot submit order {order.client_order_id}: "
                f"Market order time in force {order.tif_string()} not supported on Polymarket; "
                "use either IOC or FOK",
                LogColor.RED,
            )
            self.generate_order_denied(
                strategy_id=order.strategy_id,
                instrument_id=order.instrument_id,
                client_order_id=order.client_order_id,
                reason="UNSUPPORTED_MARKET_TIME_IN_FORCE",
                ts_event=self._clock.timestamp_ns(),
            )
            return

        market_order_type = convert_tif_to_polymarket_order_type(order.time_in_force)

        signing_start = self._clock.timestamp()
        self._log_order_init_to_sign_start(order, "market", signing_start)
        if self._rust_client is not None:
            signed_order = await self._sign_market_order_rust_as_limit(
                order,
                instrument,
            )
            base_quantity = None
            signing_backend = "rust"
        else:
            amount = float(order.quantity)
            user_usdc_balance = (
                self._cached_collateral_balance_pusd()
                if order.side == OrderSide.BUY and order.is_quote_quantity
                else 0.0
            )
            market_order_args = MarketOrderArgsV2(
                token_id=get_polymarket_token_id(order.instrument_id),
                amount=amount,
                side=order_side_to_str(order.side),
                order_type=market_order_type,
                user_usdc_balance=user_usdc_balance,
                builder_code=POLYMARKET_NAUTILUS_BUILDER_CODE,
            )
            signed_order = await asyncio.to_thread(
                self._http_client.create_market_order,
                market_order_args,
                options=self._create_order_options(instrument),
            )
            base_quantity = None
            if order.is_quote_quantity and order.side == OrderSide.BUY:
                # SignedOrderV2 is a flat dataclass; takerAmount is the share base unit count.
                taker_amount = int(signed_order.takerAmount)
                base_qty_value = taker_amount / 1e6
                base_quantity = Quantity(base_qty_value, instrument.size_precision)
            signing_backend = "python"
        interval = self._clock.timestamp() - signing_start
        self._log.info(
            f"POLYMARKET_ORDER_TIMING sign_order client_order_id={order.client_order_id} "
            f"order_type=market signer={signing_backend} elapsed={interval:.3f}s",
            LogColor.BLUE,
        )

        self.generate_order_submitted(
            strategy_id=order.strategy_id,
            instrument_id=order.instrument_id,
            client_order_id=order.client_order_id,
            ts_event=self._clock.timestamp_ns(),
        )

        neg_risk = self._get_neg_risk_for_instrument(instrument)
        expected_venue_order_id = self._expected_venue_order_id(signed_order, neg_risk=neg_risk)
        await self._post_signed_order(
            order,
            signed_order,
            order_type_override=(
                PolyOrderType.FAK
                if self._rust_client is not None and _parse_arb_completion_price_cap(order.tags) is not None
                else PolyOrderType.GTD
                if self._rust_client is not None
                else market_order_type
            ),
            base_quantity=base_quantity,
            expected_venue_order_id=expected_venue_order_id,
            timing_start=signing_start,
        )

    async def _submit_limit_order(self, command: SubmitOrder, instrument) -> None:
        self._log.debug("Creating Polymarket order", LogColor.MAGENTA)

        order = command.order

        if order.is_quote_quantity:
            self._log.error(
                f"Cannot submit order {order.client_order_id}: UNSUPPORTED_QUOTE_QUANTITY",
                LogColor.RED,
            )
            self.generate_order_denied(
                strategy_id=order.strategy_id,
                instrument_id=order.instrument_id,
                client_order_id=order.client_order_id,
                reason="UNSUPPORTED_QUOTE_QUANTITY",
                ts_event=self._clock.timestamp_ns(),
            )
            return

        neg_risk = self._get_neg_risk_for_instrument(instrument)
        signing_start = self._clock.timestamp()
        self._log_order_init_to_sign_start(order, "limit", signing_start)
        if self._rust_client is not None:
            signed_order = await self._sign_limit_order_rust(order, instrument)
            signing_backend = "rust"
        else:
            order_args = OrderArgsV2(
                price=float(order.price),
                token_id=get_polymarket_token_id(order.instrument_id),
                size=float(order.quantity),
                side=order_side_to_str(order.side),
                expiration=self._get_expiration_secs(order),
                builder_code=POLYMARKET_NAUTILUS_BUILDER_CODE,
            )
            signed_order = await asyncio.to_thread(
                self._http_client.create_order,
                order_args,
                options=self._create_order_options(instrument),
            )
            signing_backend = "python"
        interval = self._clock.timestamp() - signing_start
        self._log.info(
            f"POLYMARKET_ORDER_TIMING sign_order client_order_id={order.client_order_id} "
            f"order_type=limit signer={signing_backend} elapsed={interval:.3f}s",
            LogColor.BLUE,
        )

        self.generate_order_submitted(
            strategy_id=order.strategy_id,
            instrument_id=order.instrument_id,
            client_order_id=order.client_order_id,
            ts_event=self._clock.timestamp_ns(),
        )

        expected_venue_order_id = self._expected_venue_order_id(signed_order, neg_risk=neg_risk)
        await self._post_signed_order(
            order,
            signed_order,
            post_only=order.is_post_only,
            expected_venue_order_id=expected_venue_order_id,
            timing_start=signing_start,
        )

    def _log_order_init_to_sign_start(
        self,
        order: Order,
        order_type: str,
        signing_start: float,
    ) -> None:
        ts_init = getattr(order, "ts_init", None)
        if not ts_init:
            return

        elapsed = signing_start - (ts_init / 1_000_000_000)
        if elapsed < 0:
            return

        self._log.info(
            "POLYMARKET_ORDER_TIMING init_to_sign_start "
            f"client_order_id={order.client_order_id} order_type={order_type} elapsed={elapsed:.3f}s",
            LogColor.BLUE,
        )

    async def _post_signed_order(
        self,
        order: Order,
        signed_order,
        post_only: bool = False,
        order_type_override=None,
        base_quantity: Quantity | None = None,
        expected_venue_order_id: VenueOrderId | None = None,
        timing_start: float | None = None,
    ) -> None:
        retry_manager = await self._retry_manager_pool.acquire()
        try:
            poly_order_type = order_type_override or convert_tif_to_polymarket_order_type(
                order.time_in_force,
            )
            if timing_start is not None:
                elapsed = self._clock.timestamp() - timing_start
                self._log.info(
                    "POLYMARKET_ORDER_TIMING post_orders_start "
                    f"client_order_id={order.client_order_id} elapsed={elapsed:.3f}s",
                    LogColor.BLUE,
                )
            response: JSON | None = await retry_manager.run(
                "submit_order",
                [order.client_order_id],
                asyncio.to_thread,
                self._http_client.post_order,
                signed_order,
                poly_order_type,
                post_only,
            )

            if response is None:
                if self._is_unknown_submit_result(retry_manager.last_exception):
                    self._handle_unknown_submit_result(
                        order,
                        expected_venue_order_id,
                        str(retry_manager.message),
                        base_quantity=base_quantity,
                    )
                    return

                self.generate_order_rejected(
                    strategy_id=order.strategy_id,
                    instrument_id=order.instrument_id,
                    client_order_id=order.client_order_id,
                    reason=str(retry_manager.message),
                    ts_event=self._clock.timestamp_ns(),
                )
            elif not response.get("success"):
                reason = response.get("errorMsg") or response.get("error") or retry_manager.message
                self.generate_order_rejected(
                    strategy_id=order.strategy_id,
                    instrument_id=order.instrument_id,
                    client_order_id=order.client_order_id,
                    reason=str(reason),
                    ts_event=self._clock.timestamp_ns(),
                )
            else:
                venue_order_id = VenueOrderId(response["orderID"])
                self._cache.add_venue_order_id(order.client_order_id, venue_order_id)

                if timing_start is not None:
                    elapsed = self._clock.timestamp() - timing_start
                    self._order_timing_starts[venue_order_id] = timing_start
                    self._log.info(
                        "POLYMARKET_ORDER_TIMING sign_and_post_order "
                        f"client_order_id={order.client_order_id} "
                        f"venue_order_id={venue_order_id} elapsed={elapsed:.3f}s",
                        LogColor.BLUE,
                    )
                    ack_seen_at = self._order_ack_seen_at.pop(venue_order_id, None)
                    if ack_seen_at is not None:
                        self._log_order_ack_timing(
                            venue_order_id=venue_order_id,
                            client_order_id=order.client_order_id,
                            ack_seen_at=ack_seen_at,
                            store_if_missing=False,
                        )
                    if venue_order_id in self._order_timing_starts:
                        self.create_task(
                            self._expire_order_ack_timing(
                                venue_order_id=venue_order_id,
                                client_order_id=order.client_order_id,
                            ),
                        )

                # Emit quote-to-base conversion after successful submission
                if base_quantity is not None:
                    self._send_quote_to_base_update(order, venue_order_id, base_quantity)

                # Register with fill tracker for dust detection
                self._register_fill_tracker(
                    order,
                    venue_order_id,
                    submitted_qty=base_quantity or order.quantity,
                )

                # Signal order event
                event = self._ack_events_order.get(venue_order_id)
                if event:
                    event.set()

                # Signal trade event
                trade_event = self._ack_events_trade.get(venue_order_id)
                if trade_event:
                    trade_event.set()

                # Check if cancel was requested during the HTTP round-trip
                self._execute_deferred_cancel_if_pending(order, venue_order_id)
        finally:
            await self._retry_manager_pool.release(retry_manager)

    def _send_quote_to_base_update(
        self,
        order: Order,
        venue_order_id: VenueOrderId,
        base_quantity: Quantity,
    ) -> None:
        self._log.info(
            f"Converted {order.instrument_id} quote quantity {order.quantity} "
            f"to base quantity {base_quantity}",
        )
        ts_now = self._clock.timestamp_ns()
        updated = OrderUpdated(
            trader_id=self.trader_id,
            strategy_id=order.strategy_id,
            instrument_id=order.instrument_id,
            client_order_id=order.client_order_id,
            venue_order_id=venue_order_id,
            account_id=self.account_id,
            quantity=base_quantity,
            price=None,
            trigger_price=None,
            event_id=UUID4(),
            ts_event=ts_now,
            ts_init=ts_now,
            is_quote_quantity=False,
        )
        self._send_order_event(updated)

    def _register_fill_tracker(
        self,
        order: Order,
        venue_order_id: VenueOrderId,
        submitted_qty: Quantity,
    ) -> None:
        instrument = self._cache.instrument(order.instrument_id)
        if instrument is None:
            return

        self._fill_tracker.register(
            venue_order_id=venue_order_id,
            submitted_qty=submitted_qty,
            order_side=order.side,
            instrument_id=order.instrument_id,
            size_precision=instrument.size_precision,
            price_precision=instrument.price_precision,
        )

    def _execute_deferred_cancel_if_pending(
        self,
        order: Order,
        venue_order_id: VenueOrderId,
    ) -> None:
        if not order.is_pending_cancel:
            return

        self._log.info(
            f"Order {order.client_order_id!r} is pending cancel, "
            f"issuing deferred cancel for {venue_order_id!r}",
        )
        self.create_task(self._execute_deferred_cancel(order, venue_order_id))

    def _expected_venue_order_id(self, signed_order, neg_risk: bool) -> VenueOrderId | None:
        if not hasattr(signed_order, "salt"):
            return None

        signer = getattr(self._http_client, "signer", None)
        if signer is None:
            return None

        try:
            chain_id = signer.get_chain_id()
            contract_config = get_contract_config(chain_id)

            if hasattr(signed_order, "timestamp"):
                exchange_address = (
                    contract_config.neg_risk_exchange_v2
                    if neg_risk
                    else contract_config.exchange_v2
                )
                builder = ExchangeOrderBuilderV2(exchange_address, chain_id, signer)
            else:
                exchange_address = (
                    contract_config.neg_risk_exchange if neg_risk else contract_config.exchange
                )
                builder = ExchangeOrderBuilderV1(exchange_address, chain_id, signer)

            typed_data = builder.build_order_typed_data(signed_order)
            return VenueOrderId(builder.build_order_hash(typed_data))
        except Exception as e:
            self._log.debug(f"Could not derive Polymarket order ID: {e}")
            return None

    def _is_unknown_submit_result(self, exc: BaseException | None) -> bool:
        return isinstance(exc, PolyApiException) and getattr(exc, "status_code", None) is None

    def _handle_unknown_submit_result(
        self,
        order: Order,
        expected_venue_order_id: VenueOrderId | None,
        reason: str,
        base_quantity: Quantity | None = None,
    ) -> None:
        if expected_venue_order_id is not None:
            self._cache_submit_venue_order_id(order, expected_venue_order_id)
            if base_quantity is not None:
                self._send_quote_to_base_update(order, expected_venue_order_id, base_quantity)
            self._register_fill_tracker(
                order,
                expected_venue_order_id,
                submitted_qty=base_quantity or order.quantity,
            )
            self._signal_ack_events(expected_venue_order_id)
            self._execute_deferred_cancel_if_pending(order, expected_venue_order_id)
            self._log.warning(
                f"Submit result unknown for {order.client_order_id!r}: {reason}. "
                f"Cached expected {expected_venue_order_id!r}; awaiting WS or reconciliation",
            )
            return

        self._log.warning(
            f"Submit result unknown for {order.client_order_id!r}: {reason}. "
            "Leaving order submitted; awaiting WS or reconciliation",
        )

    def _cache_submit_venue_order_id(self, order: Order, venue_order_id: VenueOrderId) -> None:
        try:
            self._cache.add_venue_order_id(order.client_order_id, venue_order_id)
        except ValueError as e:
            self._log.warning(
                f"Could not cache expected {venue_order_id!r} for {order.client_order_id!r}: {e}",
            )

    def _signal_ack_events(self, venue_order_id: VenueOrderId) -> None:
        event = self._ack_events_order.get(venue_order_id)
        if event:
            event.set()

        trade_event = self._ack_events_trade.get(venue_order_id)
        if trade_event:
            trade_event.set()

    def _log_order_ack_timing(
        self,
        venue_order_id: VenueOrderId,
        client_order_id: ClientOrderId | None,
        ack_seen_at: float,
        store_if_missing: bool,
    ) -> None:
        timing_start = self._order_timing_starts.pop(venue_order_id, None)
        if timing_start is None:
            if store_if_missing:
                self._order_ack_seen_at[venue_order_id] = ack_seen_at
            return

        elapsed = ack_seen_at - timing_start
        self._log.info(
            "POLYMARKET_ORDER_TIMING order_ack "
            f"client_order_id={client_order_id} "
            f"venue_order_id={venue_order_id} elapsed={elapsed:.3f}s",
            LogColor.BLUE,
        )

    async def _expire_order_ack_timing(
        self,
        venue_order_id: VenueOrderId,
        client_order_id: ClientOrderId,
    ) -> None:
        await asyncio.sleep(self._config.ack_timeout_secs)

        timing_start = self._order_timing_starts.pop(venue_order_id, None)
        self._order_ack_seen_at.pop(venue_order_id, None)
        if timing_start is None:
            return

        elapsed = self._clock.timestamp() - timing_start
        self._log.warning(
            "POLYMARKET_ORDER_TIMING order_ack_missing "
            f"client_order_id={client_order_id} "
            f"venue_order_id={venue_order_id} elapsed={elapsed:.3f}s",
        )

    def _handle_ws_message(self, raw: bytes) -> None:
        try:
            if self._config.log_raw_ws_messages:
                self._log.info(
                    str(json.dumps(msgspec.json.decode(raw), indent=4)),
                    color=LogColor.MAGENTA,
                )

            msg = self._decoder_user_msg.decode(raw)
            if isinstance(msg, PolymarketUserOrder):
                self._handle_ws_order_msg(msg, wait_for_ack=True)
            elif isinstance(msg, PolymarketUserTrade):
                self._add_trade_to_cache(msg, raw)
                self._handle_ws_trade_msg(msg, wait_for_ack=True)
            else:
                self._log.error(f"Unrecognized websocket message {msg}")
        except Exception as e:
            self._log.exception(
                f"Error handling websocket message: {e.__class__.__name__} - "
                f"raw message: {raw.decode(errors='replace')}",
                e,
            )

    def _add_trade_to_cache(self, msg: PolymarketUserTrade, raw: bytes) -> None:
        start_us = self._clock.timestamp_us()
        cache_key = get_polymarket_trades_key(msg.taker_order_id, msg.id)
        self._cache.add(cache_key, raw)
        interval_us = self._clock.timestamp_us() - start_us
        self._log.info(
            f"Added trade {msg.id} {msg.status.value} to {cache_key} in {interval_us}μs",
            LogColor.BLUE,
        )

    async def _wait_for_ack_order(
        self,
        msg: PolymarketUserOrder,
        venue_order_id: VenueOrderId,
    ) -> None:
        ack_seen_at = self._clock.timestamp()
        client_order_id = self._cache.client_order_id(venue_order_id)
        event = self._ack_events_order.get(venue_order_id)
        if client_order_id is None and event is None:
            event = asyncio.Event()
            self._ack_events_order[venue_order_id] = event

        if msg.type == PolymarketEventType.PLACEMENT:
            self._log_order_ack_timing(
                venue_order_id=venue_order_id,
                client_order_id=client_order_id,
                ack_seen_at=ack_seen_at,
                store_if_missing=client_order_id is None,
            )
        if client_order_id is not None:
            self._handle_ws_order_msg(msg, wait_for_ack=False)
            return

        try:
            await asyncio.wait_for(event.wait(), timeout=self._config.ack_timeout_secs)
        except TimeoutError:
            self._log.warning(f"Timed out awaiting placement ack for {venue_order_id!r}")
        finally:
            self._ack_events_order.pop(venue_order_id, None)

        self._handle_ws_order_msg(msg, wait_for_ack=False)

    async def _wait_for_ack_trade(
        self,
        msg: PolymarketUserTrade,
        venue_order_id: VenueOrderId,
    ) -> None:
        self._log.debug(f"Waiting for trade ack for {venue_order_id!r}...")

        client_order_id = self._cache.client_order_id(venue_order_id)
        if client_order_id is None:
            # Reuse existing event if present to avoid overwriting a pending waiter
            event = self._ack_events_trade.get(venue_order_id)
            if event is None:
                event = asyncio.Event()
                self._ack_events_trade[venue_order_id] = event

            try:
                await asyncio.wait_for(event.wait(), timeout=self._config.ack_timeout_secs)
            except TimeoutError:
                self._log.warning(f"Timed out awaiting placement ack for {venue_order_id!r}")
            finally:
                self._ack_events_trade.pop(venue_order_id, None)

        # Process only this specific order, not the entire trade message
        trade_id = TradeId(msg.id)
        order_id = venue_order_id.value
        self._handle_user_trade_in_ws_trade_msg(
            msg,
            trade_id,
            wait_for_ack=False,
            order_id=order_id,
        )

    def _handle_ws_order_msg(self, msg: PolymarketUserOrder, wait_for_ack: bool):  # noqa: C901
        self._log.debug(f"Handling order message, {wait_for_ack=}")

        venue_order_id = msg.venue_order_id()
        instrument_id = get_polymarket_instrument_id(msg.market, msg.asset_id)
        instrument = self._cache.instrument(instrument_id)
        if instrument is None:
            self._log.warning(
                f"Received order message for unknown instrument {instrument_id} "
                f"(market={msg.market}, asset_id={msg.asset_id}). "
                f"This may indicate the instrument is not subscribed or cached, skipping order processing",
            )
            return

        if wait_for_ack:
            self.create_task(self._wait_for_ack_order(msg, venue_order_id))
            return

        client_order_id = self._cache.client_order_id(venue_order_id)
        self._log.debug(f"Processing order update for {client_order_id!r}")

        strategy_id = None

        if client_order_id:
            strategy_id = self._cache.strategy_id_for_order(client_order_id)

        if strategy_id is None:
            report = msg.parse_to_order_status_report(
                account_id=self.account_id,
                instrument=instrument,
                client_order_id=client_order_id,
                ts_init=self._clock.timestamp_ns(),
            )
            self._send_order_status_report(report)
            return

        self._log.debug(f"Order {msg.type.value}: {client_order_id!r}", LogColor.MAGENTA)

        order = self._cache.order(client_order_id) if client_order_id else None

        match msg.type:
            case PolymarketEventType.PLACEMENT:
                if order is None or order.status == OrderStatus.SUBMITTED:
                    self.generate_order_accepted(
                        strategy_id=strategy_id,
                        instrument_id=instrument_id,
                        client_order_id=client_order_id,
                        venue_order_id=venue_order_id,
                        ts_event=self._clock.timestamp_ns(),
                    )
                else:
                    self._log.debug(
                        f"Order {client_order_id!r} in state {order.status_string()} - "
                        "skipping placement event",
                    )
            case PolymarketEventType.CANCELLATION:
                if order is not None and order.status == OrderStatus.CANCELED:
                    self._log.debug(
                        f"Order {client_order_id!r} already canceled - "
                        "skipping duplicate cancellation event",
                    )
                    return
                self.generate_order_canceled(
                    strategy_id=strategy_id,
                    instrument_id=instrument_id,
                    client_order_id=client_order_id,
                    venue_order_id=venue_order_id,
                    ts_event=millis_to_nanos(int(msg.timestamp)),
                )
            case PolymarketEventType.UPDATE:
                if msg.status == PolymarketOrderStatus.MATCHED:
                    dust = self._fill_tracker.check_dust_residual(venue_order_id)
                    if dust is not None:
                        dust_qty, dust_px = dust
                        dust_trade_id = TradeId(f"{msg.id[:27]}-dust")
                        self._log.info(
                            f"Order {venue_order_id!r} MATCHED with dust residual "
                            f"{dust_qty} — emitting synthetic fill",
                        )

                        if order is not None:
                            self.generate_order_filled(
                                strategy_id=strategy_id,
                                instrument_id=instrument_id,
                                client_order_id=client_order_id,
                                venue_order_id=venue_order_id,
                                venue_position_id=None,
                                trade_id=dust_trade_id,
                                order_side=order.side,
                                order_type=order.order_type,
                                last_qty=dust_qty,
                                last_px=dust_px,
                                quote_currency=pUSD,
                                commission=Money(0.0, pUSD),
                                liquidity_side=LiquiditySide.NO_LIQUIDITY_SIDE,
                                ts_event=millis_to_nanos(int(msg.timestamp)),
                            )
                else:
                    self._log.debug(f"Skipping order update: {msg}")
            case PolymarketEventType.TRADE:
                self._log.debug(f"Skipping order trade event: {msg}")
            case _:  # Branch never hit unless code changes (leave in place)
                raise RuntimeError(f"Unknown `PolymarketEventType`, was '{msg.type.value}'")

    def _truncate_ordered_dict(self, store: OrderedDict[Any, Any]) -> None:
        while len(store) > self.PROCESSED_TRADES_LIMIT:
            store.popitem(last=False)

    def _record_processed_trade(
        self,
        trade_id: TradeId,
        status: PolymarketTradeStatus,
    ) -> None:
        if status in POLYMARKET_FINALIZED_TRADE_STATUSES:
            # Keep final trades in their own cache so duplicates are still suppressed
            # after we stop tracking intermediate status transitions.
            self._finalized_trades[trade_id] = None
            self._finalized_trades.move_to_end(trade_id)
            self._processed_trades.pop(trade_id, None)
            self._truncate_ordered_dict(self._finalized_trades)
            return

        self._processed_trades[trade_id] = status
        self._processed_trades.move_to_end(trade_id)
        self._truncate_ordered_dict(self._processed_trades)

    def _record_processed_fill(
        self,
        trade_id: TradeId,
        venue_order_id: VenueOrderId,
    ) -> None:
        fill_key = (trade_id, venue_order_id)
        self._processed_fills[fill_key] = None
        self._processed_fills.move_to_end(fill_key)
        self._truncate_ordered_dict(self._processed_fills)

    def _handle_ws_trade_msg(self, msg: PolymarketUserTrade, wait_for_ack: bool):
        self._log.debug(f"Handling trade message, {wait_for_ack=}")

        trade_id = TradeId(msg.id)
        trade_str = f"Trade {trade_id}"
        log_msg = f"{trade_str} {msg.status.value}: {msg}"

        match msg.status:
            case PolymarketTradeStatus.RETRYING:
                self._log.warning(log_msg)
                return
            case PolymarketTradeStatus.FAILED:
                self._log.error(log_msg)
                return
            case _:
                self._log.info(log_msg, LogColor.BLUE)

        if trade_id in self._finalized_trades:
            self._log.debug(f"Trade {trade_id} already finalized - skipping duplicate")
            return

        # Handle status transitions (e.g., MATCHED -> MINED -> CONFIRMED)
        previous_status = self._processed_trades.get(trade_id)

        if (
            previous_status is not None
            and msg.status in POLYMARKET_FINALIZED_TRADE_STATUSES
            and previous_status not in POLYMARKET_FINALIZED_TRADE_STATUSES
        ):
            self._record_processed_trade(trade_id, msg.status)
            self._log.debug(
                f"Trade {trade_id} transitioned from {previous_status.value} "
                f"to {msg.status.value} - refreshing account state",
            )
            self.create_task(self._update_account_state())
            return

        # For same status or new trades, process fills (per-fill dedup handles duplicates)

        filled_user_order_ids = msg.get_filled_user_order_ids(self._wallet_address, self._api_key)
        for order_id in filled_user_order_ids:
            self._handle_user_trade_in_ws_trade_msg(msg, trade_id, wait_for_ack, order_id)

    def _handle_user_trade_in_ws_trade_msg(
        self,
        msg: PolymarketUserTrade,
        trade_id: TradeId,
        wait_for_ack: bool,
        order_id: str,
    ):
        venue_order_id = msg.venue_order_id(order_id)
        composite_trade_id = make_composite_trade_id(msg.id, venue_order_id)

        asset_id = msg.get_asset_id(order_id)
        instrument_id = get_polymarket_instrument_id(msg.market, asset_id)
        instrument = self._cache.instrument(instrument_id)

        if instrument is None:
            self._log.warning(
                f"Received trade message for unknown instrument {instrument_id} "
                f"(market={msg.market}, asset_id={asset_id}). "
                f"This may indicate the instrument is not subscribed or cached, skipping trade processing",
            )
            return

        if wait_for_ack:
            self.create_task(self._wait_for_ack_trade(msg, venue_order_id))
            return

        # Check if this specific fill was already processed (handles multi-order trades)
        fill_key = (trade_id, venue_order_id)
        if fill_key in self._processed_fills:
            self._log.debug(
                f"Fill {trade_id} for {venue_order_id!r} already processed - skipping",
            )
            return

        client_order_id = self._cache.client_order_id(venue_order_id)
        strategy_id = None

        if client_order_id:
            strategy_id = self._cache.strategy_id_for_order(client_order_id)

        if strategy_id is None:
            self._log.warning("Strategy ID not found - parsing fill report")
            report = msg.parse_to_fill_report(
                account_id=self.account_id,
                instrument=instrument,
                client_order_id=client_order_id,
                ts_init=self._clock.timestamp_ns(),
                filled_user_order_id=order_id,
            )
            self._send_fill_report(report)
            self._record_processed_fill(trade_id, venue_order_id)
            self._record_processed_trade(trade_id, msg.status)
            return

        order = self._cache.order(client_order_id)

        if order is None:
            self._log.error(f"Cannot process trade: {client_order_id!r} not found in cache")
            return

        if composite_trade_id in order.trade_ids:
            self._log.debug(
                f"Trade {composite_trade_id} already processed for {order.client_order_id} - skipping",
            )
            return

        if order.is_closed:
            self._log.warning(f"Order already closed - skipping trade processing: {order}")
            return  # Already closed (only status update)

        raw_last_qty = instrument.make_qty(msg.last_qty(order_id))
        last_px = instrument.make_price(msg.last_px(order_id))
        liquidity_side = msg.liquidity_side()
        # Compute commission from the venue-reported quantity, then snap for
        # engine acceptance. The fee Polymarket actually charged tracks the
        # on-chain fill, not our local-only dust adjustment.
        commission = calculate_commission(
            quantity=raw_last_qty.as_decimal(),
            price=last_px.as_decimal(),
            fee_rate=instrument.taker_fee,
            liquidity_side=liquidity_side,
        )
        last_qty = self._fill_tracker.snap_fill_qty(venue_order_id, raw_last_qty)
        ts_event = secs_to_nanos(int(msg.match_time))

        self.generate_order_filled(
            strategy_id=strategy_id,
            instrument_id=instrument_id,
            client_order_id=client_order_id,
            venue_order_id=venue_order_id,
            venue_position_id=None,  # Not applicable on Polymarket
            trade_id=composite_trade_id,
            order_side=order.side,
            order_type=order.order_type,
            last_qty=last_qty,
            last_px=last_px,
            quote_currency=pUSD,
            commission=Money(commission, pUSD),
            liquidity_side=liquidity_side,
            ts_event=ts_event,
            info=msg.to_dict(),
        )

        self._record_processed_fill(trade_id, venue_order_id)
        self._fill_tracker.record_fill(
            venue_order_id=venue_order_id,
            qty=float(last_qty),
            px=float(last_px),
            ts=ts_event,
        )
        self._record_processed_trade(trade_id, msg.status)

        # Only update account balance after trade is mined on-chain
        if msg.status in POLYMARKET_FINALIZED_TRADE_STATUSES:
            self.create_task(self._update_account_state())
