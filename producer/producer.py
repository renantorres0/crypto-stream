"""
CryptoStream Producer
─────────────────────
Conecta ao Coinbase Advanced Trade WebSocket e publica
eventos de preço no Kafka com exactly-once semantics.

Tópico Kafka: crypto-prices
Particionamento: por símbolo (BTC-USD → partição 0, etc.)
"""

import json
import os
import time
import threading
import signal
import sys
from datetime import datetime, timezone
from typing import Any

import websocket
import structlog
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

# ── Configuração de logging estruturado ──────────────────────────────────────
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.stdlib.add_log_level,
        structlog.dev.ConsoleRenderer(),
    ]
)
log = structlog.get_logger()

# ── Variáveis de ambiente ─────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC", "crypto-prices")
SYMBOLS_RAW             = os.getenv("SYMBOLS", "BTC-USD,ETH-USD,SOL-USD,DOGE-USD,BNB-USD")
SYMBOLS                 = [s.strip() for s in SYMBOLS_RAW.split(",")]

# Mapeamento símbolo → partição (consumer groups podem ler por moeda)
SYMBOL_PARTITION_MAP = {sym: i for i, sym in enumerate(SYMBOLS)}

# ── Criação do tópico com partições ──────────────────────────────────────────
def ensure_topic_exists():
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    topics_meta = admin.list_topics(timeout=10)
    if KAFKA_TOPIC not in topics_meta.topics:
        log.info("creating_kafka_topic", topic=KAFKA_TOPIC, partitions=len(SYMBOLS))
        new_topic = NewTopic(
            KAFKA_TOPIC,
            num_partitions=len(SYMBOLS),
            replication_factor=1,
            config={
                "retention.ms": str(24 * 60 * 60 * 1000),  # 24h
                "cleanup.policy": "delete",
            },
        )
        fs = admin.create_topics([new_topic])
        for topic, f in fs.items():
            try:
                f.result()
                log.info("topic_created", topic=topic)
            except Exception as e:
                log.warning("topic_may_exist", topic=topic, error=str(e))


# ── Kafka Producer com exactly-once semantics ─────────────────────────────────
def build_producer() -> Producer:
    return Producer({
        "bootstrap.servers":           KAFKA_BOOTSTRAP_SERVERS,
        # Exactly-once / idempotência
        "enable.idempotence":          True,
        "acks":                        "all",
        "retries":                     10,
        "max.in.flight.requests.per.connection": 5,
        # Performance
        "linger.ms":                   5,
        "batch.size":                  16384,
        "compression.type":            "snappy",
        # Observabilidade
        "statistics.interval.ms":      10000,
    })


def delivery_report(err, msg):
    """Callback chamado após cada mensagem ser confirmada pelo broker."""
    if err:
        log.error("delivery_failed", error=str(err), topic=msg.topic())
    else:
        log.debug(
            "delivered",
            topic=msg.topic(),
            partition=msg.partition(),
            offset=msg.offset(),
        )


# ── WebSocket handler ─────────────────────────────────────────────────────────
class CoinbaseWebSocketClient:
    WS_URL = "wss://advanced-trade-ws.coinbase.com"

    def __init__(self, producer: Producer):
        self.producer     = producer
        self.ws           = None
        self._running     = False
        self._reconnect_delay = 2
        # Estado para calcular variação por símbolo
        self._last_price: dict[str, float] = {}

    def _on_open(self, ws):
        log.info("ws_connected", url=self.WS_URL)
        self._reconnect_delay = 2
        # Subscribe no canal de ticker para todos os símbolos
        subscribe_msg = {
            "type":        "subscribe",
            "product_ids": SYMBOLS,
            "channel":     "ticker",
        }
        ws.send(json.dumps(subscribe_msg))
        log.info("subscribed", symbols=SYMBOLS)

    def _on_message(self, ws, raw: str):
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return

        msg_type = msg.get("channel")
        if msg_type != "ticker":
            return

        events = msg.get("events", [])
        for event in events:
            for tick in event.get("tickers", []):
                self._handle_ticker(tick)

    def _handle_ticker(self, tick: dict[str, Any]):
        symbol = tick.get("product_id")
        if not symbol or symbol not in SYMBOLS:
            return

        try:
            price = float(tick.get("price", 0))
        except (ValueError, TypeError):
            return

        if price <= 0:
            return

        # Calcula variação percentual em relação ao tick anterior
        prev_price = self._last_price.get(symbol)
        change_pct = None
        if prev_price:
            change_pct = round((price - prev_price) / prev_price * 100, 4)
        self._last_price[symbol] = price

        payload = {
            "symbol":     symbol,
            "price":      price,
            "volume_24h": float(tick.get("volume_24_h", 0) or 0),
            "bid":        float(tick.get("best_bid", 0) or 0),
            "ask":        float(tick.get("best_ask", 0) or 0),
            "change_pct": change_pct,
            "event_time": datetime.now(timezone.utc).isoformat(),
        }

        partition = SYMBOL_PARTITION_MAP.get(symbol, 0)

        self.producer.produce(
            topic=KAFKA_TOPIC,
            key=symbol.encode("utf-8"),
            value=json.dumps(payload).encode("utf-8"),
            partition=partition,
            on_delivery=delivery_report,
        )
        # Flush assíncrono — não bloqueia o WebSocket
        self.producer.poll(0)

        log.info(
            "tick_produced",
            symbol=symbol,
            price=price,
            change_pct=change_pct,
            partition=partition,
        )

    def _on_error(self, ws, error):
        log.error("ws_error", error=str(error))

    def _on_close(self, ws, close_status_code, close_msg):
        log.warning("ws_closed", status=close_status_code, message=close_msg)
        if self._running:
            log.info("reconnecting", delay=self._reconnect_delay)
            time.sleep(self._reconnect_delay)
            self._reconnect_delay = min(self._reconnect_delay * 2, 60)
            self._connect()

    def _connect(self):
        self.ws = websocket.WebSocketApp(
            self.WS_URL,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
        )
        t = threading.Thread(target=self.ws.run_forever, kwargs={"ping_interval": 30})
        t.daemon = True
        t.start()

    def start(self):
        self._running = True
        self._connect()

    def stop(self):
        self._running = False
        if self.ws:
            self.ws.close()
        # Flush final do producer
        remaining = self.producer.flush(timeout=10)
        if remaining > 0:
            log.warning("unflushed_messages", count=remaining)
        log.info("producer_stopped")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    log.info("starting_crypto_producer", symbols=SYMBOLS, kafka=KAFKA_BOOTSTRAP_SERVERS)

    # Aguarda Kafka estar pronto
    for attempt in range(30):
        try:
            ensure_topic_exists()
            break
        except Exception as e:
            log.warning("kafka_not_ready", attempt=attempt, error=str(e))
            time.sleep(3)
    else:
        log.error("kafka_unreachable")
        sys.exit(1)

    producer = build_producer()
    client   = CoinbaseWebSocketClient(producer)

    def shutdown(sig, frame):
        log.info("shutdown_signal_received")
        client.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT,  shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    client.start()

    # Mantém o processo vivo
    while True:
        time.sleep(1)


if __name__ == "__main__":
    main()
