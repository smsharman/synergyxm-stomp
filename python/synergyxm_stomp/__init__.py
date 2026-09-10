"""STOMP 1.2 over WebSocket for SynergyXM workers (RabbitMQ Web STOMP)."""

from .broker import Broker, ConnectionLost, Message, StompError, ws_url
from .frames import Decoder, Frame, encode

__all__ = ["Broker", "ConnectionLost", "Message", "StompError", "ws_url", "Decoder", "Frame", "encode"]
__version__ = "0.1.0"
