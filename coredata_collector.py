"""
Real-time market data collector with multi-exchange support.
Architectural choice: Using ccxt for unified exchange API access with
rate limiting and error recovery baked in.
"""

import asyncio
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import ccxt
import pandas as pd
import numpy as np
from dataclasses import dataclass
from enum import Enum
import time
from firebase_admin import firestore

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ExchangeType(Enum):
    """Supported exchange types with their ccxt identifiers"""
    BINANCE = "binance"
    COINBASE = "coinbase"
    KRAKEN = "kraken"
    FTX = "ftx"
    BYBIT = "bybit"

@dataclass
class MarketData:
    """Structured market data container"""
    symbol: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    vwap: Optional[float] = None
    trades: Optional[int] = None
    funding_rate: Optional[float] = None
    
class MarketDataCollector:
    """Robust market data collection system with error handling"""
    
    def __init__(
        self,
        exchange_type: ExchangeType = ExchangeType.BINANCE,
        symbols: List[str] = None,
        timeframe: str = "1h",
        firestore_client: Optional[Any] = None
    ):
        """
        Initialize market data collector
        
        Args:
            exchange_type: Exchange to connect to
            symbols: Trading pairs to monitor (default: major crypto pairs)
            timeframe: OHLCV timeframe (1m, 5m, 15m, 1h, 4h, 1d)
            firestore_client: Firebase Firestore client for data persistence
        """
        self.exchange_type = exchange_type
        self.timeframe = timeframe
        self.firestore_client = firestore_client
        self.symbols = symbols or [
            "BTC/USDT", "ETH/USDT", "SOL/USDT",
            "ADA/USDT", "DOT/USDT", "AVAX/USDT"
        ]
        
        # Initialize exchange connection
        self._init_exchange()
        
        # Rate limiting and error tracking
        self.last_request_time = {}
        self.request_count = 0
        self.max_requests_per_minute = 120
        self.error_count = 0
        self.max_errors_before_reset = 10
        
    def _init_exchange(self) -> None:
        """Initialize exchange connection with error handling"""
        try:
            exchange_class = getattr(ccxt, self.exchange_type.value)
            self.exchange = exchange_class({
                'enableRateLimit': True,
                'options': {'defaultType': 'spot'},
                'timeout': 30000,
            })
            
            # Load markets
            self.exchange.load_markets()
            logger.info(f"Successfully connected to {self.exchange_type.value}")
            
            # Validate symbols
            self._validate_symbols()
            
        except AttributeError as e:
            logger.error(f"Invalid exchange type: {self.exchange_type.value}")
            raise ValueError(f"Unsupported exchange: {self.exchange_type.value}") from e
        except Exception as e:
            logger.error(f"Failed to initialize exchange: {str(e)}")
            raise ConnectionError(f"Exchange initialization failed: {str(e)}") from e
    
    def _validate_symbols(self) -> None:
        """Validate trading symbols against exchange"""
        valid_symbols = []
        for symbol in self.symbols:
            if symbol in self.exchange.markets:
                valid_symbols.append(symbol)
            else:
                logger.warning(f"Symbol {symbol} not available on {self.exchange_type.value}")
        
        if not valid_symbols:
            raise ValueError("No valid symbols found for the exchange")
        
        self.symbols = valid_symbols
        logger.info(f"Validated {len(self.symbols)} symbols")
    
    def _rate_limit_check(self, symbol: str) -> None:
        """Implement rate limiting to avoid API bans"""
        current_time = time.time()
        
        if symbol in self.last_request_time:
            time_since_last = current_time - self.last_request_time[symbol]
            if time_since_last < 60 / self.max_requests_per_minute:
                sleep_time = (60 / self.max_requests_per_minute) - time_since_last
                logger.debug(f"Rate limiting: sleeping {sleep_time:.2f}s for