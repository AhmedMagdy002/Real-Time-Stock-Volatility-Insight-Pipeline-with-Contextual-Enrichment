# prod.py
from kafka import KafkaProducer
import json
import time
from datetime import datetime
import random

# Multiple exchanges and their stocks
exchange_stocks = {
    'XNAS': ['AAPL', 'MSFT', 'TSLA', 'GOOGL'],  # NASDAQ (New York)
    'XNYS': ['JPM', 'GE', 'XOM', 'BAC'],        # NYSE (New York)  
    'XLON': ['BP', 'VODL', 'HSBA', 'AZN']      # London Stock Exchange
}

producer = KafkaProducer(
    bootstrap_servers='kafka:29092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("🚀 Starting multi-exchange trade producer...")

for i in range(40):  # More trades across exchanges
    # Random exchange selection
    exchange = random.choice(list(exchange_stocks.keys()))
    symbol = random.choice(exchange_stocks[exchange])
    
    trade = {
        "symbol": symbol,
        "price": round(random.uniform(100, 300), 2),
        "volume": random.randint(10, 1000),
        "timestamp": datetime.utcnow().isoformat(),
        "exchange": exchange  # ✅ Now varies across exchanges!
    }
    producer.send('test-topic', value=trade)
    print(f"✅ Sent: {trade}")
    time.sleep(0.5)

producer.close()
print("🚀 Finished sending multi-exchange trades.")