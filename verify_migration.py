#!/usr/bin/env python
import os
import sys

project_root = os.path.dirname(os.path.abspath(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from app.config.database import engine
from sqlalchemy import text

intervals = ['15m', '1h', '4h', '1d', '1w', '1mo']
print('📊 Data Migration Verification:\n')
print(f"{'Interval':<10} {'Raw Rows':<12} {'Processed Rows':<15} {'Status':<10}")
print('-' * 50)

for interval in intervals:
    if interval == '1mo':
        raw_table = 'btcusdt_1mo_raw'
        proc_table = 'btcusdt_1mo_processed'
    else:
        raw_table = f'btcusdt_{interval}_raw'
        proc_table = f'btcusdt_{interval}_processed'
    
    raw_count_query = text(f'SELECT COUNT(*) FROM {raw_table}')
    proc_count_query = text(f'SELECT COUNT(*) FROM {proc_table}')
    
    with engine.begin() as conn:
        raw_count = conn.execute(raw_count_query).scalar() or 0
        proc_count = conn.execute(proc_count_query).scalar() or 0
    
    status = '✅ OK' if raw_count == proc_count and proc_count > 0 else '⚠️ CHECK'
    print(f"{interval:<10} {raw_count:<12} {proc_count:<15} {status:<10}")

print('\n✅ Data migration complete!')
