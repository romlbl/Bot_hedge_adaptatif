import sqlite3

conn = sqlite3.connect('algo.db')
cursor = conn.cursor()

cursor.execute('''
    CREATE TABLE IF NOT EXISTS positions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        id_exchange TEXT DEFAULT NULL,
        symbol TEXT NOT NULL,
        side TEXT NOT NULL,
        status TEXT NOT NULL,
        stop_loss REAL DEFAULT NULL,
        take_profit REAL DEFAULT NULL,
        seuil REAL DEFAULT NULL,
        target REAL DEFAULT NULL,
        id_hedge INTEGER DEFAULT NULL,
        seuil_apport_low REAL DEFAULT NULL,
        seuil_apport_lower REAL DEFAULT NULL,
        seuil_apport_high REAL DEFAULT NULL,
        seuil_apport_higher REAL DEFAULT NULL,
        atr REAL DEFAULT NULL,
        ask_A REAL DEFAULT NULL,
        bid_A REAL DEFAULT NULL,
        openTime_A INTEGER DEFAULT NULL,
        size_A REAL DEFAULT NULL,
        ask_B REAL DEFAULT NULL,
        bid_B REAL DEFAULT NULL,
        openTime_B INTEGER DEFAULT NULL,
        size_B REAL DEFAULT NULL,
        ask_C REAL DEFAULT NULL,
        bid_C REAL DEFAULT NULL,
        openTime_C INTEGER DEFAULT NULL,
        size_C REAL DEFAULT NULL,
        score REAL NOT NULL,
        score_var REAL DEFAULT NULL,
        close REAL DEFAULT NULL,
        closeTime INTEGER DEFAULT NULL,
        drawdown REAL DEFAULT NULL,
        nb_hedge INTEGER DEFAULT 0,
        nb_apport INTEGER DEFAULT 0,
        remaining_fraction REAL DEFAULT 1.0,
        realized_partial_gain REAL DEFAULT 0.0,
        nb_partial_exit INTEGER DEFAULT 0,
        gain REAL DEFAULT NULL,
        pnl DEFAULT NULL,
        top_pnl DEFAULT NULL,
        total_fees REAL DEFAULT NULL,
        total_fees_paid REAL DEFAULT 0.0
    )
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS hedges (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    id_exchange TEXT DEFAULT NULL,
    id_parent INTEGER NOT NULL,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    status TEXT NOT NULL,
    ask REAL DEFAULT NULL,
    bid REAL DEFAULT NULL,
    openTime INTEGER DEFAULT NULL,
    size REAL DEFAULT NULL,
    original_size REAL DEFAULT NULL,
    nb_partial_exit INTEGER DEFAULT 0,
    realized_partial_gain REAL DEFAULT 0.0,
    close REAL DEFAULT NULL,
    closeTime INTEGER DEFAULT NULL,
    gain REAL DEFAULT NULL,
    pnl DEFAULT NULL,
    total_fees REAL DEFAULT NULL
    )
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS pending_orders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        id_position INTEGER NOT NULL,
        id_hedge INTEGER DEFAULT NULL,
        kind TEXT NOT NULL,              -- 'open_A' | 'open_B' | 'open_C' | 'close' | 'hedge_open' | 'hedge_close'
        side TEXT NOT NULL,              -- 'buy' | 'sell' -- sens RÉEL de l'ordre (pas LONG/SHORT)
        target_size REAL NOT NULL,       -- quantité totale visée
        filled_size REAL NOT NULL DEFAULT 0.0,
        vwap_price REAL DEFAULT NULL,    -- prix moyen pondéré déjà obtenu sur filled_size
        ref_price REAL NOT NULL,         -- prix de référence initial (base de la tolérance de slippage)
        status TEXT NOT NULL DEFAULT 'filling',  -- 'filling' | 'done' | 'aborted'
        started_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL
    )
''')

cursor.execute('''
    CREATE INDEX IF NOT EXISTS idx_pending_orders_status ON pending_orders(status)
''')

cursor.execute('''
    CREATE INDEX IF NOT EXISTS idx_pending_orders_position ON pending_orders(id_position)
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS waitlist (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    openTime INTEGER NOT NULL,
    delta REAL DEFAULT NULL,
    variation REAL DEFAULT NULL,
    slope REAL DEFAULT NULL,
    score REAL NOT NULL,
    adx REAL NOT NULL
    )
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS logs (
    log TEXT NOT NULL,
    Time INTEGER NOT NULL
    )
''')

cursor.execute('''
    CREATE INDEX IF NOT EXISTS idx_logs_time ON logs(Time)
''')
    
conn.commit()
conn.close()