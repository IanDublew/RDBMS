# examples.py
from rdbms_enhanced import EnhancedRDBMS, SQLREPL
import os
import time

def print_header(msg):
    print(f"\n{'='*70}\n 🏦 {msg}\n{'='*70}")

def run_fintech_demo():
    DB_FILE = "core_banking.db"
    if os.path.exists(DB_FILE): os.remove(DB_FILE)
    
    # Initialize the engine
    print_header("BOOTING CORE LEDGER SYSTEM v2.0")
    db = EnhancedRDBMS(DB_FILE)
    
    # ---------------------------------------------------------
    # 1. SCHEMA DEFINITION (KYC & WALLETS)
    # ---------------------------------------------------------
    print("[1] Provisioning Ledger Schema (Strict Foreign Keys)...")
    
    # 1. Customers (KYC Data)
    db.execute("""
        CREATE TABLE customers (
            c_id INTEGER PRIMARY KEY, 
            name TEXT, 
            risk_score TEXT
        )
    """)
    
    # 2. Wallets (The Balances)
    db.execute("""
        CREATE TABLE wallets (
            w_id INTEGER PRIMARY KEY, 
            c_id INTEGER, 
            currency TEXT,
            balance REAL,
            FOREIGN KEY (c_id) REFERENCES customers(c_id)
        )
    """)

    # 3. Transaction Log (Immutable Audit Trail)
    db.execute("""
        CREATE TABLE tx_log (
            tx_id INTEGER PRIMARY KEY,
            w_id INTEGER,
            amount REAL,
            type TEXT,
            FOREIGN KEY (w_id) REFERENCES wallets(w_id)
        )
    """)
    print("✓ Schema applied. Integrity constraints active.")

    # ---------------------------------------------------------
    # 2. SEEDING & INTEGRITY CHECKS
    # ---------------------------------------------------------
    print("\n[2] Onboarding Customers...")
    db.execute("INSERT INTO customers VALUES (101, 'Stark Industries', 'LOW')")
    db.execute("INSERT INTO customers VALUES (102, 'Wayne Ent', 'LOW')")
    
    # Create Wallets
    db.execute("INSERT INTO wallets VALUES (1, 101, 'USD', 1000000.00)") # Stark Wallet
    db.execute("INSERT INTO wallets VALUES (2, 102, 'USD', 500000.00)")  # Wayne Wallet
    
    print(f"✓ Wallets created.")
    
    print("\n[2.1] SECURITY TEST: Attempting to create wallet for ghost user...")
    try:
        res = db.execute("INSERT INTO wallets VALUES (99, 999, 'USD', 0.00)")
        if res.get('status') == 'error': raise Exception(res['message'])
    except Exception as e:
        print(f"🛡️  BLOCKED: {e}")
        print("   (System successfully prevented orphan record creation)")

    # ---------------------------------------------------------
    # 3. THE "ATOMIC TRANSFER" (ACID COMPLIANCE)
    # ---------------------------------------------------------
    print_header("SCENARIO: HIGH-VALUE WIRE TRANSFER")
    print("Initiating Transfer: $50,000 from Stark (ID:1) -> Wayne (ID:2)")
    
    print("\n... Step 1: Opening Transaction Block (BEGIN) ...")
    db.execute("BEGIN")
    
    # 1. Debit Sender
    db.execute("UPDATE wallets SET balance = 950000.00 WHERE w_id = 1")
    db.execute("INSERT INTO tx_log VALUES (5001, 1, 50000.00, 'DEBIT')")
    print("... Debited Stark Wallet ...")
    
    # 2. Simulating Network Failure
    print("... ⚠️  CRITICAL ERROR: DESTINATION WALLET API TIMEOUT ...")
    
    print("\n[3.1] EXECUTING EMERGENCY ROLLBACK")
    db.execute("ROLLBACK")
    print("✓ Rollback successful.")
    
    # Verify Balances
    print("\n[3.2] Auditing Stark Wallet Balance:")
    res = db.execute("SELECT balance FROM wallets WHERE w_id = 1")
    val = res['rows'][0][0]
    print(f"   Expected: 1000000.0 | Actual: {val}")
    if val == 1000000.0:
        print("   ✅ FUNDS SAFE. No money was lost.")

    # ---------------------------------------------------------
    # 4. ANALYTICS & REPORTING
    # ---------------------------------------------------------
    print_header("REAL-TIME LIQUIDITY REPORTING")
    
    # Seed some valid transactions
    db.execute("INSERT INTO tx_log VALUES (1, 1, 150.00, 'FEE')")
    db.execute("INSERT INTO tx_log VALUES (2, 1, 5000.00, 'DEBIT')")
    db.execute("INSERT INTO tx_log VALUES (3, 2, 200.00, 'CREDIT')")
    db.execute("INSERT INTO tx_log VALUES (4, 1, 150.00, 'FEE')")
    db.execute("INSERT INTO tx_log VALUES (5, 2, 5000.00, 'CREDIT')")

    print("Query: Total Volume by Transaction Type")
    res = db.execute("SELECT type, COUNT(*), SUM(amount) FROM tx_log GROUP BY type")
    
    print("\nTYPE    | COUNT | TOTAL VOLUME")
    print("-" * 30)
    if 'rows' in res:
        for row in res['rows']:
            print(f"{row[0]:<7} |   {row[1]}   | ${row[2]:,.2f}")

    print("\nQuery: Wealthiest Customers (JOIN Wallets -> Customers)")
    res = db.execute("""
        SELECT customers.name, wallets.currency, wallets.balance 
        FROM wallets 
        JOIN customers ON wallets.c_id = customers.c_id
    """)
    if 'rows' in res:
        for r in res['rows']:
            print(f" - {r[0]}: {r[2]:,.2f} {r[1]}")

    # ---------------------------------------------------------
    # 5. CONSOLE
    # ---------------------------------------------------------
    print_header("LAUNCHING ADMIN SHELL")
    repl = SQLREPL(db)
    repl.run()

if __name__ == "__main__":
    run_fintech_demo()