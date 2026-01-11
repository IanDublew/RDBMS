# web_app.py
from flask import Flask, render_template_string, request, jsonify, redirect, url_for, flash
from rdbms_enhanced import EnhancedRDBMS
import os
import time
import random
from datetime import datetime, timedelta

app = Flask(__name__)
app.secret_key = 'fintech_secure_key'
DB_FILE = "ledger.db"

# --- CUSTOM FILTERS (Safe Number Formatting) ---
@app.template_filter('money')
def format_currency(value):
    try:
        if value is None: return "0"
        return "{:,.0f}".format(float(value))
    except:
        return "0"

@app.template_filter('money_dec')
def format_currency_dec(value):
    try:
        if value is None: return "0.00"
        return "{:,.2f}".format(float(value))
    except:
        return "0.00"

# --- DATABASE FACTORY ---
class LedgerSystem:
    def __init__(self, path):
        self.path = path
        self.db = None
        self.load_or_init()

    def get_instance(self): return self.db

    def load_or_init(self):
        if os.path.exists(self.path):
            try: self.db = EnhancedRDBMS(self.path)
            except: self._seed()
        else: self._seed()

    def reset(self):
        if os.path.exists(self.path): os.remove(self.path)
        self._seed()

    def _seed(self):
        print("--- PROVISIONING LEDGER ---")
        self.db = EnhancedRDBMS(self.path)
        self.db.execute("CREATE TABLE users (uid INTEGER PRIMARY KEY, name TEXT, kyc_level TEXT, country TEXT)")
        self.db.execute("CREATE TABLE accounts (acc_id INTEGER PRIMARY KEY, uid INTEGER, currency TEXT, balance REAL, status TEXT)")
        self.db.execute("CREATE TABLE ledger (tx_id INTEGER PRIMARY KEY, acc_id INTEGER, amount REAL, type TEXT, date DATE)")

        users = [(101,'Alpha Capital','TIER_1','USA'), (102,'Beta Trading','TIER_2','UK'), (103,'Gamma Retail','TIER_3','SG'), (104,'Delta Hedge','TIER_1','USA')]
        for u in users: self.db.execute(f"INSERT INTO users VALUES ({u[0]}, '{u[1]}', '{u[2]}', '{u[3]}')")

        accounts = [(1,101,'USD',5000000.0,'ACTIVE'), (2,102,'GBP',250000.0,'ACTIVE'), (3,103,'SGD',12000.0,'FROZEN'), (4,104,'USD',8900000.0,'ACTIVE')]
        for a in accounts: self.db.execute(f"INSERT INTO accounts VALUES ({a[0]}, {a[1]}, '{a[2]}', {a[3]}, '{a[4]}')")

        for i in range(1, 101):
            acc = random.choice([1, 2, 3, 4])
            amt = round(random.uniform(10, 50000), 2)
            tt = random.choice(['WIRE_IN', 'WIRE_OUT', 'FEE', 'FX_SWAP'])
            ds = (datetime.now() - timedelta(days=random.randint(0,7))).strftime('%Y-%m-%d')
            self.db.execute(f"INSERT INTO ledger VALUES ({i}, {acc}, {amt}, '{tt}', '{ds}')")

        self.db.execute("CREATE INDEX idx_ledger_type ON ledger (type)")
        self.db.save(self.path)

ledger = LedgerSystem(DB_FILE)

# --- ROUTES ---

@app.route('/')
def dashboard():
    db = ledger.get_instance()
    
    # Safe data retrieval with defaults
    res_aum = db.execute("SELECT SUM(balance) FROM accounts WHERE currency = 'USD'")
    aum = res_aum.get('rows', [[0]])[0][0] or 0
    
    res_vol = db.execute("SELECT COUNT(*), SUM(amount) FROM ledger")
    row_vol = res_vol.get('rows', [[0,0]])[0]
    tx_count, tx_vol = row_vol[0], row_vol[1]
    
    res_flags = db.execute("SELECT COUNT(*) FROM accounts WHERE status = 'FROZEN'")
    flag_count = res_flags.get('rows', [[0]])[0][0]
    
    # SINGLE JOIN Query (Safe)
    res_recent = db.execute("""
        SELECT ledger.tx_id, accounts.currency, ledger.type, ledger.amount 
        FROM ledger 
        JOIN accounts ON ledger.acc_id = accounts.acc_id
    """)
    recent_txs = res_recent.get('rows', [])[-5:] 
    
    return render_template_string(BASE_TEMPLATE, page='dashboard', 
                                  aum=aum, tx_count=tx_count, tx_vol=tx_vol, flag_count=flag_count,
                                  recent_txs=recent_txs)

# Renamed to /consoles to avoid Werkzeug Debugger conflict
@app.route('/consoles')
def console():
    # Pass defaults to prevent template crashing
    return render_template_string(BASE_TEMPLATE, page='consoles', 
                                  aum=0, tx_count=0, tx_vol=0, flag_count=0, recent_txs=[])

@app.route('/data')
def data_explorer():
    db = ledger.get_instance()
    tables = [{'name': k, 'rows': len(v.rows), 'cols': [c['name'] for c in v.columns]} for k,v in db.tables.items()]
    return render_template_string(BASE_TEMPLATE, page='data', tables=tables,
                                  aum=0, tx_count=0, tx_vol=0, flag_count=0, recent_txs=[])

@app.route('/inspect/<table_name>')
def inspect(table_name):
    db = ledger.get_instance()
    if table_name not in db.tables: return redirect('/')
    t = db.tables[table_name]
    raw = {k: v for i, (k, v) in enumerate(t.rows.items()) if i < 15}
    return render_template_string(BASE_TEMPLATE, page='inspect', t_name=table_name, raw_rows=raw, indexes=t.indexes,
                                  aum=0, tx_count=0, tx_vol=0, flag_count=0, recent_txs=[])

@app.route('/api/query', methods=['POST'])
def api_query():
    db = ledger.get_instance()
    query = request.json.get('query')
    start = time.perf_counter()
    try:
        res = db.execute(query)
        # Auto-save for modifying queries
        if any(x in query.upper() for x in ['INSERT', 'UPDATE', 'DELETE']): db.save(DB_FILE)
        success = res.get('status') == 'success'
    except Exception as e:
        res = {'status': 'error', 'message': str(e)}
        success = False
    dur = (time.perf_counter() - start) * 1000
    return jsonify({'result': res, 'ms': f"{dur:.2f}", 'success': success})

@app.route('/reset')
def reset():
    ledger.reset()
    flash("System reset.", "warning")
    return redirect('/')

# --- UI TEMPLATE ---
BASE_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Core Ledger | FinTech OS</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css" rel="stylesheet">
    <style>
        :root { --nav-bg: #0f172a; --accent: #10b981; --bg: #f8fafc; }
        body { background: var(--bg); font-family: 'Inter', system-ui, sans-serif; }
        .navbar { background: var(--nav-bg); padding: 0.8rem 1rem; }
        .navbar-brand { font-weight: 700; letter-spacing: -0.5px; color: white !important; }
        .nav-link { color: #94a3b8 !important; font-size: 0.9rem; margin-left: 1rem; }
        .nav-link.active { color: white !important; }
        .stat-card { background: white; border: none; border-radius: 12px; padding: 1.5rem; box-shadow: 0 1px 3px rgba(0,0,0,0.05); height: 100%; }
        .stat-label { color: #64748b; font-size: 0.75rem; text-transform: uppercase; font-weight: 600; }
        .stat-val { font-size: 1.75rem; font-weight: 700; color: #0f172a; margin-top: 0.5rem; }
        .icon-box { width: 40px; height: 40px; border-radius: 8px; display: flex; align-items: center; justify-content: center; font-size: 1.2rem; }
        .console-editor { background: #1e293b; color: #e2e8f0; font-family: 'JetBrains Mono', monospace; border: none; border-radius: 8px; padding: 1.5rem; width: 100%; min-height: 200px; }
        .table-custom th { background: #f1f5f9; font-size: 0.75rem; text-transform: uppercase; color: #475569; border: none; }
        .table-custom td { border-bottom: 1px solid #f1f5f9; font-size: 0.9rem; padding: 1rem 0.75rem; }
    </style>
</head>
<body>
<nav class="navbar navbar-expand-lg fixed-top">
    <div class="container-fluid">
        <a class="navbar-brand" href="/"><i class="fas fa-cube me-2 text-success"></i>FIN<span class="text-white-50">LEDGER</span></a>
        <div class="collapse navbar-collapse">
            <ul class="navbar-nav ms-auto">
                <li class="nav-item"><a class="nav-link {% if page=='dashboard' %}active{% endif %}" href="/">Overview</a></li>
                <li class="nav-item"><a class="nav-link {% if page=='consoles' %}active{% endif %}" href="/consoles">SQL Terminal</a></li>
                <li class="nav-item"><a class="nav-link {% if page=='data' %}active{% endif %}" href="/data">Data Governance</a></li>
                <li class="nav-item"><a class="nav-link text-danger" href="/reset">Reset System</a></li>
            </ul>
        </div>
    </div>
</nav>
<div class="container" style="margin-top: 80px; padding-bottom: 50px;">
    {% if page == 'dashboard' %}
    <div class="row g-4 mb-4">
        <div class="col-md-3"><div class="stat-card"><div><div class="stat-label">USD Liquidity</div><div class="stat-val">${{ aum|money }}</div></div><div class="icon-box bg-success bg-opacity-10 text-success"><i class="fas fa-coins"></i></div></div></div>
        <div class="col-md-3"><div class="stat-card"><div><div class="stat-label">24h Volume</div><div class="stat-val">${{ tx_vol|money }}</div></div><div class="icon-box bg-primary bg-opacity-10 text-primary"><i class="fas fa-chart-line"></i></div></div></div>
        <div class="col-md-3"><div class="stat-card"><div><div class="stat-label">Transactions</div><div class="stat-val">{{ tx_count }}</div></div><div class="icon-box bg-info bg-opacity-10 text-info"><i class="fas fa-receipt"></i></div></div></div>
        <div class="col-md-3"><div class="stat-card"><div><div class="stat-label">Frozen</div><div class="stat-val text-danger">{{ flag_count }}</div></div><div class="icon-box bg-danger bg-opacity-10 text-danger"><i class="fas fa-ban"></i></div></div></div>
    </div>
    <div class="row">
        <div class="col-lg-8">
            <div class="stat-card">
                <h6 class="mb-4 fw-bold">Recent Ledger Activity</h6>
                <table class="table table-custom mb-0">
                    <thead><tr><th>TX ID</th><th>Currency</th><th>Type</th><th>Amount</th></tr></thead>
                    <tbody>
                        {% for tx in recent_txs %}
                        <tr>
                            <td><span class="badge bg-light text-dark border">#{{ tx[0] }}</span></td>
                            <td class="fw-bold">{{ tx[1] }}</td>
                            <td><span class="badge bg-primary bg-opacity-10 text-primary">{{ tx[2] }}</span></td>
                            <td class="font-monospace">${{ tx[3]|money_dec }}</td>
                        </tr>
                        {% endfor %}
                    </tbody>
                </table>
            </div>
        </div>
        <div class="col-lg-4">
            <div class="stat-card bg-dark text-white">
                <h6 class="mb-3 text-white-50">System Status</h6>
                <div class="d-flex justify-content-between mb-2"><span>Core Engine</span><span class="text-success">ONLINE <i class="fas fa-circle fa-xs"></i></span></div>
                <div class="d-flex justify-content-between mb-2"><span>ACID Mode</span><span class="text-success">ACTIVE <i class="fas fa-lock"></i></span></div>
                <hr class="border-secondary"><small class="text-white-50">PyRDBMS Enterprise v2.1</small>
            </div>
        </div>
    </div>
    {% elif page == 'consoles' %}
    <div class="row">
        <div class="col-lg-9">
            <h4 class="mb-3 fw-bold">SQL Command Center</h4>
            <div class="card border-0 shadow-sm mb-4">
                <div class="card-body p-0">
                    <textarea id="sqlInput" class="console-editor" spellcheck="false">SELECT accounts.currency, SUM(ledger.amount) 
FROM ledger 
JOIN accounts ON ledger.acc_id = accounts.acc_id 
GROUP BY accounts.currency</textarea>
                    <div class="d-flex justify-content-between p-3 bg-white border-top rounded-bottom">
                        <span id="timing" class="text-muted small align-self-center">Ready.</span>
                        <button onclick="runQuery()" class="btn btn-success fw-bold px-4"><i class="fas fa-play me-2"></i>RUN</button>
                    </div>
                </div>
            </div>
            <div id="resultsArea" class="stat-card d-none"></div>
        </div>
        <div class="col-lg-3">
            <h6 class="text-muted text-uppercase small fw-bold mb-3">Templates</h6>
            <div class="list-group list-group-flush shadow-sm rounded-3">
                <button onclick="setSql('SELECT * FROM users')" class="list-group-item list-group-item-action">All Clients</button>
                <button onclick="setSql('SELECT currency, SUM(balance) FROM accounts GROUP BY currency')" class="list-group-item list-group-item-action">Liquidity</button>
            </div>
        </div>
    </div>
    {% elif page == 'data' %}
    <h4 class="mb-4 fw-bold">Data Governance</h4>
    <div class="row g-4">
        {% for t in tables %}
        <div class="col-md-6"><div class="stat-card h-100"><div class="d-flex justify-content-between align-items-center mb-3"><h5 class="fw-bold mb-0 text-primary">{{ t.name }}</h5><a href="/inspect/{{ t.name }}" class="btn btn-sm btn-outline-secondary">Inspect</a></div><div class="mb-2"><span class="badge bg-light text-dark border">{{ t.rows }} Records</span></div><p class="text-muted small mb-0">{{ t.cols|join(', ') }}</p></div></div>
        {% endfor %}
    </div>
    {% elif page == 'inspect' %}
    <div class="d-flex justify-content-between align-items-center mb-4"><h4>In-Memory: <span class="font-monospace text-primary">{{ t_name }}</span></h4><a href="/data" class="btn btn-outline-dark">Back</a></div>
    <div class="row">
        <div class="col-lg-7"><div class="stat-card"><h6 class="mb-3 border-bottom pb-2">Heap Storage</h6><div style="font-family: monospace; font-size: 0.85rem;">{% for rid, val in raw_rows.items() %}<div class="d-flex mb-2"><span class="text-success me-3 fw-bold">ID:{{ rid }}</span><span class="text-muted">{{ val }}</span></div>{% endfor %}</div></div></div>
        <div class="col-lg-5"><div class="stat-card"><h6 class="mb-3 border-bottom pb-2">Indexes</h6>{% for name, map in indexes.items() %}<div class="mb-4"><div class="badge bg-secondary mb-2">{{ name }}</div>{% for k, v in map.items() %}{% if loop.index < 5 %}<div class="small text-muted">{{ k }} &rarr; {{ v }}</div>{% endif %}{% endfor %}</div>{% endfor %}</div></div>
    </div>
    {% endif %}
</div>
<script>
    function setSql(q) { document.getElementById('sqlInput').value = q; }
    async function runQuery() {
        const q = document.getElementById('sqlInput').value;
        const resDiv = document.getElementById('resultsArea');
        const timing = document.getElementById('timing');
        timing.innerHTML = '<span class="text-primary"><i class="fas fa-circle-notch fa-spin"></i> Processing...</span>';
        const resp = await fetch('/api/query', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({query: q}) });
        const data = await resp.json();
        timing.innerText = `Time: ${data.ms} ms`;
        resDiv.classList.remove('d-none');
        if(!data.success) { resDiv.innerHTML = `<div class="alert alert-danger mb-0">${data.result.message}</div>`; return; }
        const r = data.result;
        if(r.rows) {
            if(r.rows.length === 0) { resDiv.innerHTML = '<div class="text-muted text-center p-3">No results.</div>'; return; }
            let h = '<div class="table-responsive"><table class="table table-custom mb-0"><thead><tr>';
            r.columns.forEach(c => h += `<th>${c}</th>`);
            h += '</tr></thead><tbody>';
            r.rows.forEach(row => { h += '<tr>'; row.forEach(cell => h += `<td>${cell}</td>`); h += '</tr>'; });
            h += '</tbody></table></div>';
            resDiv.innerHTML = h;
        } else { resDiv.innerHTML = `<div class="alert alert-success mb-0">${r.message || 'Success'}</div>`; }
    }
</script>
</body>
</html>
"""

if __name__ == "__main__":
    app.run(debug=True, port=5000)