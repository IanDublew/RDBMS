# rdbms_core.py
import re
import os
import pickle
import datetime
from enum import Enum
from typing import Dict, List, Any, Set, Tuple

class DataType(Enum):
    INTEGER = "INTEGER"
    TEXT = "TEXT"
    REAL = "REAL"
    BOOLEAN = "BOOLEAN"
    DATE = "DATE"

class Constraint(Enum):
    PRIMARY_KEY = "PRIMARY KEY"
    UNIQUE = "UNIQUE"
    NOT_NULL = "NOT NULL"
    FOREIGN_KEY = "FOREIGN KEY"

class Transaction:
    def __init__(self):
        self.active = False
        self.undo_stack: List[Tuple[str, Any]] = [] 

    def begin(self):
        self.active = True
        self.undo_stack = []

    def log_insert(self, table_name: str, row_id: int):
        if self.active: self.undo_stack.append(('DELETE', (table_name, row_id)))

    def log_delete(self, table_name: str, row_data: List[Any], row_id: int):
        if self.active: self.undo_stack.append(('INSERT', (table_name, row_data, row_id)))

    def log_update(self, table_name: str, row_id: int, old_data: List[Any]):
        if self.active: self.undo_stack.append(('UPDATE', (table_name, row_id, old_data)))

class Table:
    def __init__(self, name: str, columns: List[Dict], primary_key: List[str] = None, foreign_keys: List[Dict] = None):
        self.name = name
        self.columns = columns
        self.primary_key = primary_key or []
        self.foreign_keys = foreign_keys or []
        self.rows: Dict[int, List[Any]] = {}
        self.indexes: Dict[str, Dict[Any, Set[int]]] = {}
        self.row_id_counter = 1
        self.column_map = {col['name']: idx for idx, col in enumerate(columns)}
        
        # Unique Columns (Non-PK)
        self.unique_columns = []
        for col in columns:
            is_pk = primary_key and len(primary_key) == 1 and primary_key[0] == col['name']
            if Constraint.UNIQUE in col['constraints'] and not is_pk:
                self.unique_columns.append(col['name'])
        
        if primary_key: self.indexes['__pk__'] = {}
        for col in self.unique_columns:
            self.indexes[f"__uniq_{col}"] = {}

    def insert(self, values: List[Any], row_id: int = None) -> int:
        if len(values) != len(self.columns): raise ValueError(f"Column count mismatch in {self.name}")
        validated = [self._validate(c, v) for c, v in zip(self.columns, values)]

        # 1. PK Check
        if self.primary_key:
            pk_val = validated[self.column_map[self.primary_key[0]]]
            if pk_val in self.indexes.get('__pk__', {}): raise ValueError(f"Primary Key violation: {pk_val}")

        # 2. Unique Check
        for col in self.unique_columns:
            val = validated[self.column_map[col]]
            if val is not None:
                idx_name = f"__uniq_{col}"
                if val in self.indexes[idx_name]:
                    raise ValueError(f"UNIQUE constraint violation: Column '{col}' value '{val}' already exists")

        # 3. Row ID Resolution
        final_id = row_id if row_id is not None else self.row_id_counter
        
        # Ensure we don't overwrite if manual ID was passed that conflicts with counter logic (rare collision check)
        if final_id in self.rows:
             raise ValueError(f"System Error: Row ID {final_id} already occupied.")

        self.rows[final_id] = validated
        
        # Advance counter if we used it or surpassed it
        if final_id >= self.row_id_counter:
            self.row_id_counter = final_id + 1
        
        self._update_indexes(final_id, validated)
        return final_id

    def delete(self, row_id: int):
        if row_id in self.rows:
            self._remove_from_indexes(row_id, self.rows[row_id])
            del self.rows[row_id]

    def update_row(self, row_id: int, new_values: List[Any]):
        if row_id not in self.rows: return
        
        # Unique Check on Update
        for col in self.unique_columns:
            col_idx = self.column_map[col]
            val = new_values[col_idx]
            if val is not None:
                idx_name = f"__uniq_{col}"
                if val in self.indexes[idx_name]:
                    existing_ids = self.indexes[idx_name][val]
                    if row_id not in existing_ids:
                         raise ValueError(f"UNIQUE constraint violation: Column '{col}' value '{val}' already exists")

        self._remove_from_indexes(row_id, self.rows[row_id])
        self.rows[row_id] = new_values
        self._update_indexes(row_id, new_values)

    def _validate(self, col: Dict, val: Any) -> Any:
        if val is None:
            if Constraint.NOT_NULL in col['constraints'] or Constraint.PRIMARY_KEY in col['constraints']:
                raise ValueError(f"NULL violation in {col['name']}")
            return None
        t = col['type'].upper()
        try:
            if t == 'INTEGER': return int(val)
            if t == 'REAL': return float(val)
            if t == 'TEXT': return str(val)
            if t == 'BOOLEAN': return str(val).upper() == 'TRUE' if isinstance(val, str) else bool(val)
            if t == 'DATE': return str(val)
        except: raise ValueError(f"Type mismatch: {val} is not {t}")
        return val

    def _update_indexes(self, row_id: int, values: List[Any]):
        if self.primary_key:
            pk_val = values[self.column_map[self.primary_key[0]]]
            if '__pk__' not in self.indexes: self.indexes['__pk__'] = {}
            self.indexes['__pk__'][pk_val] = {row_id}
        
        for col in self.unique_columns:
            val = values[self.column_map[col]]
            if val is not None:
                idx = self.indexes[f"__uniq_{col}"]
                if val not in idx: idx[val] = set()
                idx[val].add(row_id)

        for name, data in self.indexes.items():
            if name.startswith('__'): continue
            col = name.replace("idx_", "")
            if col in self.column_map:
                val = values[self.column_map[col]]
                if val not in data: data[val] = set()
                data[val].add(row_id)

    def _remove_from_indexes(self, row_id: int, values: List[Any]):
        for name, data in self.indexes.items():
            col = None
            if name == '__pk__': col = self.primary_key[0]
            elif name.startswith('__uniq_'): col = name.replace('__uniq_', '')
            else: col = name.replace("idx_", "")
            
            if col in self.column_map:
                val = values[self.column_map[col]]
                if val in data:
                    data[val].discard(row_id)
                    if not data[val]: del data[val]

    def select(self, conditions: List[Tuple] = None) -> List[Tuple[int, List[Any]]]:
        results = []
        pk_lookup = False
        if conditions and self.primary_key:
            for col, op, val in conditions:
                if col == self.primary_key[0] and op == '=':
                    idx = self.indexes.get('__pk__', {})
                    if val in idx:
                        for rid in idx[val]:
                            if self._match(self.rows[rid], conditions):
                                results.append((rid, self.rows[rid]))
                    pk_lookup = True
                    break
        if pk_lookup: return results
        for rid, row in self.rows.items():
            if self._match(row, conditions): results.append((rid, row))
        return results

    def _match(self, row: List, conds: List[Tuple]) -> bool:
        if not conds: return True
        for col, op, val in conds:
            if col not in self.column_map: continue
            cell = row[self.column_map[col]]
            if cell is None: return False
            try:
                if op == '=': match = str(cell) == str(val)
                elif op == '!=': match = cell != val
                elif op == '>': match = cell > val
                elif op == '<': match = cell < val
                elif op == 'LIKE': match = str(val).replace('%','') in str(cell)
                else: match = False
            except: match = False
            if not match: return False
        return True

    def create_index(self, name: str, col: str):
        if col not in self.column_map: raise ValueError(f"Column {col} not found")
        self.indexes[name] = {}
        idx = self.column_map[col]
        for rid, row in self.rows.items():
            val = row[idx]
            if val not in self.indexes[name]: self.indexes[name][val] = set()
            self.indexes[name][val].add(rid)

class SimpleRDBMS:
    def __init__(self, path: str = None):
        self.tables: Dict[str, Table] = {}
        self.path = path
        self.trx = Transaction()
        if path and os.path.exists(path): self.load()

    def execute(self, query: str) -> Dict[str, Any]:
        try:
            q = " ".join(query.strip().split())
            if not q: return {'status': 'error', 'message': 'Empty query'}
            
            self._log_query(q)
            
            cmd = q.split(' ')[0].upper()
            if cmd == "BEGIN": 
                self.trx.begin(); return {'status': 'success', 'message': 'Transaction Started'}
            if cmd == "COMMIT":
                self.trx.active = False; self.trx.undo_stack = []
                return {'status': 'success', 'message': 'Transaction Committed'}
            if cmd == "ROLLBACK": return self._rollback()
            
            if cmd == "CREATE": return self._create(q)
            if cmd == "INSERT": return self._insert(q)
            if cmd == "SELECT": return self._select(q)
            if cmd == "UPDATE": return self._update(q)
            if cmd == "DELETE": return self._delete(q)
            if cmd == "DROP": return self._drop(q)
            
            return {'status': 'error', 'message': f"Unknown command: {cmd}"}
        except Exception as e:
            return {'status': 'error', 'message': f"Error: {str(e)}"}

    def _log_query(self, q):
        try:
            with open("audit.log", "a") as f:
                f.write(f"[{datetime.datetime.now().isoformat()}] {q}\n")
        except: pass

    def _rollback(self):
        if not self.trx.active: return {'status': 'error', 'message': 'No active transaction'}
        cnt = 0
        while self.trx.undo_stack:
            op, pay = self.trx.undo_stack.pop()
            cnt += 1
            if op == 'DELETE': self.tables[pay[0]].delete(pay[1])
            elif op == 'INSERT': self.tables[pay[0]].insert(pay[1], row_id=pay[2])
            elif op == 'UPDATE': self.tables[pay[0]].update_row(pay[1], pay[2])
        self.trx.active = False
        return {'status': 'success', 'message': f'Rolled back {cnt} operations'}

    def _create(self, q):
        if "INDEX" in q.upper():
            m = re.match(r"CREATE INDEX (\w+) ON (\w+)\s*\((.*)\)", q, re.IGNORECASE)
            self.tables[m.group(2)].create_index(m.group(1), m.group(3).strip())
            return {'status': 'success', 'message': 'Index created'}
        m = re.match(r"CREATE TABLE (\w+)\s*\((.*)\)", q, re.IGNORECASE)
        name, body = m.groups()
        defs, curr, lvl = [], "", 0
        for c in body:
            if c == '(': lvl += 1
            elif c == ')': lvl -= 1
            if c == ',' and lvl == 0: defs.append(curr.strip()); curr = ""
            else: curr += c
        defs.append(curr.strip())
        
        cols, pks, fks = [], [], []
        for d in defs:
            if d.upper().startswith("FOREIGN KEY"):
                fm = re.match(r"FOREIGN KEY\s*\((.*?)\)\s*REFERENCES\s*(\w+)\s*\((.*?)\)", d, re.IGNORECASE)
                fks.append({'col': fm.group(1).strip(), 'ref_table': fm.group(2).strip(), 'ref_col': fm.group(3).strip()})
            else:
                p = d.split()
                cn, ct = p[0], p[1]
                cs = []
                d_up = d.upper()
                if "PRIMARY KEY" in d_up: cs.append(Constraint.PRIMARY_KEY); pks.append(cn)
                if "NOT NULL" in d_up: cs.append(Constraint.NOT_NULL)
                if "UNIQUE" in d_up: cs.append(Constraint.UNIQUE)
                cols.append({'name': cn, 'type': ct, 'constraints': cs})
        self.tables[name] = Table(name, cols, pks, fks)
        return {'status': 'success', 'message': f'Table {name} created'}

    def _insert(self, q):
        m = re.match(r"INSERT INTO (\w+).+VALUES\s*\((.*)\)", q, re.IGNORECASE)
        tn, vstr = m.groups()
        if tn not in self.tables: raise ValueError(f"Table {tn} not found")
        t = self.tables[tn]
        vals = self._parse_args(vstr)
        
        for fk in t.foreign_keys:
            val = vals[t.column_map[fk['col']]]
            if val is not None:
                rt = self.tables[fk['ref_table']]
                ridx = rt.column_map[fk['ref_col']]
                found = False
                if fk['ref_col'] == rt.primary_key[0] and val in rt.indexes.get('__pk__', {}): found = True
                else:
                    for r in rt.rows.values():
                        if str(r[ridx]) == str(val): found = True; break
                if not found: raise ValueError(f"FK Integrity Error: {val} not in {fk['ref_table']}")

        # --- FIX: Use PK as Row ID if Integer ---
        rid = None
        if t.primary_key:
            pk_idx = t.column_map[t.primary_key[0]]
            pk_val = vals[pk_idx]
            if isinstance(pk_val, int):
                rid = pk_val

        final_rid = t.insert(vals, row_id=rid)
        self.trx.log_insert(tn, final_rid)
        return {'status': 'success', 'row_id': final_rid}

    def _delete(self, q):
        m = re.match(r"DELETE FROM (\w+)(?:\s+WHERE\s+(.*))?", q, re.IGNORECASE)
        tn, where = m.groups()
        t = self.tables[tn]
        rows = t.select(self._parse_where(where))
        
        for rid, row in rows:
            for oname, otbl in self.tables.items():
                for fk in otbl.foreign_keys:
                    if fk['ref_table'] == tn:
                        pidx = t.column_map[fk['ref_col']]
                        cidx = otbl.column_map[fk['col']]
                        for crow in otbl.rows.values():
                            if str(crow[cidx]) == str(row[pidx]):
                                raise ValueError(f"FK Integrity Error: Referenced by {oname}")
        
        for rid, row in rows:
            self.trx.log_delete(tn, row[:], rid)
            t.delete(rid)
        return {'status': 'success', 'rows_affected': len(rows)}

    def _update(self, q):
        m = re.match(r"UPDATE (\w+) SET (.*?)(?:\s+WHERE\s+(.*))?", q, re.IGNORECASE)
        tn, sstr, where = m.groups()
        t = self.tables[tn]
        
        # --- FIX: Robust Parsing ---
        ups = {}
        for x in sstr.split(','):
            parts = x.split('=', 1)
            if len(parts) != 2: raise ValueError(f"Invalid SET clause: '{x}'")
            ups[parts[0].strip()] = self._val(parts[1])

        rows = t.select(self._parse_where(where))
        for rid, row in rows:
            self.trx.log_update(tn, rid, row[:])
            nr = row[:]
            for k,v in ups.items(): 
                if k in t.column_map: nr[t.column_map[k]] = v
            t.update_row(rid, nr)
        return {'status': 'success', 'rows_affected': len(rows)}

    def _select(self, q):
        m = re.match(r"SELECT (.*?) FROM (\w+)(?:\s+WHERE\s+(.*))?", q, re.IGNORECASE)
        cols, tn, where = m.groups()
        t = self.tables[tn]
        data = t.select(self._parse_where(where))
        rows = [d[1] for d in data]
        cnames = [c['name'] for c in t.columns]
        if cols.strip() != '*':
            req = [c.strip() for c in cols.split(',')]
            idxs = [t.column_map[c] for c in req if c in t.column_map]
            rows = [[r[i] for i in idxs] for r in rows]
            cnames = req
        return {'status': 'success', 'columns': cnames, 'rows': rows}

    def _drop(self, q):
        self.tables.pop(q.split()[-1], None)
        return {'status': 'success'}

    def _val(self, s):
        s = s.strip()
        if s.upper() == 'NULL': return None
        if s[0] in "'\"": return s[1:-1]
        try: return float(s) if '.' in s else int(s)
        except: return s

    def _parse_args(self, s):
        args, curr, qt = [], [], False
        for c in s:
            if c in "'\"": qt = not qt
            if c == ',' and not qt: args.append("".join(curr).strip()); curr=[]
            else: curr.append(c)
        args.append("".join(curr).strip())
        return [self._val(x) for x in args]

    def _parse_where(self, s) -> List[Tuple]:
        if not s: return []
        conds = []
        parts = s.split(' AND ') if ' AND ' in s else [s]
        ops = ['>=', '<=', '=', '>', '<', 'LIKE']
        for p in parts:
            matched = False
            for op in ops:
                if op in p:
                    l, r = p.split(op, 1)
                    conds.append((l.strip(), op, self._val(r)))
                    matched = True
                    break
        return conds

    def save(self, p=None):
        d = {n: {'cols': t.columns, 'pk': t.primary_key, 'fk': t.foreign_keys, 'rows': t.rows, 'ctr': t.row_id_counter, 'idx': t.indexes} for n, t in self.tables.items()}
        with open(p or self.path, 'wb') as f: pickle.dump(d, f)
    
    def load(self, p=None):
        if not os.path.exists(p or self.path): return
        with open(p or self.path, 'rb') as f:
            for n, d in pickle.load(f).items():
                t = Table(n, d['cols'], d['pk'], d.get('fk'))
                t.rows, t.row_id_counter, t.indexes = d['rows'], d['ctr'], d['idx']
                self.tables[n] = t