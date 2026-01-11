# rdbms_enhanced.py
from rdbms_core import SimpleRDBMS
import re
import time

class EnhancedRDBMS(SimpleRDBMS):
    def execute(self, query: str):
        clean_query = " ".join(query.strip().split())
        self._log_query(clean_query) # Log before routing
        
        q_upper = clean_query.upper()
        if q_upper.startswith("SELECT"):
            if " JOIN " in q_upper: return self._exec_join(clean_query)
            if "GROUP BY" in q_upper or any(f in q_upper for f in ["SUM(", "COUNT(", "AVG(", "MIN(", "MAX("]):
                return self._exec_aggregate(clean_query)
        return super().execute(clean_query)

    def _exec_aggregate(self, query):
        m = re.match(r"SELECT (.*?) FROM (\w+)(?:\s+WHERE\s+(.*?))?(?:\s+GROUP BY\s+(.*))?$", query, re.IGNORECASE)
        if not m: return super().execute(query)
        
        sel_clause, t_name, where_clause, group_clause = m.groups()
        if t_name not in self.tables: return {'status': 'error', 'message': 'Table not found'}
        table = self.tables[t_name]
        
        # New: _parse_where returns list, select accepts list
        raw_data_tuples = table.select(self._parse_where(where_clause))
        rows = [x[1] for x in raw_data_tuples]
        
        groups = {}
        if group_clause:
            group_col = group_clause.strip()
            if group_col not in table.column_map: return {'status': 'error', 'message': f'Unknown GROUP BY column: {group_col}'}
            g_idx = table.column_map[group_col]
            for r in rows:
                key = r[g_idx]
                if key not in groups: groups[key] = []
                groups[key].append(r)
        else: groups['__global__'] = rows
        
        sel_parts = [x.strip() for x in sel_clause.split(',')]
        final_rows = []
        final_headers = []
        headers_built = False
        
        for g_key, g_rows in groups.items():
            result_row = []
            current_headers = []
            for part in sel_parts:
                agg_match = re.match(r"(SUM|AVG|COUNT|MIN|MAX)\((.*?)\)", part, re.IGNORECASE)
                if agg_match:
                    func, target = agg_match.groups()
                    target = target.strip()
                    val = 0
                    if func == 'COUNT': val = len(g_rows)
                    else:
                        if target in table.column_map:
                            t_idx = table.column_map[target]
                            values = [r[t_idx] for r in g_rows if isinstance(r[t_idx], (int, float))]
                            if not values: val = 0
                            elif func == 'SUM': val = sum(values)
                            elif func == 'AVG': val = sum(values) / len(values)
                            elif func == 'MIN': val = min(values)
                            elif func == 'MAX': val = max(values)
                    if isinstance(val, float): val = round(val, 2)
                    result_row.append(val)
                    if not headers_built: current_headers.append(f"{func}({target})")
                else:
                    if part in table.column_map:
                        idx = table.column_map[part]
                        result_row.append(g_rows[0][idx] if g_rows else None)
                        if not headers_built: current_headers.append(part)
            final_rows.append(result_row)
            if not headers_built: final_headers = current_headers; headers_built = True
        return {'status': 'success', 'columns': final_headers, 'rows': final_rows}

    def _exec_join(self, query):
        m = re.match(r"SELECT (.*?) FROM (\w+) (?:INNER )?JOIN (\w+) ON (.*)", query, re.IGNORECASE)
        if not m: return super().execute(query)
        cols_req, t1_name, t2_name, on_cond = m.groups()
        if t1_name not in self.tables or t2_name not in self.tables: return {'status': 'error', 'message': 'One or more tables not found'}
        if ' JOIN ' in on_cond.upper(): return {'status': 'error', 'message': 'Complex nested JOINs are not supported. Use Single JOIN.'}
        
        try:
            parts = on_cond.split('=')
            if len(parts) != 2: raise ValueError()
            left, right = [x.strip() for x in parts]
        except: return {'status': 'error', 'message': 'Invalid JOIN condition. Use: table1.col = table2.col'}
        
        t1, t2 = self.tables[t1_name], self.tables[t2_name]
        def resolve_col(ref): return ref.split('.') if '.' in ref else (None, ref)
        ltbl, lcol = resolve_col(left)
        rtbl, rcol = resolve_col(right)
        try:
            if ltbl == t1_name: idx1, idx2 = t1.column_map[lcol], t2.column_map[rcol]
            else: idx1, idx2 = t1.column_map[rcol], t2.column_map[lcol]
        except: return {'status': 'error', 'message': 'Column in JOIN ON clause not found'}
        
        build_tbl, probe_tbl = t2, t1
        build_idx, probe_idx = idx2, idx1
        build_name, probe_name = t2_name, t1_name
        if len(t1.rows) < len(t2.rows):
            build_tbl, probe_tbl = t1, t2
            build_idx, probe_idx = idx1, idx2
            build_name, probe_name = t1_name, t2_name

        hash_map = {}
        for row in build_tbl.rows.values():
            key = row[build_idx]
            if key not in hash_map: hash_map[key] = []
            hash_map[key].append(row)
        joined_data = [] 
        for row in probe_tbl.rows.values():
            key = row[probe_idx]
            if key in hash_map:
                for match in hash_map[key]:
                    if build_name == t1_name: joined_data.append({'t1': match, 't2': row})
                    else: joined_data.append({'t1': row, 't2': match})

        final_rows, final_headers = [], []
        all_cols = [f"{t1_name}.{c['name']}" for c in t1.columns] + [f"{t2_name}.{c['name']}" for c in t2.columns]
        target_cols = all_cols if cols_req.strip() == '*' else [c.strip() for c in cols_req.split(',')]
        for row_pair in joined_data:
            res_row, built_headers = [], not final_headers
            for req in target_cols:
                tbl_ref, col_ref = resolve_col(req)
                val = None
                if tbl_ref == t1_name: val = row_pair['t1'][t1.column_map[col_ref]]
                elif tbl_ref == t2_name: val = row_pair['t2'][t2.column_map[col_ref]]
                elif not tbl_ref:
                    if col_ref in t1.column_map: val = row_pair['t1'][t1.column_map[col_ref]]
                    elif col_ref in t2.column_map: val = row_pair['t2'][t2.column_map[col_ref]]
                res_row.append(val)
                if built_headers: final_headers.append(req)
            final_rows.append(res_row)
        return {'status': 'success', 'columns': final_headers, 'rows': final_rows}

class SQLREPL:
    def __init__(self, db): self.db = db
    def run(self):
        print("SQL Console Active"); 
        while True:
            try:
                q = input("SQL> ").strip()
                if q.lower() in ('exit','quit'): break
                res = self.db.execute(q)
                if 'rows' in res:
                    for row in res['rows']: print(row)
                else: print(res.get('message', 'OK'))
            except: break