"""Reproduce the results shown in the video on your laptop, without Databricks.

Uses pandas, SQLite and scikit-learn to run the same logic as the notebooks on the files in ../data.
Handy for checking your Databricks results, or for exploring the data before you sign up.

    pip install pandas scikit-learn
    python local/reproduce_results.py
"""
import json, os, sqlite3, sys
import numpy as np, pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, '..', 'data')
OUT = os.path.join(HERE, '..', 'expected_output')
os.makedirs(OUT, exist_ok=True)
RES = {}
UPDATE_ORDER_ID = 506319   # the order notebook 02 cancels


def show(key, df, note=''):
    RES[key] = dict(cols=list(map(str, df.columns)), rows=[[(None if (isinstance(v, float) and np.isnan(v)) else (v.item() if hasattr(v, 'item') else v)) for v in r] for r in df.itertuples(index=False)])
    txt = df.to_string(index=False)
    open(os.path.join(OUT, key + '.txt'), 'w').write((note + '\n' if note else '') + txt + '\n')
    print(f'\n== {key}\n{txt}')


def val(key, v):
    RES[key] = v; print(f'{key}: {v}')


orders_raw = pd.read_csv(f'{DATA}/orders.csv', parse_dates=['order_ts'])
items_raw = pd.read_csv(f'{DATA}/order_items.csv')
products = pd.read_csv(f'{DATA}/products.csv')
customers = pd.read_csv(f'{DATA}/customers.csv', parse_dates=['signup_date'])
updates = pd.read_csv(f'{DATA}/orders_updates.csv', parse_dates=['order_ts'])

# ------------------------------------------------------------------ 01 spark basics
val('01_rows', len(orders_raw))
show('01_head', orders_raw.head(5).assign(order_ts=lambda d: d.order_ts.dt.strftime('%Y-%m-%d %H:%M:%S')))
show('01_by_status', orders_raw.groupby('status').size().rename('orders').reset_index().sort_values('orders', ascending=False))
rc = items_raw.merge(products[['product_id', 'category']], on='product_id').assign(amount=lambda d: d.quantity * d.unit_price)
show('01_rev_by_cat', rc.groupby('category').agg(revenue_yen=('amount', 'sum'), orders=('order_id', 'nunique')).reset_index().sort_values('revenue_yen', ascending=False))
ch = orders_raw.assign(h=orders_raw.order_ts.dt.hour, c=(orders_raw.status == 'cancelled').astype(int)).groupby('channel').agg(orders=('order_id', 'size'), avg_hour=('h', 'mean'), cancelled=('c', 'sum')).reset_index().sort_values('orders', ascending=False)
ch['avg_hour'] = ch.avg_hour.round(1)
show('01_by_channel', ch)
val('01_dupes', int((orders_raw.order_id.value_counts() > 1).sum()))

# ------------------------------------------------------------------ 02 delta lake
hist = []
t = orders_raw.drop_duplicates('order_id').copy(); hist.append((0, 'CREATE OR REPLACE TABLE AS SELECT', f'numOutputRows: {len(t)}'))
val('02_v0_rows', len(t))
assert UPDATE_ORDER_ID in set(t.order_id) and UPDATE_ORDER_ID not in set(updates.order_id)
n = int((t.order_id == UPDATE_ORDER_ID).sum()); t.loc[t.order_id == UPDATE_ORDER_ID, 'status'] = 'cancelled'; hist.append((1, 'UPDATE', f'numUpdatedRows: {n}'))
v0 = orders_raw.drop_duplicates('order_id')
m = t.order_id.isin(updates.order_id); nu = int(m.sum())
t = pd.concat([t[~m], updates]); ni = len(updates) - nu
hist.append((2, 'MERGE', f'numTargetRowsUpdated: {nu}, numTargetRowsInserted: {ni}'))
val('02_merge', dict(updated=nu, inserted=ni))
dm = (t.status == 'cancelled') & (t.order_ts < '2026-02-01'); nd = int(dm.sum()); t = t[~dm]
hist.append((3, 'DELETE', f'numDeletedRows: {nd}'))
show('02_history', pd.DataFrame(sorted(hist, reverse=True), columns=['version', 'operation', 'operationMetrics']))
show('02_timetravel', pd.DataFrame([('v0', len(v0), int((v0.status == 'returned').sum())), ('now', len(t), int((t.status == 'returned').sum()))], columns=['version', 'orders', 'returned']))

# ------------------------------------------------------------------ 03 medallion
so = orders_raw.drop_duplicates('order_id'); so = so[so.customer_id.notna()].copy(); so['order_date'] = so.order_ts.dt.normalize()
bad = items_raw[(items_raw.quantity <= 0) | (items_raw.unit_price <= 0)]
si = items_raw[(items_raw.quantity > 0) & (items_raw.unit_price > 0)].merge(products[['product_id', 'product_name', 'category']], on='product_id')
si['amount'] = si.quantity * si.unit_price
show('03_counts', pd.DataFrame([('bronze_orders', len(orders_raw)), ('silver_orders', len(so)), ('bronze_order_items', len(items_raw)), ('silver_order_items', len(si)), ('quarantine_order_items', len(bad))], columns=['table_name', 'row_count']))
comp = so[so.status == 'completed']; lines = comp.merge(si, on='order_id')
gd = lines.groupby(['order_date', 'category']).agg(revenue_yen=('amount', 'sum'), orders=('order_id', 'nunique')).reset_index()
val('03_gold_daily_rows', len(gd))
fav = lines.groupby(['customer_id', 'category']).amount.sum().reset_index().sort_values(['customer_id', 'amount', 'category'], ascending=[True, False, True]).drop_duplicates('customer_id')[['customer_id', 'category']].rename(columns={'category': 'favourite_category'})
c360 = lines.groupby('customer_id').agg(orders=('order_id', 'nunique'), lifetime_value_yen=('amount', 'sum'), first_order=('order_date', 'min'), last_order=('order_date', 'max')).reset_index().merge(fav, on='customer_id').merge(customers[['customer_id', 'city', 'segment']], on='customer_id')
val('03_c360_rows', len(c360))
show('03_c360_top', c360.sort_values('lifetime_value_yen', ascending=False).head(5).assign(first_order=lambda d: d.first_order.dt.strftime('%Y-%m-%d'), last_order=lambda d: d.last_order.dt.strftime('%Y-%m-%d')))

# ------------------------------------------------------------------ 04 streaming
ev = {}
for day in ['2026-07-01', '2026-07-02', '2026-07-03']:
    ev[day] = pd.read_json(f'{DATA}/events_all/events_{day}.json', lines=True)
val('04_batch1', len(ev['2026-07-01']) + len(ev['2026-07-02'])); val('04_batch2', len(ev['2026-07-03']))
e = pd.concat(ev.values()); e['day'] = pd.to_datetime(e.event_ts).dt.strftime('%Y-%m-%d')
fn = e.groupby('day').apply(lambda g: pd.Series(dict(views=(g.event_type == 'page_view').sum(), carts=(g.event_type == 'add_to_cart').sum(), purchases=(g.event_type == 'purchase').sum())), include_groups=False).reset_index()
fn['conversion_pct'] = (100 * fn.purchases / fn.views).round(1)
show('04_funnel', fn)
val('04_dropped_no_customer', int(e.customer_id.isna().sum()))
val('04_silver_rows', int(e.customer_id.notna().sum()))

# ------------------------------------------------------------------ 05 SQL (SQLite stands in for Databricks SQL)
db = sqlite3.connect(':memory:')
so2 = so.assign(order_date=so.order_date.dt.strftime('%Y-%m-%d'), order_ts=so.order_ts.dt.strftime('%Y-%m-%d %H:%M:%S'))
so2.to_sql('silver_orders', db, index=False); si.to_sql('silver_order_items', db, index=False)
customers.assign(signup_date=customers.signup_date.dt.strftime('%Y-%m-%d')).to_sql('silver_customers', db, index=False)
gd.assign(order_date=gd.order_date.dt.strftime('%Y-%m-%d')).to_sql('gold_daily_sales', db, index=False)
c360.assign(first_order=c360.first_order.dt.strftime('%Y-%m-%d'), last_order=c360.last_order.dt.strftime('%Y-%m-%d')).to_sql('gold_customer_360', db, index=False)
q = lambda s: pd.read_sql(s, db)
show('05_q1', q("""SELECT i.product_name, i.category, SUM(i.quantity) AS units, SUM(i.amount) AS revenue_yen
FROM silver_order_items i JOIN silver_orders o USING (order_id) WHERE o.status='completed'
GROUP BY 1,2 ORDER BY revenue_yen DESC LIMIT 5"""))
show('05_q2', q("""WITH monthly AS (SELECT substr(order_date,1,7) AS month, SUM(revenue_yen) AS revenue_yen FROM gold_daily_sales GROUP BY 1)
SELECT month, revenue_yen, ROUND(100.0*(revenue_yen*1.0/LAG(revenue_yen) OVER (ORDER BY month)-1),1) AS mom_growth_pct FROM monthly ORDER BY month"""))
show('05_q3', q("""SELECT c.city, ROUND(SUM(CASE WHEN o.channel='app' THEN i.amount END)/1e6,2) AS app_m_yen,
ROUND(SUM(CASE WHEN o.channel='web' THEN i.amount END)/1e6,2) AS web_m_yen, ROUND(SUM(i.amount)/1e6,2) AS total_m_yen
FROM silver_orders o JOIN silver_order_items i USING (order_id) JOIN silver_customers c USING (customer_id)
WHERE o.status='completed' GROUP BY c.city ORDER BY total_m_yen DESC"""))
show('05_q4', q("""WITH firsts AS (SELECT customer_id, substr(MIN(order_date),1,7) AS cohort FROM silver_orders WHERE status='completed' GROUP BY customer_id),
activity AS (SELECT DISTINCT o.customer_id, f.cohort,
  (CAST(substr(o.order_date,1,4) AS INT)*12+CAST(substr(o.order_date,6,2) AS INT))-(CAST(substr(f.cohort,1,4) AS INT)*12+CAST(substr(f.cohort,6,2) AS INT)) AS month_n
  FROM silver_orders o JOIN firsts f USING (customer_id) WHERE o.status='completed')
SELECT cohort, SUM(month_n=0) AS customers, ROUND(100.0*SUM(month_n=1)/SUM(month_n=0)) AS m1_pct, ROUND(100.0*SUM(month_n=2)/SUM(month_n=0)) AS m2_pct,
ROUND(100.0*SUM(month_n=3)/SUM(month_n=0)) AS m3_pct FROM activity GROUP BY cohort ORDER BY cohort"""))
show('05_q5', q("""WITH rfm AS (SELECT customer_id, CAST(julianday('2026-06-30')-julianday(last_order) AS INT) AS recency_days, orders AS frequency, lifetime_value_yen AS monetary FROM gold_customer_360),
scored AS (SELECT *, NTILE(4) OVER (ORDER BY recency_days DESC, customer_id) AS r, NTILE(4) OVER (ORDER BY frequency, customer_id) AS f, NTILE(4) OVER (ORDER BY monetary, customer_id) AS m FROM rfm)
SELECT CASE WHEN r=4 AND f>=3 THEN 'Champions' WHEN r>=3 AND f>=2 THEN 'Loyal' WHEN r<=2 AND f>=3 THEN 'At risk' WHEN r=1 THEN 'Hibernating' ELSE 'Needs attention' END AS segment,
COUNT(*) AS customers, ROUND(AVG(monetary)) AS avg_value_yen, ROUND(AVG(recency_days)) AS avg_recency_days FROM scored GROUP BY 1 ORDER BY avg_value_yen DESC"""))
# Genie example question: which city grew most from Q1 to Q2?
show('05_genie', q("""WITH c AS (SELECT c.city, SUM(CASE WHEN o.order_date < '2026-04-01' THEN i.amount END) AS q1, SUM(CASE WHEN o.order_date >= '2026-04-01' THEN i.amount END) AS q2
FROM silver_orders o JOIN silver_order_items i USING (order_id) JOIN silver_customers c USING (customer_id) WHERE o.status='completed' GROUP BY c.city)
SELECT city, ROUND(q1/1e6,2) AS q1_m_yen, ROUND(q2/1e6,2) AS q2_m_yen, ROUND(100.0*(q2*1.0/q1-1),1) AS growth_pct FROM c ORDER BY growth_pct DESC LIMIT 3"""))

# Dashboard KPIs (first half of 2026)
kp = dict(revenue_m_yen=float(round(lines.amount.sum() / 1e6, 1)), completed_orders=int(comp.order_id.nunique()), active_customers=int(comp.customer_id.nunique()),
          avg_order_value_yen=int(round(lines.amount.sum() / comp.order_id.nunique())))
val('05_kpis', kp)

# ------------------------------------------------------------------ 06 governance
def mask(e): return e[:2] + '***@' + e.split('@')[1]
show('06_masked', customers.sort_values('customer_id').head(4)[['customer_id', 'first_name', 'email', 'city']].assign(email=lambda d: d.email.map(mask)))

# ------------------------------------------------------------------ 07 ML
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import roc_auc_score, precision_score, recall_score
CUT = pd.Timestamp('2026-05-01')
cl = comp.merge(si, on='order_id')
b = cl[cl.order_date < CUT]
came = set(comp[comp.order_date >= CUT].customer_id)
f = b.groupby('customer_id').apply(lambda g: pd.Series(dict(
    recency_days=(CUT - g.order_date.max()).days, orders=g.order_id.nunique(), spend_yen=g.amount.sum(), categories=g.category.nunique(),
    app_share=(g.channel == 'app').mean(), orders_last_30d=g[g.order_date >= '2026-04-01'].order_id.nunique())), include_groups=False).reset_index()
f = f.merge(customers[['customer_id', 'segment', 'signup_date']], on='customer_id')
f['tenure_days'] = (CUT - f.signup_date).dt.days; f['is_premium'] = (f.segment == 'Premium').astype(int)
f['churned'] = (~f.customer_id.isin(came)).astype(int); f = f.drop(columns=['segment', 'signup_date'])
show('07_label_counts', f.groupby('churned').size().rename('count').reset_index())
FEATS = ['recency_days', 'orders', 'spend_yen', 'categories', 'app_share', 'orders_last_30d', 'tenure_days', 'is_premium']
Xtr, Xte, ytr, yte = train_test_split(f[FEATS], f.churned, test_size=0.25, random_state=42, stratify=f.churned)
models = {'logistic_regression': make_pipeline(StandardScaler(), LogisticRegression(max_iter=1000)),
          'random_forest': RandomForestClassifier(n_estimators=300, min_samples_leaf=5, random_state=42)}
mres = []
for name, mdl in models.items():
    mdl.fit(Xtr, ytr); p = mdl.predict_proba(Xte)[:, 1]
    mres.append((name, round(roc_auc_score(yte, p), 3), round(precision_score(yte, p > 0.5), 2), round(recall_score(yte, p > 0.5), 2)))
show('07_metrics', pd.DataFrame(mres, columns=['model', 'test_auc', 'test_precision', 'test_recall']))
best = max(mres, key=lambda r: r[1])[0]; val('07_best', best)
champ = models[best]; f['churn_risk'] = champ.predict_proba(f[FEATS])[:, 1]
if best == 'random_forest':
    imp = pd.DataFrame(dict(feature=FEATS, importance=champ.feature_importances_)).sort_values('importance', ascending=False)
    imp['importance'] = imp.importance.round(3); show('07_importance', imp)
ar = f.merge(customers[['customer_id', 'first_name', 'city']], on='customer_id').sort_values('churn_risk', ascending=False)
ar = ar[ar.spend_yen > 30000].head(5)[['customer_id', 'recency_days', 'orders', 'spend_yen', 'churn_risk', 'first_name', 'city']]
ar['churn_risk'] = ar.churn_risk.round(3)
for c in ['recency_days', 'orders', 'spend_yen']: ar[c] = ar[c].astype(int)
show('07_at_risk', ar)
val('07_n_customers', len(f))

row = f[f.customer_id == 1340][FEATS].iloc[0]
val('07_req', {k: (float(row[k]) if k == 'app_share' else int(row[k])) for k in FEATS})
val('07_req_pred', int(champ.predict(f[f.customer_id == 1340][FEATS])[0]))
json.dump(RES, open(os.path.join(OUT, 'results.json'), 'w'), indent=1, default=str)
print('\nwritten', OUT)
