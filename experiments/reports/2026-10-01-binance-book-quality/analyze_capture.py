"""Strict-ordering checks: trades vs book before/after on Binance transaction time T,
trade-price-change decomposition, and detection delay of depth10 vs bookTicker vs trades."""
import bisect, json, statistics, sys
from collections import Counter

TICK = 0.1
bt, dp, tr, rest = [], [], [], []
for l in open(sys.argv[1]):
    d = json.loads(l)
    r = json.loads(d["raw"])
    r = r.get("data", r)
    t = d["recv_ns"]
    if d["src"] == "bookticker":
        bt.append((r["T"], r["u"], float(r["b"]), float(r["a"]), t))
    elif d["src"] == "depth_public":
        dp.append((r["T"], r["u"], float(r["b"][0][0]), float(r["a"][0][0]), t))
    elif d["src"] == "aggtrade":
        tr.append((r["T"], float(r["p"]), r["m"], r["a"], t, r["f"], r["l"]))
    elif d["src"] == "rest":
        rest.append((r["T"], float(r["bids"][0][0]), float(r["asks"][0][0])))
bt.sort(key=lambda x: x[1]); dp.sort(key=lambda x: x[1]); tr.sort(key=lambda x: x[3])
dur = (tr[-1][4] - tr[0][4]) / 1e9
q = lambda v, p: sorted(v)[int(p * (len(v) - 1))]
print(f"duration {dur:.0f}s bookTicker {len(bt)} depth10 {len(dp)} aggTrade {len(tr)} rest {len(rest)}")

# 1. depth10 top equals bookTicker at same update id?
bt_u = [x[1] for x in bt]
mism = 0
for T, u, b, a, _ in dp:
    i = bisect.bisect_right(bt_u, u) - 1
    if i >= 0 and (bt[i][2], bt[i][3]) != (b, a):
        mism += 1
print(f"\n[1] depth10 top != bookTicker at same-or-earlier u: {mism}/{len(dp)}")
# REST vs bookTicker on T
bt_T = [x[0] for x in bt]
rm = 0
for T, b, a in rest:
    i = bisect.bisect_right(bt_T, T) - 1
    rm += (bt[i][2], bt[i][3]) != (b, a)
print(f"    REST top != bookTicker as-of T: {rm}/{len(rest)}")

# 2. trades vs book strictly before / strictly after on T
cls = Counter()
for T, p, m, *_ in tr:
    i = bisect.bisect_left(bt_T, T) - 1          # last book with T_b < T
    j = bisect.bisect_right(bt_T, T)             # first book with T_b > T
    if i < 0 or j >= len(bt):
        continue
    pb, pa = bt[i][2], bt[i][3]
    nb, na = bt[j][2], bt[j][3]
    in_prev = pb <= p <= pa
    in_next = nb <= p <= na
    sweep = (m and p <= pb) or ((not m) and p >= pa)   # aggressor walks into its own side's book
    cls[("in_prev" if in_prev else "sweep_prev" if sweep else "other_prev",
         "in_next" if in_next else "out_next")] += 1
n = sum(cls.values())
print(f"\n[2] trades classified vs strictly-previous and strictly-next bookTicker (n={n}):")
for k, v in cls.most_common():
    print(f"    {k}: {v/n:.1%}")

# 3. why trade price changes >> mid changes
mid_changes_bt = sum(1 for x, y in zip(bt, bt[1:]) if (x[2], x[3]) != (y[2], y[3]) and x[2] + x[3] != y[2] + y[3])
mid_changes_dp = sum(1 for x, y in zip(dp, dp[1:]) if x[2] + x[3] != y[2] + y[3])
pc = [(x, y) for x, y in zip(tr, tr[1:]) if x[1] != y[1]]
same_T = sum(1 for x, y in pc if x[0] == y[0])
bounce = 0
for x, y in pc:
    if x[0] == y[0]:
        continue
    i = bisect.bisect_left(bt_T, y[0]) - 1
    if i >= 0 and {x[1], y[1]} <= {bt[i][2], bt[i][3]}:
        bounce += 1
print(f"\n[3] per second: bookTicker mid changes {mid_changes_bt/dur:.2f}, depth10 mid changes {mid_changes_dp/dur:.2f}, trade price changes {len(pc)/dur:.2f}")
print(f"    of trade price changes: within one match (same T, sweep levels) {same_T/len(pc):.0%}, bid<->ask bounce {bounce/len(pc):.0%}, other {(len(pc)-same_T-bounce)/len(pc):.0%}")
print(f"    distinct trade timestamps T per second {len(set(x[0] for x in tr))/dur:.2f}")

# 4. depth10 delay behind bookTicker for each bookTicker mid change (by exchange T and local recv)
dp_T = [x[0] for x in dp]
lag_T, lag_recv = [], []
for x, y in zip(bt, bt[1:]):
    if x[2] + x[3] == y[2] + y[3]:
        continue
    k = bisect.bisect_left(dp_T, y[0])  # first depth snapshot at/after change
    while k < len(dp) and dp[k][1] < y[1]:
        k += 1
    if k < len(dp):
        lag_T.append(dp[k][0] - y[0])
        lag_recv.append((dp[k][4] - y[4]) / 1e6)
print(f"\n[4] depth10 lag behind bookTicker per mid change (n={len(lag_T)}): exchange-T ms p50 {q(lag_T,.5)} p90 {q(lag_T,.9)} max {max(lag_T)};"
      f" local-recv ms p50 {q(lag_recv,.5):.1f} p90 {q(lag_recv,.9):.1f}")

# 5. receive lag per stream (local wall clock minus exchange T; includes clock offset)
print("\n[5] recv - exchange T, ms p50/p99 (includes clock offset, compare only relatively):")
for name, s, ti in [("bookTicker", bt, 4), ("depth10", dp, 4), ("aggTrade", tr, 4)]:
    v = [x[ti] / 1e6 - x[0] for x in s]
    print(f"    {name:10s} {q(v,.5):.1f} / {q(v,.99):.1f}")

# 6. local arrival order for the same match: aggTrade vs bookTicker carrying that T
first_bt_recv = {}
for x in bt:
    first_bt_recv.setdefault(x[0], x[4])
d = [(x[4] - first_bt_recv[x[0]]) / 1e6 for x in tr if x[0] in first_bt_recv]
print(f"\n[6] aggTrade recv - bookTicker recv for same T, ms (n={len(d)}): p10 {q(d,.1):.1f} p50 {q(d,.5):.1f} p90 {q(d,.9):.1f}")

# 7. 200 ms >= X bps move events: detection time per source
def events(series, X, window_ms=200, cool_ms=2000):
    """series: list of (T_ms, recv_ns, price). Returns T of first sample whose move vs window ago >= X bps."""
    out, last = [], -10**18
    Ts = [s[0] for s in series]
    for idx, (T, recv, p) in enumerate(series):
        i = bisect.bisect_right(Ts, T - window_ms) - 1
        if i < 0 or T - last < cool_ms:
            continue
        mv = (p / series[i][2] - 1) * 1e4
        if abs(mv) >= X:
            out.append((T, recv, 1 if mv > 0 else -1))
            last = T
    return out

for X in (2, 3, 5):
    s_bt = events([(x[0], x[4], (x[2] + x[3]) / 2) for x in bt], X)
    s_dp = events([(x[0], x[4], (x[2] + x[3]) / 2) for x in dp], X)
    s_tr = events([(x[0], x[4], x[1]) for x in tr], X)
    def match(ref, other):
        ds = []
        oT = [o[0] for o in other]
        for T, recv, sgn in ref:
            k = bisect.bisect_left(oT, T - 300)
            if k < len(other) and abs(other[k][0] - T) <= 300 and other[k][2] == sgn:
                ds.append((other[k][1] - recv) / 1e6)
        return ds
    m1, m2 = match(s_bt, s_dp), match(s_bt, s_tr)
    print(f"\n[7] X={X} bps / 200 ms: events bookTicker {len(s_bt)}, depth10 {len(s_dp)}, aggTrade-price {len(s_tr)}")
    if m1:
        print(f"    depth10 detection - bookTicker detection (local ms): n={len(m1)} p50 {statistics.median(m1):.1f} min {min(m1):.1f} max {max(m1):.1f}")
    if m2:
        print(f"    trade detection - bookTicker detection (local ms):   n={len(m2)} p50 {statistics.median(m2):.1f} min {min(m2):.1f} max {max(m2):.1f}")

# 8. server-side delay E - T per stream (exchange clock only, no local offset)
raw_ET = {"bookticker": [], "depth_public": [], "aggtrade": []}
for l in open(sys.argv[1]):
    d = json.loads(l)
    if d["src"] in raw_ET:
        r = json.loads(d["raw"])["data"]
        raw_ET[d["src"]].append(r["E"] - r["T"])
print("\n[8] E - T (ms, exchange clock) p50/p90/p99/max:")
for k, v in raw_ET.items():
    print(f"    {k:12s} {q(v,.5)} / {q(v,.9)} / {q(v,.99)} / {max(v)}")

# 9. depth10 lag behind bookTicker, conditioned on mid jump size
buckets = {"1 tick": [], "2-4 ticks": [], ">=5 ticks": []}
dp_u = [x[1] for x in dp]
for x, y in zip(bt, bt[1:]):
    jump = abs(round(((y[2] + y[3]) - (x[2] + x[3])) / 2 / TICK))
    if jump == 0:
        continue
    k = bisect.bisect_left(dp_u, y[1])
    if k >= len(dp):
        continue
    b = "1 tick" if jump <= 1 else "2-4 ticks" if jump <= 4 else ">=5 ticks"
    buckets[b].append((dp[k][4] - y[4]) / 1e6)
print("\n[9] depth10 local lag behind bookTicker mid change, by jump size (ms):")
for k, v in buckets.items():
    if v:
        print(f"    {k:10s} n={len(v):4d} p50 {q(v,.5):6.1f} p90 {q(v,.9):6.1f} max {max(v):6.1f}")

for X in (0.5, 1.0):
    s_bt = events([(x[0], x[4], (x[2] + x[3]) / 2) for x in bt], X)
    s_dp = events([(x[0], x[4], (x[2] + x[3]) / 2) for x in dp], X)
    s_tr = events([(x[0], x[4], x[1]) for x in tr], X)
    print(f"\n[7b] X={X} bps: events bookTicker {len(s_bt)}, depth10 {len(s_dp)}, trade {len(s_tr)} (indicative)")
    for name, other in (("depth10", s_dp), ("trade", s_tr)):
        oT = [o[0] for o in other]; ds = []
        for T, recv, sgn in s_bt:
            k = bisect.bisect_left(oT, T - 300)
            if k < len(other) and abs(other[k][0] - T) <= 300 and other[k][2] == sgn:
                ds.append((other[k][1] - recv) / 1e6)
        if ds:
            print(f"    {name} - bookTicker detection, local ms: n={len(ds)} p50 {statistics.median(ds):.1f} p10 {q(ds,.1):.1f} p90 {q(ds,.9):.1f}")
