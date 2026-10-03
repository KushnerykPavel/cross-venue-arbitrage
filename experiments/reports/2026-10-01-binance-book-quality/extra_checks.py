"""Extra live-capture checks: /public vs legacy depth10, aggTrade hold-back share, bookTicker mid reversion."""
import json, sys
from collections import defaultdict

pub, leg, bt, tr = {}, {}, [], []
for l in open(sys.argv[1]):
    d = json.loads(l)
    if d["src"] == "rest":
        continue
    r = json.loads(d["raw"])["data"]
    if d["src"] == "depth_public":
        pub[r["u"]] = (r["b"], r["a"])
    elif d["src"] == "depth_legacy":
        leg[r["u"]] = (r["b"], r["a"])
    elif d["src"] == "bookticker":
        bt.append((r["T"], r["u"], r["b"], r["a"]))
    elif d["src"] == "aggtrade":
        tr.append((r["T"], r["E"], r["f"], r["l"]))
common = set(pub) & set(leg)
print(f"public {len(pub)} legacy {len(leg)} common u {len(common)}; full 10-level book differs: {sum(pub[u] != leg[u] for u in common)}")

held = [t for t in tr if t[1] - t[0] >= 100]
per_T = defaultdict(int)
for t in tr:
    per_T[t[0]] += 1
single = [t for t in tr if per_T[t[0]] == 1]
multi = [t for t in tr if per_T[t[0]] > 1]
share = lambda xs: sum(1 for t in xs if t[1] - t[0] >= 100) / len(xs)
print(f"aggTrade E-T >= 100 ms: {share(tr):.1%} of {len(tr)}; single-agg matches {share(single):.1%} (n={len(single)}), multi-agg matches {share(multi):.1%} (n={len(multi)})")

# bookTicker mid changes that revert to the previous mid within 100 ms (exchange T)
bt.sort(key=lambda x: x[1])
mids = [(T, float(b) + float(a)) for T, u, b, a in bt]
changes = [(mids[i - 1][1], mids[i][0], mids[i][1], i) for i in range(1, len(mids)) if mids[i][1] != mids[i - 1][1]]
rev = 0
for prev, T, new, i in changes:
    j = i + 1
    while j < len(mids) and mids[j][0] - T <= 100:
        if mids[j][1] == prev:
            rev += 1
            break
        j += 1
print(f"bookTicker mid changes {len(changes)}; reverted to previous mid within 100 ms: {rev/len(changes):.1%}")
