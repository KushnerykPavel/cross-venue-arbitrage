"""Raw Binance BTCUSDT capture for the book-quality investigation.

Writes one JSONL line per frame: {"src", "recv_ns" (wall), "mono_ns", "raw"}.
Sources: depth10@100ms on /public (what the adapter uses), depth10@100ms on the
legacy /stream endpoint, bookTicker on /public, aggTrade on /market, REST depth
snapshots every second.
"""
import asyncio, json, sys, time, urllib.request
import websockets

SYM = "btcusdt"
WS = {
    "depth_public": f"wss://fstream.binance.com/public/stream?streams={SYM}@depth10@100ms",
    "depth_legacy": f"wss://fstream.binance.com/stream?streams={SYM}@depth10@100ms",
    "bookticker": f"wss://fstream.binance.com/public/stream?streams={SYM}@bookTicker",
    "aggtrade": f"wss://fstream.binance.com/market/stream?streams={SYM}@aggTrade",
}
REST = "https://fapi.binance.com/fapi/v1/depth?symbol=BTCUSDT&limit=10"


def line(src, raw):
    return json.dumps({"src": src, "recv_ns": time.time_ns(), "mono_ns": time.monotonic_ns(), "raw": raw})


async def ws_loop(src, url, out, stop):
    async with websockets.connect(url, max_size=2**22, ping_interval=None) as ws:
        while time.monotonic() < stop:
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=5)
            except asyncio.TimeoutError:
                continue
            out.write(line(src, msg) + "\n")


async def rest_loop(out, stop):
    while time.monotonic() < stop:
        t = time.monotonic()
        body = await asyncio.to_thread(lambda: urllib.request.urlopen(REST, timeout=5).read().decode())
        out.write(line("rest", body) + "\n")
        await asyncio.sleep(max(0, 1 - (time.monotonic() - t)))


async def main(seconds, path):
    stop = time.monotonic() + seconds
    with open(path, "w") as out:
        await asyncio.gather(*(ws_loop(s, u, out, stop) for s, u in WS.items()), rest_loop(out, stop))


if __name__ == "__main__":
    asyncio.run(main(float(sys.argv[1]), sys.argv[2]))
