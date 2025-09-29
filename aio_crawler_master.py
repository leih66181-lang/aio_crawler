#!/usr/bin/env python3
from __future__ import annotations
import asyncio
import csv
import random
import time
import math
from collections import defaultdict, deque
from urllib.parse import urlparse

import redis.asyncio as aioredis

# =============== CONFIG ===============
CSV_FILE = 'google_url.csv'            # 全量 URL CSV，首行为表头
REDIS_URL = 'redis://localhost:6379/0'
TASK_LIST = 'crawler:tasks'
DONE_KEY  = f'{TASK_LIST}:enqueue_complete'

# 控制本次要推入多少条（0/None 表示不限制）
TEST_LIMIT = 100000

# 入队随机化与批量（可按机器资源调整）
CHUNK_SIZE     = 100_000               # 每次读入并处理的行数（避免一次性装内存）
PIPELINE_BATCH = 10_000                # 每批 LPUSH 条数（一次命令多值）
PRINT_EVERY    = 100_000               # 入队进度打印频率
HOST_TAKE_PER_ROUND = 1                # 每轮从选中的 host 桶取多少条
# ======================================

def _host_from_entry(entry: str) -> str:
    """entry 形如: 'idx url' -> 提取标准化 host。解析失败则返回空串。"""
    try:
        url = entry.split(' ', 1)[1]
        host = (urlparse(url).hostname or '').lower()
        if host.startswith('www.'):
            host = host[4:]
        return host
    except Exception:
        return ''

def _interleave_by_host_weighted(rows: list[str]) -> list[str]:
    """
    先按 host 分桶（桶内保持原顺序），
    再按 log(剩余+1) 权重在桶之间随机抽取，直到全部取完。
    返回交错后的 entry 顺序列表。
    """
    buckets: dict[str, deque[str]] = defaultdict(deque)
    for e in rows:
        buckets[_host_from_entry(e)].append(e)

    hosts = [h for h, dq in buckets.items() if dq]
    remaining = {h: len(buckets[h]) for h in hosts}
    out: list[str] = []

    while hosts:
        weights = [math.log(remaining[h] + 1) for h in hosts]
        i = random.choices(range(len(hosts)), weights=weights, k=1)[0]
        h = hosts[i]

        take = min(HOST_TAKE_PER_ROUND, remaining[h])
        for _ in range(take):
            out.append(buckets[h].popleft())
        remaining[h] -= take

        if remaining[h] <= 0:
            buckets.pop(h, None)
            remaining.pop(h, None)
            hosts[i] = hosts[-1]
            hosts.pop()

    return out

async def push_chunk(redis_conn, rows: list[str]) -> int:
    """
    rows: list[str]，每个元素是 entry: f"{idx} {url}"
    先主机交错 -> 再按批次“一条 LPUSH 多值”
    """
    ordered = _interleave_by_host_weighted(rows)
    total = 0
    for i in range(0, len(ordered), PIPELINE_BATCH):
        batch = ordered[i:i + PIPELINE_BATCH]
        # 关键：一条 LPUSH 搞定一批
        await redis_conn.lpush(TASK_LIST, *batch)
        total += len(batch)
    return total

async def main():
    redis_conn = aioredis.Redis.from_url(REDIS_URL, decode_responses=False)

    # 清空旧队列（只影响本次入队，不动抓取/Mongo）
    await redis_conn.delete(TASK_LIST)
    await redis_conn.delete(DONE_KEY)

    pushed = 0
    chunk: list[str] = []
    t0 = time.monotonic()

    with open(CSV_FILE, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader, None)  # 跳过表头

        for idx, row in enumerate(reader):
            if TEST_LIMIT and idx >= TEST_LIMIT:
                break
            url = row[0] if len(row) == 1 else row[1]
            entry = f"{idx} {url}"
            chunk.append(entry)

            if len(chunk) >= CHUNK_SIZE:
                cnt = await push_chunk(redis_conn, chunk)
                pushed += cnt
                chunk.clear()
                if PRINT_EVERY and pushed % PRINT_EVERY == 0:
                    print(f"ENQUEUE_PROGRESS: {pushed} pushed")

    if chunk:
        cnt = await push_chunk(redis_conn, chunk)
        pushed += cnt
        chunk.clear()

    # 设置“入队完成”标志键
    await redis_conn.set(DONE_KEY, "1")

    qlen = await redis_conn.llen(TASK_LIST)
    t1 = time.monotonic()

    print(f"\nENQUEUE_COMPLETE: pushed={pushed}, queue_len={qlen}")
    print("START_WORKERS: 所有 URL 已按 host 分桶并随机交错入队。现在可以启动从机/worker 进程抓取。\n")

    if qlen == pushed and pushed > 0:
        print(f"RUN_STATUS: SUCCESS_ENQUEUE | pushed={pushed}, elapsed={t1 - t0:.2f}s\n")
    else:
        print(f"RUN_STATUS: INCOMPLETE_ENQUEUE | pushed={pushed}, queue_len={qlen}\n")

if __name__ == '__main__':
    asyncio.run(main())




