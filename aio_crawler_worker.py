#!/usr/bin/env python3
import asyncio
import time
import random
import ssl
import logging
from typing import Optional, Tuple
from urllib.parse import urlparse

import aiohttp
import redis.asyncio as aioredis
import motor.motor_asyncio
from pymongo.errors import BulkWriteError

# ======== Optional: uvloop for ~10–20% boost ========
try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except Exception:
    pass
# ====================================================

# =============== CONFIG ===============
REDIS_URL       = 'redis://localhost:6379/0'
TASK_LIST       = 'crawler:tasks'
DONE_KEY        = f'{TASK_LIST}:enqueue_complete'

MONGO_URI       = 'mongodb://localhost:27017'
MONGO_DB_PREFIX = 'results_'
MONGO_SPLIT_THRESHOLD = 500_000

# 并发 & 网络（建议逐步调参观察 429/封禁）
CONCURRENCY      = 300
LIMIT_PER_HOST   = 6
CONNECT_LIMIT    = max(CONCURRENCY, 2 * CONCURRENCY)  # 连接池上限
TIMEOUT          = 10  # sock_read 超时，建议 12~15 区间

# Redis 批量弹出
BATCH_POP        = 200
BRPOP_TIMEOUT    = 5
IDLE_QUIT_AFTER  = 300

# Mongo 批量
BATCH_SIZE       = 200

# 日志/进度（按“尝试数 attempts”打印）
PRINT_EVERY      = 100_000

# 总尝试上限（达到才算“最终失败”）
MAX_RETRIES      = 5

# 不重试状态码（确定性失败）
NON_RETRY_STATUS = {400, 401, 403, 404, 410, 451}

# 轻量模式：不存 html，只存长度
LIGHT_MODE       = False

# 运行次序前缀，避免 _id 撞键（0/None 表示不用）
RUN_ID           = 0  # 例如：RUN_ID = int(time.time())

# UA / 头
SESSION_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
}

# 降噪开关（需要详细排障时可改为 False）
QUIET_SSL_LOGS  = True
# =====================================

# -------- 日志与异常降噪 --------
def _setup_quiet_logging():
    for name in ('asyncio', 'aiohttp', 'aiohttp.client', 'aiohttp.server', 'urllib3', 'charset_normalizer'):
        logging.getLogger(name).setLevel(logging.CRITICAL)

def _loop_exception_filter(loop: asyncio.AbstractEventLoop, context: dict):
    if not QUIET_SSL_LOGS:
        return loop.default_exception_handler(context)
    exc = context.get('exception')
    msg = context.get('message', '')
    if isinstance(exc, ssl.SSLError):
        s = str(exc)
        noisy_fragments = (
            'TLSV1_ALERT_PROTOCOL_VERSION',
            'WRONG_VERSION_NUMBER',
            'SSLV3_ALERT_HANDSHAKE_FAILURE',
            'SSL error in data received',
        )
        if any(x in s for x in noisy_fragments):
            return
    if 'SSL error in data received' in msg:
        return
    if isinstance(exc, (ConnectionResetError, BrokenPipeError, TimeoutError)):
        return
    loop.default_exception_handler(context)
# --------------------------------------

mongo = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI)

def get_db(task_id: int):
    db_index = task_id // MONGO_SPLIT_THRESHOLD
    return mongo[f"{MONGO_DB_PREFIX}{db_index}"]

# ---------- HTTP fetch（一次尝试，不做本地重试） ----------
async def fetch_once(session: aiohttp.ClientSession, url: str) -> Tuple[bool, Optional[int], object]:
    """
    返回: (ok:bool, status:Optional[int], payload)
      - ok=True: status<400 且非伪404；payload=html 或 {'html_len':...}
      - ok=False: payload='' 或 {}
    """
    try:
        async with session.get(url, timeout=TIMEOUT, ssl=False) as resp:
            status = resp.status
            raw = await resp.read()
            if status < 400 and (b'404 Not Found' not in raw) and (b'<title>404' not in raw):
                if LIGHT_MODE:
                    return True, status, {'html_len': len(raw)}
                enc = resp.charset or 'utf-8'
                try:
                    html = raw.decode(enc, 'ignore')
                except LookupError:
                    html = raw.decode('utf-8', 'ignore')
                return True, status, html
            else:
                return False, status, ''
    except Exception:
        return False, None, ''

# ========== 队列元素尝试次数编码（兼容旧数据） ==========
def parse_entry(entry_bytes: bytes):
    """
    支持两种格式：
      1) 'idx url'            -> attempt = 1
      2) 'idx#attempt url'    -> attempt = int(attempt)
    返回: (base_idx:int, attempt:int, url:str)
    """
    s = entry_bytes.decode()
    head, url = s.split(' ', 1)
    if '#' in head:
        base_idx_str, attempt_str = head.split('#', 1)
        base_idx = int(base_idx_str)
        attempt = int(attempt_str)
    else:
        base_idx = int(head)
        attempt = 1
    return base_idx, attempt, url

def make_entry(base_idx: int, attempt: int, url: str) -> bytes:
    return f"{base_idx}#{attempt} {url}".encode()
# =======================================================

def _print_progress_if_needed(stats: dict, now: float):
    if PRINT_EVERY <= 0:
        return
    while stats['attempts'] >= stats['next_attempt_milestone']:
        elapsed = now - stats['start_time']
        att_speed = (stats['attempts'] / elapsed) if elapsed > 0 else 0.0
        print(
            f"PROGRESS_{PRINT_EVERY//1000}K: "
            f"尝试={stats['attempts']:,} | 用时={elapsed:.1f}s | "
            f"速度={att_speed:.1f} attempts/s | "
            f"最终成功URL={stats['ok']:,} | 最终失败URL={stats['fail']:,}"
        )
        stats['next_attempt_milestone'] += PRINT_EVERY

async def db_writer(queue: asyncio.Queue, first_persist_flag: dict, stats: dict):
    pages, fails = [], []
    while True:
        item = await queue.get()
        if item is None:
            break

        if item['success']:
            pages.append(item['record'])
            if len(pages) >= BATCH_SIZE:
                db = get_db(pages[0]['_id'] if '_id' in pages[0] else pages[0]['task_id'])
                try:
                    res = await db.pages.insert_many(pages, ordered=False)
                    n = len(res.inserted_ids)
                    stats['written_ok'] += n
                    stats['written_total'] += n
                    if not first_persist_flag['done']:
                        print("PERSIST_READY: first batch written to Mongo (pages).")
                        first_persist_flag['done'] = True
                    _print_progress_if_needed(stats, time.perf_counter())
                except BulkWriteError as e:
                    n = e.details.get('nInserted', 0)
                    stats['written_ok'] += n
                    stats['written_total'] += n
                except Exception:
                    pass
                pages.clear()
        else:
            fails.append(item['record'])
            if len(fails) >= BATCH_SIZE:
                db = get_db(fails[0]['task_id'])
                try:
                    res = await db.failed_tasks.insert_many(fails, ordered=False)
                    n = len(res.inserted_ids)
                    stats['written_fail'] += n
                    stats['written_total'] += n
                    if not first_persist_flag['done']:
                        print("PERSIST_READY: first batch written to Mongo (failed_tasks).")
                        first_persist_flag['done'] = True
                    _print_progress_if_needed(stats, time.perf_counter())
                except BulkWriteError as e:
                    n = e.details.get('nInserted', 0)
                    stats['written_fail'] += n
                    stats['written_total'] += n
                except Exception:
                    pass
                fails.clear()

        queue.task_done()

    # flush
    if pages:
        db = get_db(pages[0]['_id'] if '_id' in pages[0] else pages[0]['task_id'])
        try:
            res = await db.pages.insert_many(pages, ordered=False)
            n = len(res.inserted_ids)
            stats['written_ok'] += n
            stats['written_total'] += n
            if not first_persist_flag['done']:
                print("PERSIST_READY: first batch written to Mongo (pages).")
                first_persist_flag['done'] = True
            _print_progress_if_needed(stats, time.perf_counter())
        except BulkWriteError as e:
            n = e.details.get('nInserted', 0)
            stats['written_ok'] += n
            stats['written_total'] += n
        except Exception:
            pass
    if fails:
        db = get_db(fails[0]['task_id'])
        try:
            res = await db.failed_tasks.insert_many(fails, ordered=False)
            n = len(res.inserted_ids)
            stats['written_fail'] += n
            stats['written_total'] += n
            if not first_persist_flag['done']:
                print("PERSIST_READY: first batch written to Mongo (failed_tasks).")
                first_persist_flag['done'] = True
            _print_progress_if_needed(stats, time.perf_counter())
        except BulkWriteError as e:
            n = e.details.get('nInserted', 0)
            stats['written_fail'] += n
            stats['written_total'] += n
        except Exception:
            pass

async def blmpop_batch(redis_conn, key: str, count: int, timeout: int):
    """
    优先使用 Redis 7 的 BLMPOP（阻塞、批量）。
    若不支持，则退化为：BRPOP 1条 + LPOP(count-1)。
    返回: list[bytes]
    """
    try:
        res = await redis_conn.execute_command("BLMPOP", timeout, "1", key, "COUNT", count, "RIGHT")
        if res and isinstance(res, (list, tuple)) and len(res) == 2 and isinstance(res[1], (list, tuple)):
            return list(res[1])
    except Exception:
        pass

    out = []
    popped = await redis_conn.brpop(key, timeout=timeout)
    if popped:
        _, first = popped
        out.append(first)
        if count > 1:
            try:
                more = await redis_conn.execute_command("LPOP", key, str(count - 1))
                if more:
                    if isinstance(more, (list, tuple)):
                        out.extend(more)
                    else:
                        out.append(more)
            except Exception:
                for _ in range(count - 1):
                    m = await redis_conn.lpop(key)
                    if not m:
                        break
                    out.append(m)
    return out

def should_retry(status: Optional[int]) -> bool:
    if status is None:
        return True
    if status in NON_RETRY_STATUS:
        return False
    if status in (408, 425, 429):
        return True
    if 500 <= status <= 504:
        return True
    if 521 <= status <= 526:
        return True
    # 其它 4xx/边角情况：默认不重试，避免慢失败拖占用
    return False

async def worker(name: str, redis_conn, session: aiohttp.ClientSession,
                 q_out: asyncio.Queue, stats: dict, first_consume_flag: dict,
                 stop_event: asyncio.Event):
    last_got = time.perf_counter()
    while not stop_event.is_set():
        batch = await blmpop_batch(redis_conn, TASK_LIST, BATCH_POP, BRPOP_TIMEOUT)
        if batch:
            if not first_consume_flag['done']:
                print("CONSUME_READY: first batch popped from Redis.")
                first_consume_flag['done'] = True
            last_got = time.perf_counter()
            stats['in_flight'] += len(batch)
        else:
            done_flag = await redis_conn.get(DONE_KEY)
            qlen = await redis_conn.llen(TASK_LIST)
            idle = time.perf_counter() - last_got
            if (done_flag and qlen == 0 and stats['in_flight'] == 0) or (idle >= IDLE_QUIT_AFTER):
                break
            await asyncio.sleep(0)
            continue

        for entry in batch:
            try:
                base_idx, attempt, url = parse_entry(entry)
            except Exception:
                stats['in_flight'] -= 1
                continue

            idx = f"{RUN_ID}-{base_idx}" if RUN_ID else base_idx
            ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

            ok, status, payload = await fetch_once(session, url)

            stats['attempts'] += 1
            _print_progress_if_needed(stats, time.perf_counter())

            if ok:
                if LIGHT_MODE:
                    record = {
                        '_id': idx,
                        'url': url,
                        'host': urlparse(url).netloc,
                        'http_status_code': status,
                        'html_len': payload.get('html_len', 0) if isinstance(payload, dict) else 0,
                        'crawl_timestamp': ts,
                    }
                else:
                    record = {
                        '_id': idx,
                        'url': url,
                        'host': urlparse(url).netloc,
                        'http_status_code': status,
                        'html': payload,
                        'crawl_timestamp': ts,
                    }
                await q_out.put({'success': True, 'record': record})
                stats['ok'] += 1
            else:
                if attempt < MAX_RETRIES and should_retry(status):
                    # 固定用左端 LPUSH（O(1)）回插
                    new_entry = make_entry(base_idx, attempt + 1, url)
                    try:
                        await redis_conn.lpush(TASK_LIST, new_entry)
                    except Exception:
                        # 兜底重试一次
                        await redis_conn.lpush(TASK_LIST, new_entry)
                else:
                    record = {
                        'task_id': idx,
                        'url': url,
                        'host': urlparse(url).netloc,
                        'status': status if status is not None else 'ERR',
                        'failed_at': ts,
                        'rounds': attempt,
                    }
                    await q_out.put({'success': False, 'record': record})
                    stats['fail'] += 1

            stats['done'] += 1
            stats['in_flight'] -= 1

async def main():
    # 更细的 timeout（连接更短，读为 TIMEOUT）
    timeout = aiohttp.ClientTimeout(
        total=None,
        connect=5,
        sock_connect=5,
        sock_read=TIMEOUT,
    )

    _setup_quiet_logging()

    redis_conn = aioredis.Redis.from_url(REDIS_URL, decode_responses=False)

    print(f"WORKERS_READY: redis={REDIS_URL}, mongo={MONGO_URI}, "
          f"concurrency={CONCURRENCY}, limit_per_host={LIMIT_PER_HOST}, "
          f"max_retries={MAX_RETRIES}, batch_pop={BATCH_POP}, light_mode={LIGHT_MODE}, run_id={RUN_ID}")

    q_out = asyncio.Queue()
    first_persist_flag = {'done': False}

    stats = {
        'done': 0, 'ok': 0, 'fail': 0,
        'written_ok': 0, 'written_fail': 0, 'written_total': 0,
        'attempts': 0,
        'in_flight': 0,
        'start_time': time.perf_counter(),
        'next_attempt_milestone': PRINT_EVERY if PRINT_EVERY > 0 else 1 << 60,
        'next_write_milestone': 1 << 60,
    }

    loop = asyncio.get_running_loop()
    loop.set_exception_handler(_loop_exception_filter)

    stop_event = asyncio.Event()
    db_task = asyncio.create_task(db_writer(q_out, first_persist_flag, stats))

    connector = aiohttp.TCPConnector(
        limit=CONNECT_LIMIT,
        limit_per_host=LIMIT_PER_HOST,
        ssl=False,
        use_dns_cache=True,
        ttl_dns_cache=300,
        keepalive_timeout=60,
    )

    async with aiohttp.ClientSession(connector=connector, timeout=timeout, headers=SESSION_HEADERS) as session:
        first_consume_flag = {'done': False}
        workers = [
            asyncio.create_task(
                worker(f"w{i}", redis_conn, session, q_out, stats, first_consume_flag, stop_event)
            )
            for i in range(CONCURRENCY)
        ]

        # 监控：只有 DONE + 队列空 + 在途为 0 才收队
        try:
            while True:
                await asyncio.sleep(1)
                done_flag = await redis_conn.get(DONE_KEY)
                qlen = await redis_conn.llen(TASK_LIST)
                if done_flag and qlen == 0 and stats['in_flight'] == 0:
                    stop_event.set()
                    break
        except asyncio.CancelledError:
            pass

        await asyncio.gather(*workers, return_exceptions=True)

    # 通知写库协程 flush 并退出
    await q_out.put(None)
    await db_task

    remaining     = await redis_conn.llen(TASK_LIST)
    total_elapsed = time.perf_counter() - stats['start_time']
    att_speed     = (stats['attempts'] / total_elapsed) if total_elapsed > 0 else 0.0

    print(
        f"WORKERS_STOPPED: "
        f"尝试总数={stats['attempts']:,} | "
        f"最终成功URL={stats['ok']:,} | 最终失败URL={stats['fail']:,} | "
        f"队列剩余={remaining:,} | 用时={total_elapsed:.1f}s | "
        f"速度={att_speed:.1f} attempts/s | "
        f"batch_pop={BATCH_POP}, limit_per_host={LIMIT_PER_HOST}, light_mode={LIGHT_MODE}"
    )

if __name__ == '__main__':
    asyncio.run(main())

