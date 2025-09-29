# Async Crawler

这是一个基于 **Python asyncio + aiohttp + Redis + MongoDB** 的高并发分布式爬虫框架。  
框架分为 **主机 (master)** 和 **从机 (worker)** 两部分：

- **master**（`aio_crawler_master.py`）：读取 URL 列表，按 host 分桶并随机交错后，批量推入 Redis 队列。  
- **worker**（`aio_crawler_worker.py`）：从 Redis 批量取任务，并发抓取网页内容，写入 MongoDB（成功与失败分集合存储）。

---

## 功能特性

-  **异步并发**：基于 asyncio + aiohttp，支持上百并发请求  
-  **Redis 队列**：集中任务管理，支持批量推送、批量弹出（Redis 7 `BLMPOP`）  
-  **MongoDB 存储**：结果与失败任务分集合；支持**自动分库**（阈值切分）  
-  **多机分布式**：一个 master + 任意数量 worker，**水平扩展**  
-  **智能重试**：最多 5 次，明确失败码（如 404）不重试  
-  **主机交错**：入队按 host 分桶并交错，避免同 host 突发压力

---

## 1) 部署拓扑（当前环境）

| 角色 | 机器标识 | 地址 | 运行服务 | 说明 |
|------|----------|------|----------|------|
| 主机 | **217**  | `192.168.0.217` | **Redis**, **MongoDB**, `aio_crawler_master.py` | 集中队列与数据库，负责入队 |
| 从机 | **任意多台** | — | `aio_crawler_worker.py` | 只需能访问主机 Redis 和 Mongo，即可参与抓取 |

- **Redis**：运行在 `192.168.0.217:6379`  
- **MongoDB**：运行在 `192.168.0.217:27017`  
- **Worker 要求**：能连通 Redis 和 Mongo 即可，不依赖自身 IP

---

## 2) 环境依赖

- Python **3.9+**（推荐 3.11/3.12）  
- Redis **6/7**（推荐 7，支持 `BLMPOP`）  
- MongoDB **6+**  
- 依赖安装：

```bash
pip install -U aiohttp redis motor pymongo uvloop
```

---

## 3) MongoDB 存储

**默认数据路径：**

- **Linux**: `/var/lib/mongo` 或 `/var/lib/mongodb` （见 `/etc/mongod.conf`）  
- **Windows**: `C:\Program Files\MongoDB\Server\<version>\data` （或安装时指定）

**分库规则：** `results_0`, `results_1`, … （由 `MONGO_SPLIT_THRESHOLD` 控制）  

**集合：**
- 成功任务：`results_*/pages`  
- 失败任务：`results_*/failed_tasks`  

---

## 4) 配置参数

### Master (`aio_crawler_master.py`)

| 参数           | 说明               | 默认值 |
|----------------|--------------------|--------|
| CSV_FILE       | URL 列表 CSV 路径  | `google_url.csv` |
| REDIS_URL      | Redis 连接字符串   | `redis://localhost:6379/0` |
| TASK_LIST      | 任务队列键名       | `crawler:tasks` |
| DONE_KEY       | 入队完成标志键     | `crawler:tasks:enqueue_complete` |
| TEST_LIMIT     | 本次推入条数上限   | `100000` |
| CHUNK_SIZE     | CSV 分块读取大小   | `100000` |
| PIPELINE_BATCH | 每次 LPUSH 批量数  | `10000` |
| PRINT_EVERY    | 入队进度打印频率   | `100000` |

### Worker (`aio_crawler_worker.py`)

| 参数                 | 说明                   | 默认值 |
|----------------------|------------------------|--------|
| CONCURRENCY          | 并发协程数             | `300` |
| LIMIT_PER_HOST       | 每个 host 最大连接数   | `6` |
| TIMEOUT              | HTTP 读取超时（秒）    | `10` |
| BATCH_POP            | Redis 每批弹出任务数   | `200` |
| IDLE_QUIT_AFTER      | 空闲多久自动退出（秒） | `300` |
| MAX_RETRIES          | 每个 URL 最大尝试次数  | `5` |
| NON_RETRY_STATUS     | 不重试状态码集合       | `{400,401,403,404,410,451}` |
| LIGHT_MODE           | 是否只存 HTML 长度     | `False` |
| MONGO_SPLIT_THRESHOLD| 分库阈值（每库条数）   | `500000` |

---

## 5) 使用方法

### 准备数据（主机 217）

创建 `google_url.csv` 文件，首行为表头：
```csv
id,url
0,https://example.com
1,https://another.com/page
```

### 启动 Master（在 217）

```bash
python aio_crawler_master.py
```

示例日志：
```
ENQUEUE_PROGRESS: 100000 pushed
ENQUEUE_COMPLETE: pushed=100000, queue_len=100000
START_WORKERS: 所有 URL 已按 host 分桶并随机交错入队。现在可以启动 worker 进程抓取。
```

### 启动 Worker（任意机器）

```bash
export REDIS_URL="redis://192.168.0.217:6379/0"
export MONGO_URI="mongodb://192.168.0.217:27017"

python aio_crawler_worker.py
```

示例日志：
```
WORKERS_READY: redis=redis://192.168.0.217:6379/0, mongo=mongodb://192.168.0.217:27017, concurrency=300, ...
CONSUME_READY: first batch popped from Redis.
PERSIST_READY: first batch written to Mongo (pages).
PROGRESS_100K: 尝试=100,000 | ...
```

### 查看 MongoDB

- 成功任务：`results_*/pages`  
- 失败任务：`results_*/failed_tasks`  

---

## 6) 调优建议

- **逐步升并发**：从 `CONCURRENCY=50~100` 起步，再慢慢提升  
- **轻量模式**：`LIGHT_MODE=True` 时只存 `html_len`，降低存储压力  
- **RUN_ID**：每次运行使用独立 RUN_ID（或默认时间戳），避免 Mongo `_id` 撞键  
- **资源限制**：Linux 调大句柄：`ulimit -n 65535`  

---

## 7) 生产级托管（可选）

### master（在 217）

`/etc/systemd/system/async-crawler-master.service`

```ini
[Unit]
Description=Async Crawler - Master
After=network-online.target redis-server.service mongod.service
Wants=network-online.target

[Service]
User=youruser
WorkingDirectory=/opt/async-crawler
Environment=REDIS_URL=redis://127.0.0.1:6379/0
ExecStart=/usr/bin/python3 /opt/async-crawler/aio_crawler_master.py
Restart=always
RestartSec=2

[Install]
WantedBy=multi-user.target
```

### worker（在任意机器）

`/etc/systemd/system/async-crawler-worker.service`

```ini
[Unit]
Description=Async Crawler - Worker
After=network-online.target
Wants=network-online.target

[Service]
User=youruser
WorkingDirectory=/opt/async-crawler
Environment=REDIS_URL=redis://192.168.0.217:6379/0
Environment=MONGO_URI=mongodb://192.168.0.217:27017
ExecStart=/usr/bin/python3 /opt/async-crawler/aio_crawler_worker.py
Restart=always
RestartSec=2

[Install]
WantedBy=multi-user.target
```

---

## 8) 常见问题

- **Worker 需要知道谁的 IP？**  
  只需知道 **主机（217）** 的 Redis / Mongo 地址，自己的 IP 不重要。  

- **队列清空但 Worker 不退出**  
  退出条件：`DONE_KEY` 已设置 + 队列空 + in-flight=0。确认 master 已写入 `DONE_KEY`。  

- **大量 404/403**  
  已在 `NON_RETRY_STATUS` 默认不重试，需排查 UA、Referer 或站点限制。  

- **Redis 用 26379 端口？**  
  26379 是 Sentinel 端口，**不是**常规 Redis 端口。若未启用 Sentinel，请使用 **6379**。
