> **当前部署**：系统包含 1 个 Master 和 2 个从机 (ID = 177、220)，通过 Redis 队列分发任务，MongoDB 持久化抓取结果。

# Async Distributed Crawler

这是一个基于 **Python asyncio + aiohttp + Redis + MongoDB** 的高并发分布式爬虫框架。  
框架分为两类程序：**Master** 和 **Worker**。  

- **Master (`aio_crawler_master.py`)**  
  负责读取 URL 列表，按 host 分桶并随机交错后，批量推入 Redis 队列。  
  默认不会清空 Redis 队列，如需强制清空请加 `--force` 参数。  

- **Worker (`aio_crawler_worker.py` / `aio_crawler_worker_slave.py`)**  
  从 Redis 批量取任务，使用 aiohttp 高并发抓取网页，并写入 MongoDB。  
  - `aio_crawler_worker.py`：本地/默认配置版本（`localhost` Redis 和 Mongo）。  
  - `aio_crawler_worker_slave.py`：适用于远程 Redis/Mongo 的分布式 worker，从机可在多台机器同时运行。  

---

## 架构流程

1. **Master 入队**
   - 从 `google_url.csv` 读取 URL。
   - 按域名分桶，利用加权随机方式交错分发，避免单 host 过载。
   - 按批量（默认 1 万条）执行 Redis `LPUSH`，大幅减少网络开销。
   - 默认不清空旧队列，如要清空需加 `--force`。

2. **Worker 消费**
   - 通过 Redis `BLMPOP`/`BRPOP` 批量弹出任务（默认 200 条/批次）。
   - aiohttp 并发请求，支持 per-host 限流（默认 6）。
   - 成功页面写入 `pages` 集合，失败任务写入 `failed_tasks` 集合。
   - 失败 URL 最多重试 5 次，部分 4xx 不重试（400/401/403/404/410/451）。
   - 当 Redis 队列空并且所有任务完成时，worker 自动退出。

3. **MongoDB 存储**
   - 按 `MONGO_SPLIT_THRESHOLD` 分库（默认 50 万一库，库名如 `results_0`、`results_1`）。
   - 成功文档字段：`_id, url, host, http_status_code, html/html_len, crawl_timestamp`。
   - 失败文档字段：`task_id, url, host, status, failed_at, rounds`。

---

## 配置参数

### Master (`aio_crawler_master.py`)

- `CSV_FILE`：待入队的 URL CSV 文件（首行为表头）。  
- `TEST_LIMIT`：限制入队条数，0 表示全部。  
- `CHUNK_SIZE`：分块读取文件，避免内存占用过大。  
- `PIPELINE_BATCH`：每次 LPUSH 的批量大小。  
- `--force`：是否清空 Redis 队列和完成标志位。  

### Worker (`aio_crawler_worker.py`)

- `CONCURRENCY`：全局并发数（默认 300）。  
- `LIMIT_PER_HOST`：单 host 并发数（默认 6）。  
- `BATCH_POP`：每次从 Redis 批量取出的任务数（默认 200）。  
- `MAX_RETRIES`：单 URL 最大尝试次数（默认 5）。  
- `LIGHT_MODE`：是否仅存储页面长度而不保存 HTML。  

### Worker Slave (`aio_crawler_worker_slave.py`)- 与 `aio_crawler_worker.py` 基本相同，但默认连接远程 Redis/Mongo，适合分布式多机部署。  
- 远程连接示例：  
  - `REDIS_URL = 'redis://192.168.0.7:26379/0'`  
  - `MONGO_URI = 'mongodb://crawler:MongoPass@192.168.0.7:27017/?authSource=admin'`

- **从机 ID 分配**
  - 当前系统中有两个从机，分别为：
    - **从机一：ID = 177**
    - **从机二：ID = 220**
  - 这两个 ID 会写入 `RUN_ID` 或 `_id`/`task_id` 字段中，用于区分不同从机的抓取结果。
- 启动示例：
  ```bash
  # 在从机 177 上运行
  python aio_crawler_worker_slave.py --run-id=177

  # 在从机 220 上运行
  python aio_crawler_worker_slave.py --run-id=220
  ```
- 日志示例：
  ```
  [177] WORKERS_READY: redis=redis://192.168.0.7:26379/0, mongo=mongodb://...
  [220] WORKERS_READY: redis=redis://192.168.0.7:26379/0, mongo=mongodb://...
  ```
- MongoDB 中 `_id` 或 `task_id` 字段会包含 `177` 或 `220`，用于后续追踪哪个从机抓取的结果。


---

## 环境依赖

```bash
pip install aiohttp redis motor pymongo uvloop
```

- Python 3.9+（推荐 Linux，Windows 可用但需要 selector loop 兼容补丁）。  
- Redis 6+/7+  
- MongoDB 4.4+  

---

## 运行步骤

1. **启动 Redis & MongoDB**  
   - Redis 作为队列中心。  
   - MongoDB 用于存储抓取结果。  

2. **运行 Master**  
   ```bash
   python aio_crawler_master.py --force
   ```
   入队完成后会提示：
   ```
   ENQUEUE_COMPLETE: pushed=100000, queue_len=100000
   START_WORKERS: 现在可以启动 worker。
   ```

3. **运行 Worker（单机/多机并行）**  
   ```bash
   python aio_crawler_worker.py
   ```
   或在其他机器上运行 slave：
   ```bash
   python aio_crawler_worker_slave.py
   ```

4. **监控进度**  
   Worker 会周期性打印：
   ```
   PROGRESS_100K: 尝试=100,000 | 用时=320.5s | 速度=312.1 attempts/s | 最终成功URL=25,430 | 最终失败URL=1,240
   ```

5. **结束条件**  
   - 所有 Redis 队列任务完成。  
   - 所有 worker “in flight” 任务为 0。  
   - 自动退出并打印总结。  

---

## 日志与容错

- Redis 和 Mongo 在 worker 退出时会优雅关闭，避免 “Event loop is closed” 报错。  
- 支持 Windows 兼容（Proactor + Selector 补丁）。  
- Worker 出现网络错误（SSL/连接重置/超时）时默认安静忽略，只统计失败。  


---

# 原始 README

# Async Crawler

这是一个基于 **Python asyncio + aiohttp + Redis + MongoDB** 的高并发分布式爬虫框架。  
框架分为 **主机 (master)** 和 **从机 (worker)** 两部分：

- **master**（`aio_crawler_master.py`）：读取 URL 列表，按 host 分桶并随机交错后，批量推入 Redis 队列。  
- **worker**（`aio_crawler_worker.py`）：从 Redis 批量取任务，并发抓取网页内容，写入 MongoDB（成功与失败分集合存储）。

---

## 功能特性

- 🚀 **异步并发**：基于 asyncio + aiohttp，支持上百并发请求  
- 📦 **Redis 队列**：集中任务管理，支持批量推送、批量弹出（Redis 7 `BLMPOP`）  
- 🗄 **MongoDB 存储**：结果与失败任务分集合；支持**自动分库**（阈值切分）  
- 🌐 **多机分布式**：一个 master + 任意数量 worker，**水平扩展**  
- 🔁 **智能重试**：最多 5 次，明确失败码（如 404）不重试  
- ⚖️ **主机交错**：入队按 host 分桶并交错，避免同 host 突发压力

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
