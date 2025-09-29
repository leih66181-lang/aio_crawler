# aio_crawler

这是一个基于 **Python asyncio + aiohttp + Redis + MongoDB** 的高并发分布式爬虫框架。  
框架分为 **主机 (master)** 和 **从机 (worker)** 两部分：

- **master**（`aio_crawler_master.py`）：读取 URL 列表，按 host 分桶并随机交错后，批量推入 Redis 队列。  
- **worker**（`aio_crawler_worker.py`）：从 Redis 批量取任务，并发抓取网页内容，写入 MongoDB（成功与失败分集合存储）。

---

## 功能特性

- **异步并发**：基于 asyncio + aiohttp，支持上百并发请求  
- **Redis 队列**：集中任务管理，支持批量推送、批量弹出（Redis 7 `BLMPOP`）  
- **MongoDB 存储**：结果与失败任务分集合；支持**自动分库**（阈值切分）  
- **多机分布式**：一个 master + 多个 worker，**水平扩展**  
- **智能重试**：最多 5 次，明确失败码（如 404）不重试  
- **主机交错**：入队按 host 分桶并交错，避免同 host 突发压力

---

## 1) 部署拓扑（当前环境）

| 角色 | 机器标识 | 内网 IP（示例） | 运行服务 | 说明 |
|---|---|---|---|---|
| 主机 | **217** | `192.168.0.217` | **Redis**, **MongoDB**, `aio_crawler_master.py` | 集中队列与数据库，负责**入队** |
| 从机 | **177** | `192.168.0.177` | `aio_crawler_worker.py` | 消费 Redis 任务，写入 217 的 Mongo |
| 从机 | **220** | `192.168.0.220` | `aio_crawler_worker.py` | 同上 |

- **Redis**：运行在 `217:6379`  
- **MongoDB**：运行在 `217:27017`  
- 所有 worker（177/220）需能访问 `217:6379` 与 `217:27017`  

---

## 2) 环境依赖

- Python **3.9+**（推荐 3.11/3.12）  
- Redis **6/7**（推荐 7，支持 `BLMPOP`）  
- MongoDB **6+**  
- 依赖安装：
  ```bash
  pip install -U aiohttp redis motor pymongo uvloop
3) MongoDB 存储
默认存储路径：

Linux: /var/lib/mongo 或 /var/lib/mongodb

Windows: C:\Program Files\MongoDB\Server\<version>\data

数据库分库规则：results_0, results_1, …

成功任务：results_*/pages

失败任务：results_*/failed_tasks

4) 配置参数
Master (aio_crawler_master.py)
参数	说明	默认值
CSV_FILE	URL 列表 CSV 路径（首行表头）	google_url.csv
REDIS_URL	Redis 连接	redis://localhost:6379/0
TASK_LIST	任务队列键名	crawler:tasks
DONE_KEY	入队完成标志键	crawler:tasks:enqueue_complete
TEST_LIMIT	本次推入条数上限（0=不限）	100000
CHUNK_SIZE	CSV 分块读取大小	100000
PIPELINE_BATCH	LPUSH 一次写入数量	10000
PRINT_EVERY	入队进度打印频率	100000

Worker (aio_crawler_worker.py)
参数	说明	默认值
CONCURRENCY	并发协程数	300
LIMIT_PER_HOST	每 host 最大连接	6
TIMEOUT	HTTP 读取超时（秒）	10
BATCH_POP	Redis 每批弹出任务数	200
IDLE_QUIT_AFTER	空闲多久自动退出（秒）	300
MAX_RETRIES	每 URL 最大尝试次数	5
NON_RETRY_STATUS	不重试状态码	{400,401,403,404,410,451}
LIGHT_MODE	只存 HTML 长度	False
MONGO_SPLIT_THRESHOLD	分库阈值（每库条数）	500000

5) 使用方法
准备数据
将所有待爬取 URL 放到 google_url.csv，首行是表头：

csv
复制代码
id,url
0,https://example.com
1,https://another.com/page
启动 Master（217）
bash
复制代码
python aio_crawler_master.py
日志输出示例：

makefile
复制代码
ENQUEUE_PROGRESS: 100000 pushed
ENQUEUE_COMPLETE: pushed=100000, queue_len=100000
START_WORKERS: 所有 URL 已按 host 分桶并随机交错入队。现在可以启动从机/worker 进程抓取。
启动 Worker（177 / 220）
bash
复制代码
export REDIS_URL="redis://192.168.0.217:6379/0"
export MONGO_URI="mongodb://192.168.0.217:27017"
python aio_crawler_worker.py
日志输出示例：

makefile
复制代码
WORKERS_READY: redis=redis://192.168.0.217:6379/0, mongo=mongodb://192.168.0.217:27017, concurrency=300, ...
CONSUME_READY: first batch popped from Redis.
PERSIST_READY: first batch written to Mongo (pages).
PROGRESS_100K: 尝试=100,000 | ...
查看 MongoDB
成功任务：results_*/pages

失败任务：results_*/failed_tasks

6) 调优建议
逐步升并发：从 CONCURRENCY=50~100 起步，根据目标站点与网络慢慢提高

轻量模式：LIGHT_MODE=True 时只存 html_len，可显著降低存储压力

RUN_ID：每次运行建议指定 RUN_ID，避免 Mongo _id 撞键

资源限制：Linux 上调大句柄数：ulimit -n 65535

7) 实际部署记录
主机 (master)：217 (192.168.0.217)

从机 (worker)：177 (192.168.0.177)、220 (192.168.0.220)

Redis：217:6379

MongoDB：217:27017

数据分库：results_0, results_1, …

yaml
复制代码

---

这样一份就是标准的 `README.md`，你直接复制保存就可以了 ✅。  

要不要我再帮你写一段 **Git 命令示例**（从克隆到推送），放在 README 的最下面，让团队成员照着操作？






您说：
从机的ip我并不知道，从机代码里的是主机ip


ChatGPT 说：

已思考 5s
明白了。你的从机（worker）只需要知道主机（master）的 IP/域名来连 Redis 和 Mongo；从机自己的 IP 并不重要，也无需写进 README。下面是已改好的可复制版 README：把“从机 IP”去掉，统一用“任意机器（可多台）”描述，并把连接都指向主机 217。

markdown
复制代码
# Async Crawler

这是一个基于 **Python asyncio + aiohttp + Redis + MongoDB** 的高并发分布式爬虫框架。  
框架分为 **主机 (master)** 和 **从机 (worker)** 两部分：

- **master**（`aio_crawler_master.py`）：读取 URL 列表，按 host 分桶并随机交错后，批量推入 Redis 队列。  
- **worker**（`aio_crawler_worker.py`）：从 Redis 批量取任务，并发抓取网页内容，写入 MongoDB（成功与失败分集合存储）。

---

## 功能特性

- **异步并发**：基于 asyncio + aiohttp，支持上百并发请求  
- **Redis 队列**：集中任务管理，支持批量推送、批量弹出（Redis 7 `BLMPOP`）  
- **MongoDB 存储**：结果与失败任务分集合；支持**自动分库**（阈值切分）  
- **多机分布式**：一个 master + 任意数量 worker，**水平扩展**  
- **智能重试**：最多 5 次，明确失败码（如 404）不重试  
- **主机交错**：入队按 host 分桶并交错，避免同 host 突发压力

---

## 1) 部署拓扑（当前环境）

| 角色 | 机器标识 | 位置 | 运行服务 | 说明 |
|---|---|---|---|---|
| 主机 | **217** | `192.168.0.217` | **Redis**, **MongoDB**, `aio_crawler_master.py` | 集中队列与数据库，负责**入队** |
| 从机 | **任意多台** | **无需预先记录 IP** | `aio_crawler_worker.py` | 只要能访问主机的 Redis 和 Mongo 即可 |

- **Redis（主机）**：`192.168.0.217:6379`  
- **MongoDB（主机）**：`192.168.0.217:27017`  
- **Worker 的要求**：网络能直连以上两个端口即可（所在机器 IP 无需写进 README/代码）

> 如使用域名：将 `192.168.0.217` 替换为你的域名（例如 `crawler-master.local`）。

---

## 2) 环境依赖

- Python **3.9+**（推荐 3.11/3.12）  
- Redis **6/7**（推荐 7，支持 `BLMPOP`）  
- MongoDB **6+**  
- 依赖安装：
  ```bash
  pip install -U aiohttp redis motor pymongo uvloop
3) MongoDB 存储
默认数据路径：

Linux: /var/lib/mongo 或 /var/lib/mongodb（见 /etc/mongod.conf）

Windows: C:\Program Files\MongoDB\Server\<version>\data（或安装时指定）

数据分库：results_0, results_1, …（由 MONGO_SPLIT_THRESHOLD 控制）

集合：

成功任务：results_*/pages

失败任务：results_*/failed_tasks

4) 配置参数
Master（aio_crawler_master.py）
参数	说明	默认值
CSV_FILE	URL 列表 CSV 路径（首行表头）	google_url.csv
REDIS_URL	Redis 连接	redis://localhost:6379/0
TASK_LIST	任务队列键名	crawler:tasks
DONE_KEY	入队完成标志键	crawler:tasks:enqueue_complete
TEST_LIMIT	本次推入条数上限（0=不限）	100000
CHUNK_SIZE	CSV 分块读取大小	100000
PIPELINE_BATCH	LPUSH 一次写入数量	10000
PRINT_EVERY	入队进度打印频率	100000

Worker（aio_crawler_worker.py）
参数	说明	默认值
CONCURRENCY	并发协程数	300
LIMIT_PER_HOST	每 host 最大连接	6
TIMEOUT	HTTP 读取超时（秒）	10
BATCH_POP	Redis 每批弹出任务数	200
IDLE_QUIT_AFTER	空闲多久自动退出（秒）	300
MAX_RETRIES	每 URL 最大尝试次数	5
NON_RETRY_STATUS	不重试状态码	{400,401,403,404,410,451}
LIGHT_MODE	只存 HTML 长度	False
MONGO_SPLIT_THRESHOLD	分库阈值（每库条数）	500000

5) 使用方法
准备数据（主机 217）
将所有待爬取 URL 放到 google_url.csv，首行是表头：

csv
复制代码
id,url
0,https://example.com
1,https://another.com/page
启动 Master（在 217）
bash
复制代码
# 确保 Redis 与 Mongo 已在 217 运行，并允许内网访问
python aio_crawler_master.py
日志示例：

makefile
复制代码
ENQUEUE_PROGRESS: 100000 pushed
ENQUEUE_COMPLETE: pushed=100000, queue_len=100000
START_WORKERS: 所有 URL 已按 host 分桶并随机交错入队。现在可以启动从机/worker 进程抓取。
启动 Worker（任意机器）
在任意一台可达 217 的机器上运行：

bash
复制代码
export REDIS_URL="redis://192.168.0.217:6379/0"
export MONGO_URI="mongodb://192.168.0.217:27017"
# 如需账号认证，改为：mongodb://user:pass@192.168.0.217:27017/?authSource=admin

python aio_crawler_worker.py
日志示例：

makefile
复制代码
WORKERS_READY: redis=redis://192.168.0.217:6379/0, mongo=mongodb://192.168.0.217:27017, concurrency=300, ...
CONSUME_READY: first batch popped from Redis.
PERSIST_READY: first batch written to Mongo (pages).
PROGRESS_100K: 尝试=100,000 | ...
查看 MongoDB（在 217）
成功任务：results_*/pages

失败任务：results_*/failed_tasks

6) 调优建议
逐步升并发：从 CONCURRENCY=50~100 起步，再根据站点与网络提高

轻量模式：LIGHT_MODE=True 仅存 html_len，适合压测/摸底

RUN_ID：每次运行使用独立 RUN_ID（或默认时间戳），避免 Mongo _id 撞键

资源限制：Linux 调大句柄：ulimit -n 65535

7) 生产级托管（可选，systemd 示例）
仅示例说明，按需使用。Worker 可在任意机器上重复部署多份。

master（在 217）
/etc/systemd/system/async-crawler-master.service

ini
复制代码
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
worker（在任意机器）
/etc/systemd/system/async-crawler-worker.service

ini
复制代码
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
8) 常见问题
Worker 必须知道谁的 IP？
只需要知道**主机（217）**的 Redis / Mongo 地址。Worker 的自身 IP 不重要，也无需固定。

队列清空但 Worker 不退出
退出条件：DONE_KEY 已设置 + 队列空 + in-flight=0。确认 master 已设 DONE_KEY。

大量 404/403
NON_RETRY_STATUS 已默认不重试；排查 UA/Referer/IP 或站点策略。

用了 26379 端口
26379 是 Sentinel 端口，不是常规 Redis 端口。除非你部署了 Sentinel 客户端模式，否则请使用 6379。
