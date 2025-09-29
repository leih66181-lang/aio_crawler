# aio_crawler
这是一个基于 Python asyncio + aiohttp + Redis + MongoDB 的高并发分布式爬虫框架。
框架分为 主机 (master) 和 从机 (worker) 两部分：

master (aio_crawler_master.py)：负责读取 URL 列表，按 host 分桶并随机交错后，批量推入 Redis 队列。
worker (aio_crawler_worker.py)：从 Redis 中批量取任务，发起并发请求，抓取网页内容，并将结果或失败信息写入 MongoDB。
功能特性
异步并发：基于 asyncio + aiohttp，支持上百并发请求。
Redis 队列：任务集中管理，支持批量推送、批量弹出 (Redis 7 BLMPOP)。
MongoDB 存储：结果和失败任务分表存储，支持自动分库分表。
多机分布式：一个 master + 多个 worker，可以水平扩展。
智能重试：支持失败任务最多重试 5 次，不重试明确失败的 HTTP 状态码 (如 404)。
高效交错：入队时按 host 分桶并交错，避免同 host URL 过于集中。
环境依赖
Python 3.9+（推荐 3.11/3.12，支持 asyncio 优化）
Redis 6/7（推荐 7，支持 BLMPOP）
MongoDB 6+
依赖库：
pip install aiohttp redis motor pymongo uvloop
配置说明
Master (aio_crawler_master.py)
参数	说明	默认值
CSV_FILE	URL 列表 CSV 文件路径（首行为表头）	google_url.csv
REDIS_URL	Redis 连接字符串	redis://localhost:6379/0
TASK_LIST	Redis 队列名	crawler:tasks
DONE_KEY	入队完成标志位	crawler:tasks:enqueue_complete
TEST_LIMIT	本次推入条数限制（0 表示不限制）	100000
CHUNK_SIZE	每次读入/处理的 URL 数量	100000
PIPELINE_BATCH	每次 LPUSH 的批量大小	10000
PRINT_EVERY	入队进度打印频率	100000
Worker (aio_crawler_worker.py)
参数	说明	默认值
CONCURRENCY	并发协程数	300
LIMIT_PER_HOST	每个 host 最大连接数	6
TIMEOUT	HTTP 读取超时	10s
BATCH_POP	每次从 Redis 批量取任务数	200
IDLE_QUIT_AFTER	队列空闲多久自动退出	300s
MAX_RETRIES	每个 URL 最大尝试次数	5
NON_RETRY_STATUS	不重试的状态码集合	{400,401,403,404,410,451}
LIGHT_MODE	是否只存 HTML 长度而不存正文	False
MONGO_SPLIT_THRESHOLD	Mongo 分库阈值（每库多少条任务）	500000
使用方法
准备数据

将所有待爬取 URL 放到 google_url.csv，首行是表头。
每行格式：
id,url
0,https://example.com
1,https://another.com/page
启动 Master

python aio_crawler_master.py
任务会按 host 分桶并交错后，批量推入 Redis。
日志输出示例：
ENQUEUE_PROGRESS: 100000 pushed
ENQUEUE_COMPLETE: pushed=100000, queue_len=100000
START_WORKERS: 所有 URL 已按 host 分桶并随机交错入队。现在可以启动从机/worker 进程抓取。
启动 Worker

python aio_crawler_worker.py
Worker 会并发抓取 Redis 中的任务，写入 MongoDB。
日志输出示例：
WORKERS_READY: redis=redis://localhost:6379/0, mongo=mongodb://localhost:27017, concurrency=300, ...
CONSUME_READY: first batch popped from Redis.
PERSIST_READY: first batch written to Mongo (pages).
PROGRESS_100K: 尝试=100,000 | 用时=320.5s | 速度=312 attempts/s | 成功URL=27,000 | 失败URL=4,000
查看 MongoDB

成功任务：results_*/pages
失败任务：results_*/failed_tasks
注意事项
Redis 必须足够内存：入队时可能存放数百万 URL。
MongoDB 分库策略：默认每 500,000 条任务切一个新库，防止单库过大。
调参建议：
初始时将 CONCURRENCY 设置为 50~100，避免被封。
根据目标站点情况，逐步提高 CONCURRENCY 和 LIMIT_PER_HOST。
轻量模式：若只需要 HTML 大小而非正文，设 LIGHT_MODE=True 可显著降低存储压力。
