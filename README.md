# arque
Asyncio Reliable Queue (based on redis)

Inspired by Tom DeWire's article "Reliable Queueing in Redis (Part 1)" [[1]](#ref1) [[2]](#ref2) and the "torrelque" python module [[3]](#ref3).

#### Features:
    - Asynchronous: based on asyncio and aioredis
    - Reliable: at any moment task data stored in redis database
    - Throttling: controls number of tasks in execution
    - Delayed queue: defers task availability
    - Dead letters: put task data in failed queue after number of predefined retry attempts
    - Tested on Python 3.7 and redis server '>=3.0.6', '<=5.0.5'
    - Used in containerized applications (managed by kubernetes) in high load environments
    
#### Install:
```bash
pip install arque
```
    
#### Usage:

```python
import signal
import random
import logging
import asyncio
import aioredis
import time
from functools import wraps
from arque import Arque

logger = logging.getLogger(__name__)


async def shutdown(signal, loop):
    """Cleanup tasks tied to the service's shutdown."""
    logging.info(f"Received exit signal {signal.name}...")
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    [task.cancel() for task in tasks]
    logging.info(f"Cancelling {len(tasks)}outstanding tasks")
    await asyncio.gather(*tasks)
    logging.info(f"Flushing metrics")
    loop.stop()


def aioredis_pool(host='redis://localhost', encoding='utf8'):
    def wrapper(func):
        @wraps(func)
        async def wrapped():
            redis = await aioredis.create_redis_pool(host, encoding=encoding)
            try:
                return await func(redis=redis)
            finally:
                redis.close()
                await redis.wait_closed()

        return wrapped

    return wrapper


@aioredis_pool(host='redis://localhost', encoding='utf8')
async def produce_task(redis=None):
    logger.info('Starting producing...')
    queue = Arque(redis=redis)
    while True:
        for _ in range(1):
            task = {'value': random.randint(0, 99)}
            task_id = f"custom_{task['value']}_{time.time()}"
            logger.debug('Produced task %s', task)
            await queue.enqueue(task, task_id=task_id, task_timeout=10, delay=1)
        await asyncio.sleep(1)


async def process(task_data):
    logger.debug('Consumed task %s', task_data)
    await asyncio.sleep(1)


@aioredis_pool(host='redis://localhost', encoding='utf8')
async def consume_task(redis=None):
    logger.info('Starting consuming...')
    queue = Arque(redis=redis, working_limit=3)
    while True:
        task_id, task_data = await queue.dequeue()
        if task_id == '__not_found__':
            continue

        if task_id == '__overloaded__':
            print(f'TASK ID: {task_id}')
            await asyncio.sleep(1)
            continue

        if task_id == '__marked_as_failed___':
            print(f'FAILED  ID: {task_id}')
            continue

        try:
            await process(task_data)
            await queue.release(task_id)
        except Exception:
            logger.exception('Job processing has failed')
            await queue.requeue(task_id, delay=5)
            stats = await queue.get_stats()
            logger.info(stats)


@aioredis_pool(host='redis://localhost', encoding='utf8')
async def sweep_task(redis=None):
    logger.info('Starting sweeping...')
    queue = Arque(redis=redis, sweep_interval=5)
    await queue.schedule_sweep()


@aioredis_pool(host='redis://localhost', encoding='utf8')
async def stats_task(redis=None):
    logger.info('Starting stats...')
    queue = Arque(redis=redis)
    while True:
        stats = await queue.get_stats()
        logger.info(stats)
        await asyncio.sleep(5)


async def example():
    tasks = []
    for _ in range(5):
        tasks.append(consume_task())
    tasks.append(produce_task())
    tasks.append(sweep_task())
    tasks.append(stats_task())
    await asyncio.gather(*tasks)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(message)s')
    loop = asyncio.get_event_loop()
    signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT, signal.SIGUSR1)
    for s in signals:
        loop.add_signal_handler(s, lambda s=s: asyncio.create_task(shutdown(s, loop)))
    try:
        loop.run_until_complete(example())
    finally:
        loop.close()
        logging.info("Successfully shutdown...")

```    

#### Reference
<a name="ref1"></a>[1] [Reliable Queueing in Redis (Part 1)](http://blog.bronto.com/engineering/reliable-queueing-in-redis-part-1/)  
<a name="ref2"></a>[2] [DEWIRE Redis as a Reliable Work Queue.pdf](https://www.percona.com/sites/default/files/DEWIRE%20Redis%20as%20a%20Reliable%20Work%20Queue.pdf)  
<a name="ref3"></a>[3] [torrelque](https://bitbucket.org/saaj/torrelque)  
