import signal
import random
import logging
import asyncio
import aioredis
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
        async def wrapped(loop, redis=None):
            redis = await aioredis.create_redis_pool(host, loop=loop, encoding=encoding)
            try:
                return await func(loop=loop, redis=redis)
            finally:
                redis.close()
                await redis.wait_closed()

        return wrapped

    return wrapper


@aioredis_pool(host='redis://localhost', encoding='utf8')
async def produce_task(loop, redis=None):
    logger.info('Starting producing...')
    queue = Arque(redis=redis, loop=loop)
    while True:
        for _ in range(1):
            task = {'value': random.randint(0, 99)}
            logger.debug('Produced task %s', task)
            await queue.enqueue(task, task_timeout=10, delay=1)
        await asyncio.sleep(1)


async def process(task_data):
    logger.debug('Consumed task %s', task_data)
    await asyncio.sleep(1)


@aioredis_pool(host='redis://localhost', encoding='utf8')
async def consume_task(loop, redis=None):
    logger.info('Starting consuming...')
    queue = Arque(redis=redis, loop=loop, working_limit=3)
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
            await asyncio.sleep(1)
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
async def sweep_task(loop, redis=None):
    logger.info('Starting sweeping...')
    queue = Arque(redis=redis, loop=loop, sweep_interval=5)
    await queue.schedule_sweep()


@aioredis_pool(host='redis://localhost', encoding='utf8')
async def stats_task(loop, redis=None):
    logger.info('Starting stats...')
    queue = Arque(redis=redis, loop=loop)
    while True:
        stats = await queue.get_stats()
        logger.info(stats)
        await asyncio.sleep(5)


def create_tasks(loop):
    tasks = []
    for _ in range(5):
        tasks.append(consume_task(loop))
    tasks.append(produce_task(loop))
    tasks.append(sweep_task(loop))
    tasks.append(stats_task(loop))
    return tasks


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(message)s')
    loop = asyncio.get_event_loop()
    signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT, signal.SIGUSR1)
    for s in signals:
        loop.add_signal_handler(s, lambda s=s: asyncio.create_task(shutdown(s, loop)))
    try:
        loop.run_until_complete(asyncio.gather(*create_tasks(loop)))
    finally:
        loop.close()
        logging.info("Successfully shutdown...")
