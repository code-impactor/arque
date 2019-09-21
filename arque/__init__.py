"""
Asyncio Reliable Queue (based on redis)

Inspired by Tom DeWire' article "Reliable Queueing in Redis (Part 1)" [1] and the "torrelque" python module [2].
Features:
    - Queue with repeats, delays and failed state.
    - Based on asyncio, aioredis.
    - Tested on Python 3.7

[1] http://blog.bronto.com/engineering/reliable-queueing-in-redis-part-1/
[2] https://bitbucket.org/saaj/torrelque
"""

import asyncio
import json
import uuid
import time
import hashlib
import logging
import aioredis

logger = logging.getLogger('arque')


class Arque:
    stale_timeout = 120
    """ Default timeout for a task in working queue to be considered stale """

    requeue_limit = 3
    """ Max attempts to process task """

    working_limit = 1000
    """ Max tasks in processing """

    sweep_interval = 10
    """ Default interval between the housekeeping routine """

    keys = {
        'pending': 'pending',  # list
        'working': 'working',  # sorted set
        'delayed': 'delayed',  # sorted set
        'tasks': 'tasks',  # hash
        'stats': 'stats',  # prefix for hashes
        'failed': 'failed'  # list
    }
    """ Redis keys dict which values are prefixed on instance initialisation """

    _serializer = None
    """ Task serializer that converts task object into and from string representation """

    _redis = None
    """ Instance of aioredis connection pool  """

    _loop = None
    """ asyncio loop """

    def __init__(self, redis, loop=None, prefix='arque', serializer=json, sweep_interval=None,
                 requeue_limit=None, working_limit=None):
        self._redis = redis
        self._loop = loop
        self._serializer = serializer
        self.keys = {k: '{}:{}'.format(prefix, v) for k, v in self.keys.items()}
        self._sweep_interval = sweep_interval or self.sweep_interval
        self._working_limit = working_limit or self.working_limit
        self._requeue_limit = requeue_limit or self.requeue_limit
        self._run_sweep = False

    def _get_stats_key(self, task_id):
        return '{}:{}'.format(self.keys['stats'], task_id)

    async def _call_script(self, script, keys, args):
        """ https://redis.io/commands/eval:
            If an EVAL is performed against a Redis instance all the subsequent EVALSHA calls will succeed """

        digest = hashlib.sha1(script.encode()).hexdigest()
        try:
            with await self._redis as r:
                result = await r.evalsha(digest=digest, keys=keys, args=args)
        except aioredis.errors.ReplyError as e:
            if str(e).startswith('NOSCRIPT'):
                with await self._redis as r:
                    result = await r.eval(script=script, keys=keys, args=args)
            else:
                raise e

        return result

    async def enqueue(self, task, task_id=None, task_timeout=None, delay=None):
        """ Enqueue arbitrary (though serializable) *task* object into
        working queue. Return task id to reference the task. """
        task_timeout = task_timeout or self.stale_timeout
        task_id = '{}-{}'.format((task_id or uuid.uuid1().hex), task_timeout)
        task_data = self._serializer.dumps(task)
        now = time.time()
        with await self._redis as r:
            pipe = r.multi_exec()
            pipe.hset(self.keys['tasks'], task_id, task_data)
            pipe.hset(self._get_stats_key(task_id), 'enqueue_time', now)
            if delay is None:
                pipe.lpush(self.keys['pending'], task_id)
            else:
                pipe.zadd(self.keys['delayed'], now + delay, task_id)
            await pipe.execute()

        logger.debug(f'En-queued task ID: {task_id}. Data: {task_data}')
        return task_id

    def enqueue_sync(self, task, task_id=None, task_timeout=None, delay=None):
        """ Enqueue arbitrary (though serializable) *task* object into
        working queue. Return UUID to reference the task. """
        task_timeout = task_timeout or self.stale_timeout
        task_id = '{}-{}'.format((task_id or uuid.uuid1().hex), task_timeout)
        task_data = self._serializer.dumps(task)
        now = time.time()
        pipe = self._redis.pipeline(transaction=True)
        pipe.hset(self.keys['tasks'], task_id, task_data)
        pipe.hset(self._get_stats_key(task_id), 'enqueue_time', now)
        if delay is None:
            pipe.lpush(self.keys['pending'], task_id)
        else:
            pipe.zadd(self.keys['delayed'], now + delay, task_id)
        pipe.execute()
        logger.debug(f'En-queued task ID: {task_id}. Data: {task_data}')

        return task_id

    async def dequeue(self, timeout=None):
        """ Get a task from working queue. """
        # The trick with BRPOPLPUSH makes it possible to make dequeue()
        # blocking, thus avoid overhead and latency caused by polling.
        # Later on the ``task_id`` LREM will be applied, which is less
        # efficient that LPOP or RPOP, but because the rotation has just
        # occurred the entry being deleted is at the beginning of the
        # list and LREM complexity is close to O(1).
        # Returns task_id:
        # __not_found__ - task consumed by another worker
        # __overloaded__ - reached limit of tasks in processing
        # __marked_as_failed___ - reached limit of retry attempts - task marked as failed and cleaned up namespace

        with await self._redis as r:
            task_id = await r.brpoplpush(self.keys['pending'], self.keys['pending'], timeout=timeout or 0)

        if not task_id:
            return None, None

        script = """
            local pending, working, tasks, failed = unpack(KEYS)
            local task_id = ARGV[1]
            local now = ARGV[2]
            local max_attempts = tonumber(ARGV[3])
            local working_limit = tonumber(ARGV[4])

            local working_count = redis.call('ZCARD', working)
            if working_count >= working_limit then
                return {'__overloaded__', 'null'}
            end

            local removed = redis.call('LREM', pending, 1, task_id)
            if removed == 0 then
                return {'__not_found__', 'null'}
            end

            local task_data = redis.call('HGET', tasks, task_id)
            local stale = now + task_id:match('-([^\-]+)$')
            local stats = KEYS[5] .. ':' .. task_id
            local attempts = redis.call('HGET', stats, 'requeue_count')
            local attempts_n = tonumber(attempts)

            if attempts_n ~= nil and attempts_n > max_attempts then
                redis.call('HDEL', tasks, task_id)
                redis.call('DEL', stats)
                redis.call('LPUSH', failed, task_data)
                return {'__marked_as_failed___', task_data}
            end

            redis.call('ZADD', working, stale, task_id)
            redis.call('HSET', stats, 'last_dequeue_time', now)
            redis.call('HINCRBY', stats, 'dequeue_count', 1)

            return {task_id, task_data}
        """

        keys = [self.keys[k] for k in ('pending', 'working', 'tasks', 'failed', 'stats')]
        args = [task_id, time.time(), self._requeue_limit, self._working_limit]
        task_id, task_data = await self._call_script(script, keys, args)

        logger.debug(f'De-queued task ID: {task_id}. Data: {task_data}')

        return task_id or None, self._serializer.loads(task_data)

    async def requeue(self, task_id, delay=None):
        """ Return failed task into into working queue. Its dequeue may be
        deferred on given amount of seconds and then the task is put in
        delayed queue. """

        now = time.time()
        with await self._redis as r:
            pipe = r.multi_exec()
            pipe.zrem(self.keys['working'], task_id)
            stats_key = self._get_stats_key(task_id)
            pipe.hset(stats_key, 'last_requeue_time', now)
            pipe.hincrby(stats_key, 'requeue_count', 1)
            if delay is None:
                pipe.lpush(self.keys['pending'], task_id)
            else:
                pipe.zadd(self.keys['delayed'], now + delay, task_id)
            await pipe.execute()

        logger.debug(f'Re-queued task ID: {task_id}.')

    async def release(self, task_id):
        """ Mark task as successfully processed. """
        with await self._redis as r:
            pipe = r.multi_exec()
            pipe.zrem(self.keys['working'], task_id)
            pipe.hdel(self.keys['tasks'], task_id)
            pipe.delete(self._get_stats_key(task_id))
            await pipe.execute()

        logger.debug(f'Released task ID: {task_id}.')

    async def sweep(self):
        """ Return stale tasks from working queue into pending list. Move ready
        deferred tasks into pending list. """
        script = """
            local function requeue(pending_key, target_key, stats_prefix, now)
                local task_ids = redis.call('ZRANGEBYSCORE', target_key, 0, now)
                if #task_ids == 0 then
                    return 0
                end
                redis.call('LPUSH', pending_key, unpack(task_ids))
                redis.call('ZREM', target_key, unpack(task_ids))
                local stats_key
                for _, task_id in ipairs(task_ids) do
                    stats_key = stats_prefix .. ':' .. task_id
                    redis.call('HSET', stats_key, 'last_requeue_time', now)
                    redis.call('HINCRBY', stats_key, 'requeue_count', 1)
                end
                return #task_ids
            end

            local pending, working, delayed, stats = unpack(KEYS)
            local now = ARGV[1]
            
            return requeue(pending, working, stats, now) + requeue(pending, delayed, stats, now)
        """

        keys = [self.keys[k] for k in ('pending', 'working', 'delayed', 'stats')]
        args = [time.time()]
        result = await self._call_script(script, keys, args=args)
        logger.debug(f'Swept {result} tasks.')

        return result

    async def schedule_sweep(self):
        self._run_sweep = True
        while self._run_sweep:
            await asyncio.sleep(self._sweep_interval)
            await self.sweep()

    async def unschedule_sweep(self):
        self._run_sweep = False

    async def get_stats(self):
        with await self._redis as r:
            pipe = r.pipeline()
            pipe.hlen(self.keys['tasks'])
            pipe.llen(self.keys['pending'])
            pipe.zcard(self.keys['working'])
            pipe.zcard(self.keys['delayed'])
            pipe.llen(self.keys['failed'])
            result = await pipe.execute()
        return dict(zip(('tasks', 'pending', 'working', 'delayed', 'failed'), result))

    async def get_task_stats(self, task_id):
        with await self._redis as r:
            result = await r.hgetall(self._get_stats_key(task_id))
        if not result:
            return None
        return {
            'enqueue_time': float(result['enqueue_time']),
            'last_dequeue_time': float(result.get('last_dequeue_time', 0)) or None,
            'dequeue_count': int(result.get('dequeue_count', 0)),
            'last_requeue_time': float(result.get('last_requeue_time', 0)) or None,
            'requeue_count': int(result.get('requeue_count', 0))
        }
