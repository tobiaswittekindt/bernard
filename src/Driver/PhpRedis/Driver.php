<?php

namespace Bernard\Driver\PhpRedis;

use Bernard\Driver\Message;
use Redis;

/**
 * Implements a Driver for use with https://github.com/nicolasff/phpredis.
 */
class Driver implements \Bernard\Driver
{
    const QUEUE_PREFIX = 'queue:';

    protected $redis;

    /**
     * @param Redis $redis
     */
    public function __construct(Redis $redis)
    {
        $this->redis = $redis;
    }

    /**
     * {@inheritdoc}
     */
    public function listQueues(): array
    {
        return $this->redis->sMembers('queues');
    }

    /**
     * {@inheritdoc}
     */
    public function createQueue($queueName): void
    {
        $this->redis->sAdd('queues', $queueName);
    }

    /**
     * {@inheritdoc}
     */
    public function countMessages($queueName): int
    {
        return $this->redis->lLen(self::QUEUE_PREFIX.$queueName);
    }

    /**
     * {@inheritdoc}
     */
    public function pushMessage($queueName, $message): void
    {
        $this->redis->rpush($this->resolveKey($queueName), $message);
    }

    /**
     * {@inheritdoc}
     */
    public function popMessage($queueName, $duration = 0.005): ?Message
    {
        // When PhpRedis is set up with an Redis::OPT_PREFIX
        // it does set the prefix to the key and to the timeout value something like:
        // "BLPOP" "bernard:queue:my-queue" "bernard:5"
        //
        // To set the resolved key in an array seems fixing this issue. We get:
        // "BLPOP" "bernard:queue:my-queue" "5"
        //
        // see https://github.com/nicolasff/phpredis/issues/158
        list(, $message) = $this->redis->blpop([$this->resolveKey($queueName)], $duration) ?: null;

        if(!empty($message)) {
            return new Message($message);
        }
        return null;
    }

    /**
     * {@inheritdoc}
     */
    public function peekQueue($queueName, $index = 0, $limit = 20): array
    {
        $limit += $index - 1;

        return $this->redis->lRange($this->resolveKey($queueName), $index, $limit);
    }

    /**
     * {@inheritdoc}
     */
    public function acknowledgeMessage($queueName, $receipt): void
    {
    }

    /**
     * {@inheritdoc}
     */
    public function removeQueue($queueName): void
    {
        $this->redis->sRem('queues', $queueName);
        $this->redis->del($this->resolveKey($queueName));
    }

    /**
     * {@inheritdoc}
     */
    public function info(): array
    {
        return $this->redis->info();
    }

    /**
     * Transform the queueName into a key.
     *
     * @param string $queueName
     *
     * @return string
     */
    protected function resolveKey($queueName): string
    {
        return self::QUEUE_PREFIX.$queueName;
    }
}
