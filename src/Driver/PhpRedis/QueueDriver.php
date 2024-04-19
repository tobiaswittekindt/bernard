<?php

namespace Bernard\Driver\PhpRedis;

use Bernard\Driver\Message;
use DateTime;
use Redis;

/**
 * Implements a Driver for use with https://github.com/nicolasff/phpredis.
 */
class QueueDriver implements \Bernard\Driver
{
    const QUEUE_PREFIX = 'future-queue:';

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
    public function pushMessageInFuture($queueName, $message, int $timestamp): void
    {
        error_log('Push message: ' . json_encode($queueName));
        $this->redis->zAdd($this->resolveKey($queueName), $timestamp, $message);
    }

    /**
     * {@inheritdoc}
     */
    public function pushMessage(string $queueName, string $message): void {
        // TODO: Implement pushMessage() method.
    }

    /**
     * {@inheritdoc}
     */
    public function popMessage($queueName, $duration = 5): ?Message
    {
        // When PhpRedis is set up with an Redis::OPT_PREFIX
        // it does set the prefix to the key and to the timeout value something like:
        // "BLPOP" "bernard:queue:my-queue" "bernard:5"
        //
        // To set the resolved key in an array seems fixing this issue. We get:
        // "BLPOP" "bernard:queue:my-queue" "5"
        //
        // see https://github.com/nicolasff/phpredis/issues/158

        $timestamp = (new DateTime())->getTimestamp();
        $message = $this->redis->zRangeByScore($this->resolveKey($queueName), '-inf', $timestamp, ['limit' => [0, 1]]);

        if(!empty($message)) {
            $messageKey = $message[0];
            $this->redis->zRem($this->resolveKey($queueName), $messageKey);
            return new Message($messageKey);
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
