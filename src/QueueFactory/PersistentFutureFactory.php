<?php

namespace Bernard\QueueFactory;

use Bernard\Queue\PersistentFutureQueue;
use Bernard\Queue\PersistentQueue;

class PersistentFutureFactory extends PersistentFactory {

    /**
     * {@inheritdoc}
     */
    public function create($queueName)
    {
        if (isset($this->queues[$queueName])) {
            return $this->queues[$queueName];
        }

        $queue = new PersistentFutureQueue($queueName, $this->driver, $this->serializer);

        return $this->queues[$queueName] = $queue;
    }

}
