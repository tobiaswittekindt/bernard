<?php

namespace Bernard\Queue;

use Bernard\Envelope;

class PersistentFutureQueue extends PersistentQueue {

    public function enqueueInFuture(Envelope $envelope, int $timestamp): void
    {
        $this->errorIfClosed();
        $this->driver->pushMessageInFuture($this->name, $this->serializer->serialize($envelope), $timestamp);
    }

}
