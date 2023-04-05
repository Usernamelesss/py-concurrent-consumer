from queue import Queue


class ClearableQueue(Queue):
    """Extends the Python's built-in Queue to expose a clear() method"""

    def clear(self):
        """
        Remove all elements from the queue and reset unfinished tasks to 0
        """
        with self.mutex:
            self.queue.clear()
            self.unfinished_tasks = 0

            # Notify all waiters that all tasks get dequeued, if any
            self.all_tasks_done.notify_all()

            # Notify one put waiter, if any, that queue is empty.
            self.not_full.notify()
