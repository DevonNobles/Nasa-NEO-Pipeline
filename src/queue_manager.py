import redis
import logging
from typing import Optional

logger = logging.getLogger(__name__)

class RedisQueueManager:

    def __init__(self):
        self.queue = redis.Redis(host='localhost', port=6379, db=0)


    def add_to_queue(self, item: str, queue_name: str = 'queue') -> None:
        """Add item to queue."""
        logger.info(f"Adding '{item}' to queue '{queue_name}'")
        self.queue.lpush(queue_name, item)
        logger.info(f"Successfully added '{item}' to queue")


    def get_from_queue(self, queue_name: str = 'queue') -> Optional[str]:
        """Get item from queue."""
        logger.debug(f"Getting item from queue '{queue_name}'")
        result = self.queue.rpop(queue_name)
        item = result.decode() if result else None

        if item:
            logger.info(f"Retrieved '{item}' from queue")
        else:
            logger.debug("Queue is empty")

        return item

queue = RedisQueueManager()