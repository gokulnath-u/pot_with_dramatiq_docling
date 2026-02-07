import time
from dramatiq import group
# Import the actual actor
from dramatiq_app import convert_document, result_backend
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
logger = logging.getLogger("warmup")

def force_wake_workers(worker_count=20):
    logger.info(f"WARMUP | Waking up {worker_count} workers...")
    
    # We send a "fake" task. 
    # The worker will receive it -> Load Model (if not loaded) -> Try to process -> Fail (invalid path)
    # But the MODEL will remain loaded in RAM!
    messages = [
        convert_document.message(
            pdf_path="dummy_wakeup.pdf", # Invalid path, will error but FORCE load
            start_page=0, 
            end_page=1, 
            chunk_index=-1
        ) 
        for _ in range(worker_count * 2) # Send extra to cover prefetch
    ]
    
    grp = group(messages)
    grp.run()
    
    logger.info("WARMUP | Wake-up signals sent! Check worker.log now.")
    logger.info("WARMUP | You should see 20 'WORKER READY' messages appear shortly.")

if __name__ == "__main__":
    force_wake_workers()