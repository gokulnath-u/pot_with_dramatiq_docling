import time
import logging
from pathlib import Path
import sys
import shutil
import fitz

from dramatiq import group
from dramatiq.results.errors import ResultMissing
from dotenv import load_dotenv

# Import actors
from dramatiq_app import convert_document, result_backend
from log_config import setup_logging

load_dotenv()
setup_logging()
logger = logging.getLogger("pipeline.main")

# CONFIG
TARGET_PAGES_PER_CHUNK = 10   # Keep generous size for In-Memory
OUTPUT_DIR = Path("output_md")
OUTPUT_DIR.mkdir(exist_ok=True)

def run_pipeline_for_pdf(input_pdf_path: str, output_md_path: Path):
    start_ts = time.monotonic()
    logger.info(f"PDF_START | file={input_pdf_path}")

    # 1. OPTIMIZATION: Just count pages. Don't split data.
    doc = fitz.open(input_pdf_path)
    total_pages = doc.page_count
    doc.close()

    logger.info(f"PDF_SCANNED | total_pages={total_pages} | mode=LAZY_PARALLEL")

    # 2. Create Coordinates (Instructions)
    messages = []
    chunk_idx = 0
    
    # Calculate total chunks for stats later
    total_chunks_calc = 0
    
    for start in range(0, total_pages, TARGET_PAGES_PER_CHUNK):
        end = min(start + TARGET_PAGES_PER_CHUNK, total_pages)
        
        # We send the coordinates, not the Base64 data
        # convert_document(pdf_path, start_page, end_page, chunk_index)
        msg = convert_document.message(str(input_pdf_path), start, end, chunk_idx)
        messages.append(msg)
        chunk_idx += 1
        total_chunks_calc += 1

    # 3. Send Group (Instantly - messages are tiny)
    grp = group(messages)
    grp.run()
    logger.info("PIPELINE_SENT | Waiting for results from Redis...")
    
    # 4. Result Polling Loop (IN-MEMORY)
    # We hold the message objects. We check them for results.
    results = [None] * len(messages)
    completed_count = 0
    total_chunks = len(messages)
    
    print(f"{'TIME':<10} | {'COMPLETED':<10} | {'PROGRESS':<10}")
    print("-" * 36)

    while completed_count < total_chunks:
        checked_count = 0
        for i, msg in enumerate(messages):
            # If we already have this result, skip
            if results[i] is not None:
                checked_count += 1
                continue
            
            try:
                res = result_backend.get_result(msg, block=False)
                results[i] = res
                checked_count += 1
            except ResultMissing:
                pass # Not ready yet
            except Exception as e:
                # If it's just missing, ignore. If it's a real error, log it.
                if "ResultMissing" in str(e):
                    pass
                else:
                    logger.error(f"TASK_ERR | msg_id={msg.message_id} | err={e}")
                    results[i] = "" # Mark as failed/empty to proceed
                    checked_count += 1
        
        # Log progress
        if checked_count > completed_count:
            completed_count = checked_count
            elapsed = time.monotonic() - start_ts
            percent = (completed_count / total_chunks) * 100
            print(f"{elapsed:>8.1f}s | {completed_count}/{total_chunks} | {percent:>8.1f}%")

        if completed_count == total_chunks:
            break
            
        time.sleep(0.1) # Prevent spamming Redis

    # 5. Merge Results (In Memory)
    logger.info("MERGING_RESULTS | stitching markdown...")
    full_content = []
    
    for idx, content in enumerate(results):
        full_content.append(f"\n<!-- ===== Chunk {idx} ===== -->\n")
        if content:
            full_content.append(content)
        else:
            full_content.append("\n<!-- FAILED CHUNK -->\n")

    with open(output_md_path, "w", encoding="utf-8") as f:
        f.write("".join(full_content))
        
    total_time = time.monotonic() - start_ts
    
    # Stats
    pages_processed = total_pages
    ppm = (pages_processed / total_time) * 60  # Pages per minute
    
    logger.info(f"PDF_END | file={input_pdf_path} | time={total_time:.2f}s | Speed={ppm:.1f} PPM")
    return total_time

if __name__ == "__main__":
    input_pdf = "pdf/Sample5-LC.pdf"
    if len(sys.argv) > 1:
        input_pdf = sys.argv[1]
        
    output_md = OUTPUT_DIR / f"{Path(input_pdf).stem}.md"

    if not Path(input_pdf).exists():
        print(f"Error: {input_pdf} not found.")
    else:
        run_pipeline_for_pdf(input_pdf, output_md)