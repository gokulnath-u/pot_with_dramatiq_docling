import fitz  # PyMuPDF
import logging
import base64

logger = logging.getLogger("pipeline.split_pdf")

def split_pdf_to_base64_chunks(pdf_path: str, pages_per_chunk: int) -> list[str]:

    doc = fitz.open(pdf_path)
    total_pages = doc.page_count
    logger.info(f"PDF_TOTAL_PAGES | total_pages={total_pages}")

    base64_chunks = []

    for start in range(0, total_pages, pages_per_chunk):
        end = min(start + pages_per_chunk, total_pages)
        
        # Create a new empty PDF for this chunk
        chunk_doc = fitz.open()
        chunk_doc.insert_pdf(doc, from_page=start, to_page=end-1)
        
        # Get PDF bytes
        pdf_bytes = chunk_doc.tobytes()
        
        # Encode to Base64 string (so it can travel via JSON/Redis)
        b64_str = base64.b64encode(pdf_bytes).decode('utf-8')
        base64_chunks.append(b64_str)
        
        chunk_doc.close()
        logger.info(f"CHUNK_MEM_CREATED | pages={start+1}-{end} | size={len(b64_str)/1024:.2f}KB")

    doc.close()
    return base64_chunks