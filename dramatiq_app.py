import os
import time
import logging
import fitz 
import io
import random
import multiprocessing
# ...existing imports...
from pathlib import Path

# Set clean process start method
try:
    multiprocessing.set_start_method("spawn")
except RuntimeError:
    pass

# ---------------- HARDWARE / OFFLINE CONFIG ----------------
os.environ["CUDA_VISIBLE_DEVICES"] = ""
os.environ["hf_hub_offline"] = "1"
os.environ["OMP_NUM_THREADS"] = "1"
os.environ["MKL_NUM_THREADS"] = "1"
os.environ["OPENBLAS_NUM_THREADS"] = "1"
os.environ["TORCH_NUM_THREADS"] = "1"
os.environ["OMP_PROC_BIND"] = "TRUE"
os.environ["OMP_PLACES"] = "cores"

from docling.document_converter import DocumentConverter, PdfFormatOption
from docling.datamodel.base_models import InputFormat, DocumentStream
from docling.datamodel.pipeline_options import PdfPipelineOptions, RapidOcrOptions
import dramatiq
from dramatiq.brokers.redis import RedisBroker
from dramatiq.results import Results
from dramatiq.results.backends import RedisBackend
from dramatiq.middleware import TimeLimit, callbacks, Retries, Middleware
from dotenv import load_dotenv

load_dotenv()
from log_config import setup_logging
setup_logging()
logger = logging.getLogger("dramatiq.ocr")

class ModelLoadingMiddleware(Middleware):
    def after_worker_boot(self, broker, worker):
        """Called once per worker process when it starts."""
        # Vital: Random jitter prevents 40 processes from hitting disk at exact same nanosecond
        time.sleep(random.uniform(0.1, 5.0))
        get_converter()

# ---------------- BROKER SETUP ----------------
result_backend = RedisBackend(host="localhost", encoder=dramatiq.encoder.JSONEncoder())

broker = RedisBroker(
    host="localhost", 
    middleware=[
        Results(backend=result_backend, result_ttl=7200000), 
        Retries(min_backoff=1000, max_backoff=900000, max_retries=3),
        TimeLimit(time_limit=7200 * 1000), 
        callbacks.Callbacks(),
        ModelLoadingMiddleware(),
    ]
)
broker.declare_queue("default") 
dramatiq.set_broker(broker)

_converter = None

def get_converter():
    global _converter
    if _converter is None:
        pid = os.getpid()
        
        # --- REMOVED ALL FILE LOCKING CODE HERE ---
        # We trust the OS Page Cache to serve the model files to 40 processes.
        
        logger.info(f"MODEL_LOAD | pid={pid} | Loading Docling model...")
        start_load = time.monotonic()
        
        # Path to valid ONNX models
        model_root = Path("/opt/shared/deps/rapidocr_models")
        
        if not model_root.exists():
            ocr_options = RapidOcrOptions()
        else:
            ocr_options = RapidOcrOptions(
                det_model_path=str(model_root / "en_PP-OCRv3_det_infer.onnx"),
                rec_model_path=str(model_root / "en_PP-OCRv4_rec_infer.onnx"),
                cls_model_path=str(model_root / "ch_ppocr_mobile_v2.0_cls_infer.onnx")
            )

        pipeline_options = PdfPipelineOptions(
            do_ocr=True, 
            do_table_structure=False, 
            ocr_options=ocr_options,
            generate_page_images=False,      
            generate_picture_images=False,   
        )
        _converter = DocumentConverter(
            format_options={InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options)}
        )
        
        # Warmup Inference
        try:
            dummy_pdf = fitz.open() 
            dummy_pdf.new_page()
            dummy_bytes = dummy_pdf.tobytes()
            dummy_pdf.close()
            _converter.convert(DocumentStream(name="warmup", stream=io.BytesIO(dummy_bytes)))
        except Exception:
            pass
            
        load_time = time.monotonic() - start_load
        print(f"✅ WORKER READY | pid={pid} | Loaded in {load_time:.2f}s")
        logger.info(f"MODEL_READY | pid={pid} | Took {load_time:.2f}s")
        
    return _converter

@dramatiq.actor(store_results=True, max_retries=0)
def convert_document(pdf_path: str, start_page: int, end_page: int, chunk_index: int):
    # This runs in PARALLEL because 'dramatiq -p 40' creates Independent Processes
    pid = os.getpid()
    converter = get_converter()
    
    start = time.monotonic()
    try:
        # 1. Open Source PDF
        doc = fitz.open(pdf_path)
        
        # 2. Extract specific pages
        chunk_doc = fitz.open()
        chunk_doc.insert_pdf(doc, from_page=start_page, to_page=end_page-1)
        pdf_bytes = chunk_doc.tobytes()
        doc.close()
        chunk_doc.close()

        # 3. Process
        doc_stream = DocumentStream(name=f"chunk_{chunk_index}.pdf", stream=io.BytesIO(pdf_bytes))
        result = converter.convert(doc_stream)
        md = result.document.export_to_markdown() if result and result.document else ""
    except Exception as e:
        logger.error(f"CHUNK_FAIL | chunk={chunk_index} | pid={pid} | err={e}")
        raise e

    elapsed = time.monotonic() - start
    logger.info(f"CHUNK_OK | chunk={chunk_index} | pid={pid} | time={elapsed:.2f}s")
    return md