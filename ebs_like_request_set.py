from __future__ import annotations
import asyncio, json, pathlib, random, time
from typing import Dict, List, Optional

from prefect import flow, task, get_run_logger
from prefect.states import State
from prefect.futures import PrefectFuture


# -------------------------
# Core "unit of work" tasks
# -------------------------

@task
def load_file_into_table(batch_id: str, source_system: str):
    log = get_run_logger()
    log.info(f"[S1] Loading file(s) for batch {batch_id} from {source_system} into STG tables...")
    time.sleep(0.5)  # simulate I/O
    return {"batch_id": batch_id, "rows_loaded": 1000}

@task
def update_metadata_for_batch(batch_id: str):
    log = get_run_logger()
    log.info(f"[S1] Updating metadata for batch {batch_id}...")
    time.sleep(0.3)

@task
def call_parallel_worker(item_id: int, batch_id: str):
    log = get_run_logger()
    # simulate CPU/IO + occasional warning
    work_ms = random.randint(200, 700) / 1000
    time.sleep(work_ms)
    if random.random() < 0.05:
        log.warning(f"[S2] Item {item_id} in batch {batch_id}: WARNING/ERROR simulated")
    return {"item_id": item_id, "status": "OK", "elapsed": work_ms}

@task
def validate_loaded_data(batch_id: str):
    log = get_run_logger()
    log.info(f"[S3] Validating data for batch {batch_id}...")
    time.sleep(0.4)
    return True

@task
def submit_approval_process(batch_id: str, approver_group: str = "AP"):
    log = get_run_logger()
    log.info(f"[S3] Submitting approval workflow for batch {batch_id} to {approver_group}...")
    time.sleep(0.4)
    return {"submitted": True}

@task
def print_pdf_report(batch_id: str):
    log = get_run_logger()
    log.info(f"[S4] Printing PDF output/report for batch {batch_id}...")
    time.sleep(0.3)
    return f"/tmp/report_{batch_id}.pdf"


# -------------------------------------------------
# Helpers for "EBS-like" stages with thread control
# -------------------------------------------------

async def _run_with_semaphore(n_concurrent: int, coros: List[asyncio.Future]):
    """
    Run a list of coroutines with a semaphore limiting concurrency.
    Returns gathered results when all complete.
    """
    sem = asyncio.Semaphore(n_concurrent)

    async def _wrap(coro_factory):
        async with sem:
            return await coro_factory()

    return await asyncio.gather(*[_wrap(c) for c in coros])


def _load_lookup(path: Optional[str]) -> Dict[str, int]:
    """
    Load thread counts from a JSON lookup file.
    Example file contents:
    {
      "STAGE2_THREADS": 5,
      "APPROVAL_THREADS": 3
    }
    """
    if not path:
        return {}
    p = pathlib.Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Lookup JSON not found: {p}")
    with p.open() as f:
        return json.load(f)


# ---------------
# Main EBS-like flow
# ---------------

@flow(name="HUMAP-CAS Invoice Upload Process (EBS-like)")
async def humap_cas_invoice_upload_process(
    # Common parameters (similar to your screenshots)
    operating_unit: str = "US",
    source_system: str = "CAS",
    batch_id: str = "INVBATCH",
    gl_date: Optional[str] = None,
    approval_flag: bool = True,
    sabrix_flag: bool = False,

    # Thread counts: can be given directly or looked up
    stage2_threads: Optional[int] = None,          # master parallel worker threads
    approval_stage_threads: Optional[int] = None,  # approval workflow threads

    # Lookup for defaults (JSON file path). If provided, any missing threads above
    # will be read from here: {"STAGE2_THREADS": 5, "APPROVAL_THREADS": 2}
    lookup_json_path: Optional[str] = None,

    # Size of the parallel work in Stage-2 (e.g., number of invoice sub-batches)
    stage2_items: int = 20,
):
    log = get_run_logger()

    # 1) Resolve thread counts from lookup if not directly provided
    lookup = _load_lookup(lookup_json_path)
    s2_threads = stage2_threads or int(lookup.get("STAGE2_THREADS", 4))
    appr_threads = approval_stage_threads or int(lookup.get("APPROVAL_THREADS", 2))

    log.info("----- REQUEST SET (Prefect) START -----")
    log.info(f"OU={operating_unit}, Source={source_system}, Batch={batch_id}, GL Date={gl_date}")
    log.info(f"Flags: approval={approval_flag}, sabrix={sabrix_flag}")
    log.info(f"Threads: Stage-2={s2_threads}, Approval-Stage={appr_threads}")

    # --------------------------
    # Stage-1 (Serial) - Upload
    # --------------------------
    log.info("Stage-1: Upload into interface + metadata (serial)")
    _ = load_file_into_table.submit(batch_id=batch_id, source_system=source_system)
    _.result()  # ensure serial behavior
    update_metadata_for_batch.submit(batch_id=batch_id).result()

    # ---------------------------------
    # Stage-2 (Parallel, with N threads)
    # ---------------------------------
    log.info("Stage-2: Master Parallel Processor (limited concurrency)")
    # Create N "items" to process (think: child requests)
    items = list(range(1, stage2_items + 1))

    async def _coro_factory(i: int):
        # Running Prefect task inside async wrapper
        fut: PrefectFuture = call_parallel_worker.submit(item_id=i, batch_id=batch_id)
        return await fut.aresult()

    # Build coroutine factories
    coros = [lambda i=i: _coro_factory(i) for i in items]
    s2_results = await _run_with_semaphore(s2_threads, coros)
    log.info(f"Stage-2 processed {len(s2_results)} items")

    # ------------------------------------------------
    # Stage-3 (Mix): Validate (serial) + Approval (parallel)
    # ------------------------------------------------
    log.info("Stage-3: Validate + (optional) Approval")
    valid = validate_loaded_data.submit(batch_id=batch_id).result()
    if approval_flag and valid:
        log.info("Stage-3A: Approval sub-stage (parallel, limited)")
        # Example: spawn N approval sub-requests; limit with approval semaphore
        appr_items = list(range(1, 1 + max(1, min(5, appr_threads * 2))))  # demo size
        async def _appr_coro_factory(i: int):
            fut: PrefectFuture = submit_approval_process.submit(batch_id=f"{batch_id}-{i}")
            return await fut.aresult()
        appr_coros = [lambda i=i: _appr_coro_factory(i) for i in appr_items]
        _ = await _run_with_semaphore(appr_threads, appr_coros)

    # -------------------------
    # Stage-4 (Serial) - Output
    # -------------------------
    log.info("Stage-4: Print output")
    pdf_path = print_pdf_report.submit(batch_id=batch_id).result()
    log.info(f"Report written to: {pdf_path}")
    log.info("----- REQUEST SET (Prefect) END -----")
