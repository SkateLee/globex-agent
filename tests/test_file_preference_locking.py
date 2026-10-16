"""文件偏好的进程间锁在 Windows 和 Unix 上都不能丢失并发写入。"""
import asyncio
import multiprocessing
from pathlib import Path

from app.domain.buyer.preference import BuyerPreference
from app.infrastructure.persistence.json_file_stores import JsonFilePreferenceStore


def _append_preferences(data_dir, worker_id, start):
    async def append():
        store = JsonFilePreferenceStore(Path(data_dir))
        for index in range(10):
            await store.append(BuyerPreference("concurrent-buyer", "like", f"{worker_id}-{index}"))

    if not start.wait(timeout=15):
        raise TimeoutError("并发写入测试未收到开始信号")
    asyncio.run(append())


def test_concurrent_processes_preserve_all_preferences(tmp_path):
    context = multiprocessing.get_context("spawn")
    start = context.Event()
    processes = [
        context.Process(target=_append_preferences, args=(str(tmp_path), worker_id, start))
        for worker_id in range(4)
    ]
    try:
        for process in processes:
            process.start()
        start.set()
        for process in processes:
            process.join(timeout=30)
            assert process.exitcode == 0
        preferences = asyncio.run(JsonFilePreferenceStore(tmp_path).list_by_buyer("concurrent-buyer"))
        assert {item.statement for item in preferences} == {
            f"{worker_id}-{index}" for worker_id in range(4) for index in range(10)
        }
    finally:
        for process in processes:
            if process.is_alive():
                process.terminate()
                process.join(timeout=5)
