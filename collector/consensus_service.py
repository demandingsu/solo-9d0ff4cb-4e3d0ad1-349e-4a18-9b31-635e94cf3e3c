"""共识合成任务管理：后台逐批合成、原子写出、中断可重试。

可靠性与隔离设计：
* 结果先写 `{run_id}.consensus.fastq.tmp`，全部合成并 fsync 后用
  `os.replace` 原子改名为正式文件——下载方只会拿到完整文件或
  拿不到文件，绝不会下载到半份；
* 任务状态落 SQLite（consensus 表）：进程重启时处于 running 的任务
  视为中断残留，由 :meth:`ConsensusManager.recover` 在启动时清理
  （删行 + 删临时文件），调用方重新 POST 即可重试；
* 已成功（completed）且结果文件存在时，重复触发直接返回原计数，
  不重新计算，重复下载内容保持一致；
* 逐批（BATCH_SIZE 对）处理：读库、合并计算、写盘全部放在
  `asyncio.to_thread` 中执行，大运行的慢计算不阻塞事件循环，
  其它运行的采集与下载不受影响；
* 任务使用独立 SQLite 连接（WAL 模式），与采集写事务互不阻塞。
"""
from __future__ import annotations

import asyncio
import os
import time

from . import db
from .consensus import extract_seq_qual, merge_pair, pair_fastq

BATCH_SIZE = 200  # 每批处理的配对数


class ConsensusError(RuntimeError):
    """触发共识时的可预期错误（运行不存在 / 尚未完成采集）。"""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


class ConsensusManager:
    def __init__(self, conn, db_path: str):
        self.conn = conn
        self.db_path = db_path
        self.out_dir = os.path.join(
            os.path.dirname(os.path.abspath(db_path)), "consensus"
        )
        os.makedirs(self.out_dir, exist_ok=True)
        self._tasks: dict[str, asyncio.Task] = {}
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------ 路径
    def tmp_path(self, run_id: str) -> str:
        return os.path.join(self.out_dir, f"{run_id}.consensus.fastq.tmp")

    def final_path(self, run_id: str) -> str:
        return os.path.join(self.out_dir, f"{run_id}.consensus.fastq")

    @staticmethod
    def _silent_unlink(path: str) -> None:
        try:
            os.unlink(path)
        except OSError:
            pass

    # -------------------------------------------------------------- 启动清理
    def recover(self) -> list[str]:
        """进程启动时清理中断的共识任务（running 行 + 临时文件）。

        已 completed 的结果保留，可直接下载；被清理的运行重新 POST
        即可从头再合成。返回被清理的 run_id 列表。
        """
        dropped = db.consensus_drop_running(self.conn)
        for run_id in dropped:
            self._silent_unlink(self.tmp_path(run_id))
        return dropped

    # ------------------------------------------------------------------ 查询
    def status(self, run_id: str) -> dict | None:
        row = db.consensus_get(self.conn, run_id)
        return db.consensus_snapshot(row) if row is not None else None

    # ------------------------------------------------------------------ 触发
    async def trigger(self, run_id: str) -> dict:
        """触发/查询一次共识合成（幂等）。

        * 运行不存在 -> ConsensusError("run_not_found")
        * 运行未完成采集 -> ConsensusError("run_not_completed")
        * 已成功且文件在 -> 直接返回原结果（不重新计算）
        * 正在合成 -> 返回当前进度
        * 其余（未生成 / 上次失败 / 上次中断被清理 / 结果文件丢失）-> 重新合成
        """
        row = db.get_run(self.conn, run_id)
        if row is None:
            raise ConsensusError("run_not_found", f"运行 {run_id} 不存在")
        if row["status"] != "completed":
            raise ConsensusError(
                "run_not_completed",
                f"运行 {run_id} 采集状态为 {row['status']}，"
                "只有全部读段配对完成（completed）后才能生成共识",
            )
        async with self._lock:
            existing = db.consensus_get(self.conn, run_id)
            if (
                existing is not None
                and existing["status"] == "completed"
                and os.path.exists(self.final_path(run_id))
            ):
                return db.consensus_snapshot(existing)
            task = self._tasks.get(run_id)
            if task is not None and not task.done():
                return db.consensus_snapshot(existing)
            db.consensus_reset(self.conn, run_id, row["pairs"], time.time())
            # 清掉旧的临时/结果文件，保证重新生成不受残留影响
            self._silent_unlink(self.tmp_path(run_id))
            self._silent_unlink(self.final_path(run_id))
            self._tasks[run_id] = asyncio.create_task(self._run(run_id))
            return db.consensus_snapshot(db.consensus_get(self.conn, run_id))

    # -------------------------------------------------------------- 后台执行
    async def _run(self, run_id: str) -> None:
        tmp, final = self.tmp_path(run_id), self.final_path(run_id)
        merged = unmerged = 0
        task_conn = db.connect(self.db_path)  # 独立连接，与采集写事务隔离
        try:
            fh = await asyncio.to_thread(open, tmp, "wb")
            try:
                next_seq = 0
                while True:
                    rows = await asyncio.to_thread(
                        db.fetch_pairs, task_conn, run_id, next_seq, BATCH_SIZE
                    )
                    if not rows:
                        break
                    next_seq = rows[-1]["seq"] + 1
                    chunk, m, u = await asyncio.to_thread(self._merge_batch, rows)
                    merged += m
                    unmerged += u
                    if chunk:
                        await asyncio.to_thread(fh.write, b"".join(chunk))
                    await asyncio.to_thread(
                        db.consensus_progress,
                        task_conn, run_id, merged, unmerged, time.time(),
                    )
                await asyncio.to_thread(fh.flush)
                await asyncio.to_thread(os.fsync, fh.fileno())
            finally:
                await asyncio.to_thread(fh.close)
            # 全部写完并 fsync 后原子改名：半份文件永远不会以正式名出现
            await asyncio.to_thread(os.replace, tmp, final)
            await asyncio.to_thread(
                db.consensus_complete, task_conn, run_id, merged, unmerged, time.time()
            )
        except Exception as exc:  # 失败可重试：标记 error 并丢弃临时文件
            try:
                await asyncio.to_thread(
                    db.consensus_fail, task_conn, run_id, repr(exc), time.time()
                )
            finally:
                self._silent_unlink(tmp)
        finally:
            task_conn.close()
            self._tasks.pop(run_id, None)

    @staticmethod
    def _merge_batch(rows) -> tuple[list[bytes], int, int]:
        """纯 CPU 合并一批配对，返回 (FASTQ 记录列表, 合成数, 未合成数)。

        保持 pairs 表顺序（调用方按 seq 升序传入），成功合成的记录
        按原配对顺序排列。
        """
        chunk: list[bytes] = []
        merged = unmerged = 0
        for row in rows:
            r1_seq, r1_qual = extract_seq_qual(row["r1_record"])
            r2_seq, r2_qual = extract_seq_qual(row["r2_record"])
            res = merge_pair(r1_seq, r1_qual, r2_seq, r2_qual)
            if res is None:
                unmerged += 1
            else:
                merged += 1
                chunk.append(pair_fastq(row["name"], res[0], res[1]))
        return chunk, merged, unmerged
