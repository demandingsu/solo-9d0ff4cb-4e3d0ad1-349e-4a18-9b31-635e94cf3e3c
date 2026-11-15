"""共识合成：算法单元测试 + 真实子进程集成测试。

集成部分复用 tests/test_integration.py 的 _write_run / poll_collect，
在真实 uvicorn 子进程 + SQLite 上验证：端到端合成、门控、中断重试、
大运行不阻塞采集、/docs 可见。
"""
import os
import random
import time

from collector import db as dbmod
from collector.consensus import (
    extract_seq_qual,
    find_overlap,
    merge_pair,
    pair_fastq,
    reverse_complement,
)
from tests.conftest import Server
from tests.test_integration import _write_run, poll_collect

# ---------------------------------------------------------------- 算法单元测试

# 20bp 插入片段：R1 = 前 16bp，R2rc = 后 16bp，唯一重叠 12bp
INSERT0 = b"AAACCGGTTACGTAGCTAGC"
R1_0 = INSERT0[:16]
R2RC_0 = INSERT0[4:]


def test_reverse_complement():
    assert reverse_complement(b"ACGT") == b"ACGT"
    assert reverse_complement(b"AAAC") == b"GTTT"
    assert reverse_complement(b"NNAC") == b"GTNN"
    assert reverse_complement(b"") == b""


def test_find_overlap_unique():
    assert find_overlap(R1_0, R2RC_0) == 12


def test_find_overlap_min_length_boundary():
    # 真实重叠只有 11bp：默认阈值 12 下不合格，放宽到 11 则唯一合格
    r1 = b"ACGTACGTACGA"
    r2rc = b"CGTACGTACGAT"
    assert r1[1:] == r2rc[:11]
    assert find_overlap(r1, r2rc) is None
    assert find_overlap(r1, r2rc, min_overlap=11) == 11


def test_find_overlap_multiple_positions_returns_none():
    assert find_overlap(b"A" * 20, b"A" * 20) is None


def test_find_overlap_no_position_returns_none():
    assert find_overlap(b"A" * 20, b"C" * 20) is None


def test_find_overlap_mismatch_tolerance():
    mut1 = R2RC_0[:3] + b"A" + R2RC_0[4:]          # 1 处错配：合格
    assert find_overlap(R1_0, mut1) == 12
    mut2 = mut1[:5] + b"C" + mut1[6:]              # 2 处错配：不合格
    assert find_overlap(R1_0, mut2) is None


def test_merge_perfect_overlap_takes_higher_quality():
    seq, qual = merge_pair(R1_0, b"I" * 16, reverse_complement(R2RC_0), b"5" * 16)
    assert seq == INSERT0
    # 重叠区两端一致取高质量 'I'，R2rc 独有尾部保留其低质量 '5'
    assert qual == b"I" * 16 + b"5" * 4


def test_merge_mismatch_high_quality_side_wins():
    r2rc = R2RC_0[:3] + b"A" + R2RC_0[4:]          # 重叠区 1 处错配
    q1 = bytearray(b"I" * 16)
    q1[7] = ord("5")                                # R1 错配位 Phred 20
    r2 = reverse_complement(r2rc)
    seq, qual = merge_pair(R1_0, bytes(q1), r2, b"I" * 16)  # R2 端 40，差 20
    assert seq == INSERT0[:7] + b"A" + INSERT0[8:]  # 采信 R2 碱基
    assert qual == b"I" * 20                        # 采信 R2 质量


def test_merge_mismatch_small_quality_diff_rejects():
    r2rc = R2RC_0[:3] + b"A" + R2RC_0[4:]
    r2 = reverse_complement(r2rc)
    # 质量 40 vs 32，差 8 < 10：整对不合成
    assert merge_pair(R1_0, b"I" * 16, r2, b"A" * 16) is None


def test_merge_mismatch_exact_diff_10_merges():
    r2rc = R2RC_0[:3] + b"A" + R2RC_0[4:]
    r2 = reverse_complement(r2rc)
    # 质量 40 vs 30，差正好 10：采信高质量端（R1）
    seq, qual = merge_pair(R1_0, b"I" * 16, r2, b"?" * 16)
    assert seq == INSERT0
    assert qual == b"I" * 16 + b"?" * 4


def test_merge_ambiguous_or_absent_overlap_rejects():
    # 多个合格位置
    assert merge_pair(b"A" * 16, b"I" * 16, b"A" * 16, b"I" * 16) is None
    # 无合格位置（R2 = revcomp("C"*16) = "G"*16）
    assert merge_pair(b"A" * 16, b"I" * 16, b"G" * 16, b"I" * 16) is None
    # 读长不足 12
    assert merge_pair(b"ACGT", b"IIII", b"ACGT", b"IIII") is None


def test_extract_seq_qual_and_pair_fastq():
    rec = b"@x/1\nACGT\n+\nIIII\n"
    assert extract_seq_qual(rec) == (b"ACGT", b"IIII")
    assert pair_fastq("x", b"ACGT", b"IIII") == b"@x\nACGT\n+\nIIII\n"


# ---------------------------------------------------------------- 集成测试

def _fq(name, side, seq, qual):
    return b"@" + name.encode() + b"/" + str(side).encode() + b"\n" \
        + seq + b"\n+\n" + qual + b"\n"


def _pair(name, r1_seq, q1, r2rc_seq, q2):
    """按共识算法输入构造一对仪器原始 R1/R2 记录。"""
    return (
        _fq(name, 1, r1_seq, q1),
        _fq(name, 2, reverse_complement(r2rc_seq), q2),
    )


def _poll_consensus(client, collector, run_id, timeout=60):
    deadline = time.time() + timeout
    while time.time() < deadline:
        snap = client.get(f"{collector}/consensus/runs/{run_id}").json()
        if snap["status"] in ("completed", "error"):
            return snap
        time.sleep(0.2)
    raise AssertionError(f"共识未在 {timeout}s 内结束")


# 五对标准测试数据：2 对合成、3 对不合成
INSERT1 = b"TTGCAAGTCCTAGGATCCTA"
R1_1 = INSERT1[:16]
MUT1 = INSERT1[4:7] + b"G" + INSERT1[8:]          # R2rc 重叠区 1 错配，R2 质量高
Q1_1 = b"I" * 7 + b"5" + b"I" * 8                 # R1 错配位 Phred 20

INSERT2 = b"GGCCTTAACGATCGTACGGA"
R1_2 = INSERT2[:16]
MUT2 = INSERT2[4:9] + b"C" + INSERT2[10:]         # R2rc 重叠区 1 错配，质量差 8


def _standard_pairs():
    return [
        _pair("pair0", R1_0, b"I" * 16, R2RC_0, b"5" * 16),
        _pair("pair1", R1_1, Q1_1, MUT1, b"I" * 16),
        _pair("pair2", R1_2, b"I" * 16, MUT2, b"A" * 16),
        _pair("pair3", b"A" * 16, b"I" * 16, b"A" * 16, b"I" * 16),
        _pair("pair4", b"A" * 16, b"I" * 16, b"C" * 16, b"I" * 16),
    ]


EXPECTED_FASTQ = (
    b"@pair0\n" + INSERT0 + b"\n+\n" + b"I" * 16 + b"5" * 4 + b"\n"
    b"@pair1\n" + INSERT1[:7] + b"G" + INSERT1[8:] + b"\n+\n" + b"I" * 20 + b"\n"
)


def test_consensus_end_to_end(servers, client):
    relay, collector, meta = servers
    run_id = "consensus-ok"
    pairs = _standard_pairs()
    _write_run(
        meta, run_id,
        b"".join(p[0] for p in pairs),
        b"".join(p[1] for p in pairs),
    )
    snap, _ = poll_collect(client, collector, run_id, f"{relay}/runs/{run_id}")
    assert snap["pairs"] == 5

    r = client.post(f"{collector}/consensus/runs/{run_id}")
    assert r.status_code == 200, r.text
    snap = _poll_consensus(client, collector, run_id)
    assert snap["status"] == "completed"
    assert (snap["total"], snap["merged"], snap["unmerged"]) == (5, 2, 3)

    r = client.get(f"{collector}/consensus/runs/{run_id}/fastq")
    assert r.status_code == 200
    assert r.content == EXPECTED_FASTQ  # 按原配对顺序，仅含合成成功的对

    # 成功结果重复请求保持一致：不重新计算、内容不变
    again = client.post(f"{collector}/consensus/runs/{run_id}").json()
    assert (again["status"], again["merged"], again["unmerged"]) == \
        ("completed", 2, 3)
    r = client.get(f"{collector}/consensus/runs/{run_id}/fastq")
    assert r.content == EXPECTED_FASTQ


def test_consensus_requires_completed_run(servers, client):
    relay, collector, meta = servers
    run_id = "cons-running"
    pairs = _standard_pairs()[:1]
    _write_run(meta, run_id, pairs[0][0], pairs[0][1], done=False)
    snap = client.post(
        f"{collector}/collect/runs",
        json={"run_id": run_id, "base_url": f"{relay}/runs/{run_id}"},
    ).json()
    assert snap["status"] == "running"

    r = client.post(f"{collector}/consensus/runs/{run_id}")
    assert r.status_code == 409           # 采集未完成，不能生成共识
    assert r.json()["detail"]["code"] == "run_not_completed"

    r = client.post(f"{collector}/consensus/runs/nope")
    assert r.status_code == 404           # 运行不存在
    assert client.get(f"{collector}/consensus/runs/nope").status_code == 404
    assert client.get(f"{collector}/consensus/runs/nope/fastq").status_code == 404
    # 尚未生成过共识：状态与下载都是 404
    assert client.get(f"{collector}/consensus/runs/{run_id}").status_code == 404
    assert client.get(f"{collector}/consensus/runs/{run_id}/fastq").status_code == 404


def test_consensus_interrupted_run_retries_after_restart(servers, client):
    """模拟"写到一半进程死亡"：running 行 + 半个临时文件。

    验证：半份文件不得下载；新进程启动后残留被清理；重新触发可成功，
    且内容与完整生成一致。
    """
    relay, collector, meta = servers
    run_id = "cons-restart"
    insert_b = b"TTGCAAGTCCTAGGATCCTA"
    pairs = [
        _pair("ra", R1_0, b"I" * 16, R2RC_0, b"5" * 16),
        _pair("rb", insert_b[:16], b"I" * 16, insert_b[4:], b"5" * 16),
    ]
    _write_run(
        meta, run_id,
        b"".join(p[0] for p in pairs),
        b"".join(p[1] for p in pairs),
    )
    snap, _ = poll_collect(client, collector, run_id, f"{relay}/runs/{run_id}")
    assert snap["pairs"] == 2

    # 手工埋入中断残留：DB 中 running 行 + 写了一半的临时文件
    conn = dbmod.connect(meta["db_path"])
    dbmod.consensus_reset(conn, run_id, 2, time.time())
    conn.close()
    cons_dir = os.path.join(os.path.dirname(meta["db_path"]), "consensus")
    os.makedirs(cons_dir, exist_ok=True)
    tmp_file = os.path.join(cons_dir, f"{run_id}.consensus.fastq.tmp")
    with open(tmp_file, "wb") as fh:
        fh.write(b"@ra\nAAACCG")                    # 半份文件

    # 未完成前下载：409，半份文件拿不到
    r = client.get(f"{collector}/consensus/runs/{run_id}/fastq")
    assert r.status_code == 409
    assert client.get(f"{collector}/consensus/runs/{run_id}").json()["status"] == \
        "running"

    # 进程重启（全新实例打开同一 DB）：启动清理应移除 running 行与临时文件
    srv = Server("collector.app:app", {"COLLECTOR_DB": meta["db_path"]})
    srv.start()
    try:
        assert not os.path.exists(tmp_file), "临时文件应在重启时被清理"
        assert client.get(f"{srv.base_url}/consensus/runs/{run_id}").status_code == 404

        # 重试：重新生成成功，内容完整且与算法预期一致
        r = client.post(f"{srv.base_url}/consensus/runs/{run_id}")
        assert r.status_code == 200
        snap = _poll_consensus(client, srv.base_url, run_id)
        assert (snap["status"], snap["merged"], snap["unmerged"]) == \
            ("completed", 2, 0)
        expected = (
            b"@ra\n" + INSERT0 + b"\n+\n" + b"I" * 16 + b"5" * 4 + b"\n"
            b"@rb\n" + insert_b + b"\n+\n" + b"I" * 16 + b"5" * 4 + b"\n"
        )
        r = client.get(f"{srv.base_url}/consensus/runs/{run_id}/fastq")
        assert r.status_code == 200 and r.content == expected
    finally:
        srv.stop()


def _random_insert(seed, length):
    rng = random.Random(seed)
    return "".join(rng.choice("ACGT") for _ in range(length))


def test_consensus_large_run_does_not_block_collection(servers, client):
    """大运行共识合成期间，另一个运行的采集照常进行。"""
    relay, collector, meta = servers
    big = "cons-big"
    n = 3000
    inserts = [_random_insert(f"big-{i}", 40) for i in range(n)]
    r1 = b"".join(
        _fq(f"b{i}", 1, ins[:30].encode(), b"I" * 30) for i, ins in enumerate(inserts)
    )
    r2 = b"".join(
        _fq(f"b{i}", 2, reverse_complement(ins[10:].encode()), b"5" * 30)
        for i, ins in enumerate(inserts)
    )
    _write_run(meta, big, r1, r2)
    snap, _ = poll_collect(client, collector, big, f"{relay}/runs/{big}",
                           timeout=120)
    assert snap["pairs"] == n

    r = client.post(f"{collector}/consensus/runs/{big}")
    assert r.status_code == 200 and r.json()["status"] == "running"

    # 共识后台合成期间，另一个小运行的采集请求应立即被处理
    small = "cons-small"
    pairs = [
        _pair(f"s{i}", _random_insert(f"small-{i}", 40)[:30].encode(), b"I" * 30,
              _random_insert(f"small-{i}", 40)[10:].encode(), b"5" * 30)
        for i in range(4)
    ]
    _write_run(meta, small, b"".join(p[0] for p in pairs),
               b"".join(p[1] for p in pairs))
    t0 = time.time()
    snap, _ = poll_collect(client, collector, small, f"{relay}/runs/{small}",
                           timeout=30)
    assert snap["status"] == "completed" and snap["pairs"] == 4
    assert time.time() - t0 < 30

    # 大运行共识最终完成：3000 对全部合成，内容逐字节符合预期
    snap = _poll_consensus(client, collector, big, timeout=120)
    assert (snap["status"], snap["merged"], snap["unmerged"]) == \
        ("completed", n, 0)
    expected = b"".join(
        b"@b" + str(i).encode() + b"\n" + ins.encode() + b"\n+\n"
        + b"I" * 30 + b"5" * 10 + b"\n"
        for i, ins in enumerate(inserts)
    )
    r = client.get(f"{collector}/consensus/runs/{big}/fastq")
    assert r.status_code == 200 and r.content == expected


def test_openapi_docs_include_consensus_endpoints(servers, client):
    _, collector, _ = servers
    assert client.get(f"{collector}/docs").status_code == 200
    r = client.get(f"{collector}/openapi.json")
    assert r.status_code == 200
    paths = r.json()["paths"]
    assert "/consensus/runs/{run_id}" in paths
    assert "/consensus/runs/{run_id}/fastq" in paths
