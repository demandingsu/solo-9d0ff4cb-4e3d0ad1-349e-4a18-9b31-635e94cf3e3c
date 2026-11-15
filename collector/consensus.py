"""双端读段重叠共识合成（纯算法，无 IO、可独立测试）。

适用场景：短插入片段（insert 短于两端读长之和）的双端读段合并。

规则：
* R2 先反向互补（reverse complement），得到与 R1 同向的 R2rc；
* 寻找 **R1 后缀** 与 **R2rc 前缀** 的重叠：长度至少 ``MIN_OVERLAP``
  碱基、错配至多 ``MAX_MISMATCH`` 处；
* 合格位置必须**唯一**：没有合格位置或有多个合格位置都不合成；
* 重叠区碱基一致时，取两端中较高的 Phred+33 质量；
* 重叠区碱基错配时，仅当两端质量差至少 ``QUAL_DIFF`` 才选较高质量
  一端的碱基与质量，否则整对不合成；
* 非重叠区直接取各自原始碱基与质量。

质量按 Phred+33 ASCII 字节直接比较（字节值越大质量越高）。
"""
from __future__ import annotations

MIN_OVERLAP = 12   # 最短重叠碱基数
MAX_MISMATCH = 1   # 重叠区允许的最多错配数
QUAL_DIFF = 10     # 错配时采信高质量端所需的最小质量差（Phred 分）

_COMPLEMENT = bytes.maketrans(b"ACGTNacgtn", b"TGCANtgcan")


def reverse_complement(seq: bytes) -> bytes:
    """DNA 序列反向互补；A<->T、C<->G、N->N，其余字节原样保留。"""
    return seq.translate(_COMPLEMENT)[::-1]


def find_overlap(
    r1: bytes,
    r2rc: bytes,
    min_overlap: int = MIN_OVERLAP,
    max_mismatch: int = MAX_MISMATCH,
) -> int | None:
    """寻找 R1 后缀与 R2rc 前缀的唯一合格重叠，返回重叠长度。

    合格 = 重叠长度 >= ``min_overlap`` 且错配数 <= ``max_mismatch``。
    没有合格位置或合格位置多于一个时返回 None（均不合成）。
    """
    limit = min(len(r1), len(r2rc))
    found: list[int] = []
    for ol in range(min_overlap, limit + 1):
        mismatches = 0
        base = len(r1) - ol
        for i in range(ol):
            if r1[base + i] != r2rc[i]:
                mismatches += 1
                if mismatches > max_mismatch:
                    break
        if mismatches <= max_mismatch:
            found.append(ol)
            if len(found) > 1:
                return None  # 多个合格位置：不合成，提前结束
    return found[0] if len(found) == 1 else None


def merge_pair(
    r1_seq: bytes,
    r1_qual: bytes,
    r2_seq: bytes,
    r2_qual: bytes,
    min_overlap: int = MIN_OVERLAP,
    max_mismatch: int = MAX_MISMATCH,
    qual_diff: int = QUAL_DIFF,
) -> tuple[bytes, bytes] | None:
    """合并一对双端读段，返回 (共识序列, 共识质量)；不合成返回 None。

    ``r2_seq``/``r2_qual`` 为仪器原始 R2（本函数内部做反向互补）。
    """
    if len(r1_seq) != len(r1_qual) or len(r2_seq) != len(r2_qual):
        raise ValueError("序列与质量串长度不等")
    r2rc = reverse_complement(r2_seq)
    r2rc_qual = r2_qual[::-1]
    ol = find_overlap(r1_seq, r2rc, min_overlap, max_mismatch)
    if ol is None:
        return None

    cut = len(r1_seq) - ol  # R1 非重叠前缀长度
    seq = bytearray(r1_seq[:cut])
    qual = bytearray(r1_qual[:cut])
    for i in range(ol):
        b1, q1 = r1_seq[cut + i], r1_qual[cut + i]
        b2, q2 = r2rc[i], r2rc_qual[i]
        if b1 == b2:
            # 碱基一致：取较高的 Phred+33 质量
            seq.append(b1)
            qual.append(q1 if q1 >= q2 else q2)
        elif abs(q1 - q2) >= qual_diff:
            # 错配且质量差足够：采信高质量一端
            if q1 >= q2:
                seq.append(b1)
                qual.append(q1)
            else:
                seq.append(b2)
                qual.append(q2)
        else:
            # 错配且质量差不足：整对不合成
            return None
    # R2rc 的非重叠后缀（插入片段中 R1 未覆盖的部分）
    seq.extend(r2rc[ol:])
    qual.extend(r2rc_qual[ol:])
    return bytes(seq), bytes(qual)


def extract_seq_qual(record: bytes) -> tuple[bytes, bytes]:
    """从四行 FASTQ 记录字节中取 (序列行, 质量行)。"""
    lines = record.split(b"\n")
    # 记录以换行结尾，split 末尾会多出一个空串
    if len(lines) < 5:
        raise ValueError("FASTQ 记录不足四行")
    return lines[1], lines[3]


def pair_fastq(name: str, seq: bytes, qual: bytes) -> bytes:
    """生成一条共识 FASTQ 记录（单端，名称为去 /1 /2 后缀的配对名）。"""
    return b"@" + name.encode("ascii") + b"\n" + seq + b"\n+\n" + qual + b"\n"
