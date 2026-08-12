#!/usr/bin/env python3
"""
benchmark.py
压测脚本：提交 GPUJob，收集调度延迟，输出 JSON 结果。

调度延迟定义：
    从 GPUJob 被 kubectl apply 到所属的所有 Pod 都进入 Running 状态的时间。
    这个定义覆盖了 Controller 生成 Pod + 调度器 Filter/Score/Bind 的全链路。

用法：
    # 先生成 job YAML
    python3 generate_jobs.py --scheduler kubegpu --out jobs/kubegpu/
    python3 generate_jobs.py --scheduler default --out jobs/default/

    # 分别压测两个调度器（每次测完等集群清空再测另一个）
    python3 benchmark.py --jobs-dir jobs/kubegpu/ --scheduler kubegpu --out results/kubegpu.json
    python3 benchmark.py --jobs-dir jobs/default/ --scheduler default --out results/default.json

    # 对比结果
    python3 benchmark.py --compare results/kubegpu.json results/default.json

依赖：
    pip install kubernetes
"""

import argparse
import json
import os
import statistics
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

try:
    from kubernetes import client, config
    HAS_K8S_SDK = True
except ImportError:
    HAS_K8S_SDK = False
    print("警告：未安装 kubernetes SDK，将用 kubectl 命令替代")
    print("  pip install kubernetes")


# ── 配置 ──────────────────────────────────────────────────────────────────────

NAMESPACE = "default"
POLL_INTERVAL = 2        # 秒，检查 Pod 状态的间隔
TIMEOUT = 120            # 秒，单个 job 的最大等待时间
SETTLE_WAIT = 5          # 秒，所有 job 提交后等待集群稳定的时间


# ── K8s 客户端初始化 ──────────────────────────────────────────────────────────

def init_k8s():
    if not HAS_K8S_SDK:
        return None, None
    config.load_kube_config()
    core = client.CoreV1Api()
    custom = client.CustomObjectsApi()
    return core, custom


# ── GPUJob 提交 ────────────────────────────────────────────────────────────────

def apply_jobs(jobs_dir: str) -> dict[str, float]:
    """
    批量 apply jobs_dir 下所有 YAML，记录每个 job 的提交时间戳。
    返回 {job_name: submit_timestamp}
    """
    yaml_files = sorted(Path(jobs_dir).glob("*.yaml"))
    if not yaml_files:
        raise ValueError(f"在 {jobs_dir} 下没有找到任何 YAML 文件")

    submit_times = {}
    for yaml_file in yaml_files:
        job_name = yaml_file.stem  # 文件名去掉 .yaml 就是 job name
        result = subprocess.run(
            ["kubectl", "apply", "-f", str(yaml_file)],
            capture_output=True, text=True
        )
        if result.returncode != 0:
            print(f"  ✗ apply {job_name} 失败: {result.stderr.strip()}")
            continue
        submit_times[job_name] = time.time()
        print(f"  ✓ 提交 {job_name}")

    return submit_times


def delete_jobs(jobs_dir: str):
    """清理压测产生的所有 job"""
    subprocess.run(
        ["kubectl", "delete", "-f", jobs_dir, "--ignore-not-found"],
        capture_output=True
    )


# ── 调度延迟测量 ───────────────────────────────────────────────────────────────

def get_pods_for_job(core_api, job_name: str) -> list:
    """获取某个 GPUJob 下的所有 Pod"""
    if core_api is None:
        # fallback: 用 kubectl
        result = subprocess.run(
            ["kubectl", "get", "pods", "-n", NAMESPACE,
             "-l", f"gpu-job-name={job_name}",
             "-o", "json"],
            capture_output=True, text=True
        )
        if result.returncode != 0:
            return []
        data = json.loads(result.stdout)
        return data.get("items", [])

    pod_list = core_api.list_namespaced_pod(
        namespace=NAMESPACE,
        label_selector=f"gpu-job-name={job_name}"
    )
    return [p.to_dict() for p in pod_list.items]


def all_pods_running(pods: list, expected_count: int) -> bool:
    """判断所有 Pod 是否都进入 Running 状态"""
    if len(pods) < expected_count:
        return False
    return all(
        p.get("status", {}).get("phase") == "Running"
        for p in pods
    )


def measure_latencies(core_api, submit_times: dict[str, float],
                      gang_sizes: dict[str, int]) -> list[dict]:
    """
    轮询等待每个 job 的 Pod 全部 Running，记录延迟。
    返回每个 job 的测量结果列表。
    """
    results = []
    pending = dict(submit_times)  # 还没完成的 job
    deadline = time.time() + TIMEOUT

    print(f"\n等待 {len(pending)} 个 job 的 Pod 进入 Running...")

    while pending and time.time() < deadline:
        time.sleep(POLL_INTERVAL)
        done = []
        for job_name, submit_t in pending.items():
            expected = gang_sizes.get(job_name, 1)
            pods = get_pods_for_job(core_api, job_name)
            if all_pods_running(pods, expected):
                latency = time.time() - submit_t
                results.append({
                    "jobName": job_name,
                    "gangSize": expected,
                    "latencySeconds": round(latency, 3),
                    "status": "Running",
                })
                print(f"  ✓ {job_name}: {latency:.2f}s")
                done.append(job_name)

        for name in done:
            del pending[name]

    # 超时的 job 记录为失败
    for job_name, submit_t in pending.items():
        elapsed = time.time() - submit_t
        results.append({
            "jobName": job_name,
            "gangSize": gang_sizes.get(job_name, 1),
            "latencySeconds": None,
            "status": "Timeout",
            "elapsedSeconds": round(elapsed, 3),
        })
        print(f"  ✗ {job_name}: 超时 ({elapsed:.0f}s)")

    return results


# ── 统计计算 ──────────────────────────────────────────────────────────────────

def compute_stats(results: list[dict]) -> dict:
    """计算 P50/P95/P99 和成功率"""
    latencies = [r["latencySeconds"] for r in results
                 if r["latencySeconds"] is not None]
    total = len(results)
    success = len(latencies)

    if not latencies:
        return {
            "count": total,
            "success": 0,
            "successRate": 0.0,
            "p50": None, "p95": None, "p99": None,
            "min": None, "max": None, "mean": None,
        }

    latencies.sort()
    return {
        "count": total,
        "success": success,
        "successRate": round(success / total * 100, 1),
        "p50":  round(statistics.median(latencies), 3),
        "p95":  round(latencies[int(len(latencies) * 0.95)], 3),
        "p99":  round(latencies[int(len(latencies) * 0.99)], 3),
        "min":  round(min(latencies), 3),
        "max":  round(max(latencies), 3),
        "mean": round(statistics.mean(latencies), 3),
    }


def compute_stats_by_type(results: list[dict]) -> dict:
    """按 job 类型（small/medium/large）分别统计"""
    groups = {}
    for r in results:
        # job name 格式：{type}-{scheduler}-{idx}，取第一段作为类型
        job_type = r["jobName"].split("-")[0]
        groups.setdefault(job_type, []).append(r)
    return {t: compute_stats(rs) for t, rs in groups.items()}


# ── 主流程 ────────────────────────────────────────────────────────────────────

def run_benchmark(jobs_dir: str, scheduler: str, out_file: str):
    core_api, _ = init_k8s()

    print(f"\n{'='*60}")
    print(f"压测开始: {scheduler} 调度器")
    print(f"Job 目录: {jobs_dir}")
    print(f"{'='*60}\n")

    # 1. 提交所有 job
    print("提交 GPUJob...")
    submit_times = apply_jobs(jobs_dir)
    if not submit_times:
        print("没有成功提交任何 job，退出")
        return

    # 从文件名推断 gangSize（格式: {type}-{scheduler}-{idx}.yaml）
    gang_size_map = {}
    type_to_gang = {"small": 1, "medium": 2, "large": 3}
    for name in submit_times:
        job_type = name.split("-")[0]
        gang_size_map[name] = type_to_gang.get(job_type, 1)

    print(f"\n共提交 {len(submit_times)} 个 job，等待 {SETTLE_WAIT}s 后开始监控...")
    time.sleep(SETTLE_WAIT)

    # 2. 测量调度延迟
    job_results = measure_latencies(core_api, submit_times, gang_size_map)

    # 3. 计算统计数据
    overall_stats = compute_stats(job_results)
    by_type_stats = compute_stats_by_type(job_results)

    # 4. 组装报告
    report = {
        "scheduler": scheduler,
        "benchmarkTime": datetime.now(timezone.utc).isoformat(),
        "jobCount": len(submit_times),
        "overallStats": overall_stats,
        "statsByType": by_type_stats,
        "rawResults": job_results,
    }

    # 5. 写 JSON 文件
    os.makedirs(os.path.dirname(out_file) or ".", exist_ok=True)
    with open(out_file, "w") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    # 6. 打印摘要
    print(f"\n{'='*60}")
    print(f"结果摘要: {scheduler}")
    print(f"{'='*60}")
    s = overall_stats
    print(f"  总 job 数:  {s['count']}")
    print(f"  成功调度:   {s['success']} ({s['successRate']}%)")
    print(f"  P50 延迟:   {s['p50']}s")
    print(f"  P95 延迟:   {s['p95']}s")
    print(f"  P99 延迟:   {s['p99']}s")
    print(f"  最小延迟:   {s['min']}s")
    print(f"  最大延迟:   {s['max']}s")
    print(f"\n分类统计:")
    for t, ts in by_type_stats.items():
        print(f"  {t:8s}: P50={ts['p50']}s  P95={ts['p95']}s  成功率={ts['successRate']}%")
    print(f"\n结果已写入: {out_file}")

    # 7. 清理
    print("\n清理测试 job...")
    delete_jobs(jobs_dir)
    print("清理完成")


def compare_results(file_a: str, file_b: str):
    """对比两个 benchmark 结果"""
    with open(file_a) as f:
        a = json.load(f)
    with open(file_b) as f:
        b = json.load(f)

    print(f"\n{'='*60}")
    print(f"调度器对比")
    print(f"{'='*60}")
    print(f"{'指标':<20} {a['scheduler']:>15} {b['scheduler']:>15} {'差值':>12}")
    print(f"{'-'*62}")

    def fmt(v):
        return f"{v:.3f}s" if v is not None else "N/A"

    def diff(va, vb):
        if va is None or vb is None:
            return "N/A"
        d = vb - va
        sign = "+" if d > 0 else ""
        return f"{sign}{d:.3f}s"

    metrics = [
        ("P50 延迟",     "p50"),
        ("P95 延迟",     "p95"),
        ("P99 延迟",     "p99"),
        ("平均延迟",     "mean"),
        ("最大延迟",     "max"),
    ]
    for label, key in metrics:
        va = a["overallStats"][key]
        vb = b["overallStats"][key]
        print(f"  {label:<18} {fmt(va):>15} {fmt(vb):>15} {diff(va, vb):>12}")

    print(f"\n  {'成功率':<18} "
          f"{a['overallStats']['successRate']:>14}% "
          f"{b['overallStats']['successRate']:>14}%")

    print(f"\n分类对比（P50）:")
    all_types = set(a["statsByType"]) | set(b["statsByType"])
    for t in sorted(all_types):
        va = a["statsByType"].get(t, {}).get("p50")
        vb = b["statsByType"].get(t, {}).get("p50")
        print(f"  {t:<10} {fmt(va):>15} {fmt(vb):>15} {diff(va, vb):>12}")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="KubeGPU 调度器压测工具")
    sub = parser.add_subparsers(dest="cmd")

    # run 子命令
    run_p = sub.add_parser("run", help="运行压测")
    run_p.add_argument("--jobs-dir", required=True, help="GPUJob YAML 目录")
    run_p.add_argument("--scheduler", required=True,
                       choices=["kubegpu", "default"], help="调度器标识")
    run_p.add_argument("--out", required=True, help="结果 JSON 输出路径")

    # compare 子命令
    cmp_p = sub.add_parser("compare", help="对比两个结果")
    cmp_p.add_argument("files", nargs=2, metavar="FILE",
                       help="两个结果 JSON 文件")

    args = parser.parse_args()

    if args.cmd == "run":
        run_benchmark(args.jobs_dir, args.scheduler, args.out)
    elif args.cmd == "compare":
        compare_results(args.files[0], args.files[1])
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
