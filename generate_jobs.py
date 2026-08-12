#!/usr/bin/env python3
import argparse
import os
import textwrap

# 专为 8GB 物理显存定制的混部规格 (总盘子 8192Mi)
# (type_name, 比例权重, gangSize, vramPerGPU, priority)
JOB_TYPES = [
    ("small",  0.50, 1, "1024Mi",  110),  # 占 50%，高优先级，单个消耗 512Mi
    ("medium", 0.40, 2, "2048Mi",  50),  # 占 40%，中优先级，成组消耗 2048Mi (2Gi)
    ("large",  0.10, 2, "4096Mi",  10),  # 占 10%，低优先级，成组消耗 4096Mi (4Gi)
]

SCHEDULER_MAP = {
    "kubegpu": "my-gpu-scheduler",
    "default": "default-scheduler",
}

def make_job_yaml(name: str, gang_size: int, vram: str, priority: int, scheduler_key: str) -> str:
    real_scheduler = SCHEDULER_MAP[scheduler_key]
    
    return textwrap.dedent(f"""\
        apiVersion: scheduler.lawson.com/v1alpha1
        kind: GPUJob
        metadata:
          name: {name}
          namespace: default
          labels:
            benchmark/scheduler: {scheduler_key}
            benchmark/type: {name.split('-')[0]}
        spec:
          gangSize: {gang_size}
          gpuCount: {gang_size}
          priority: {priority}
          vramPerGPU: "{vram}"
          schedulerName: "{real_scheduler}"
    """)

def main():
    parser = argparse.ArgumentParser(description="生成压测 GPUJob YAML (8GB 显存轻量版)")
    parser.add_argument("--scheduler", choices=["kubegpu", "default"], required=True, help="调度器类型")
    parser.add_argument("--count", type=int, default=24, help="总 job 数量（默认 24）")
    parser.add_argument("--out", default="jobs/", help="输出目录（默认 jobs/）")
    args = parser.parse_args()

    os.makedirs(args.out, exist_ok=True)

    generated = 0
    idx = 0

    for type_name, ratio, gang_size, vram, priority in JOB_TYPES:
        if type_name == JOB_TYPES[-1][0]:
            type_count = args.count - generated
        else:
            type_count = int(args.count * ratio)

        for i in range(type_count):
            if generated >= args.count:
                break

            name = f"{type_name}-{args.scheduler}-{idx:03d}"
            yaml_content = make_job_yaml(
                name=name,
                gang_size=gang_size,
                vram=vram,
                priority=priority,
                scheduler_key=args.scheduler,
            )
            path = os.path.join(args.out, f"{name}.yaml")
            with open(path, "w") as f:
                f.write(yaml_content)

            idx += 1
            generated += 1

    print(f"【成功】已针对 8GB 显存优化，按比例混合生成了 {generated} 个 GPUJob YAML 到 {args.out}")
    print(f"目标调度器: {SCHEDULER_MAP[args.scheduler]}")

if __name__ == "__main__":
    main()
