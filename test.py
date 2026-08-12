import io
import unittest
from typing import Tuple

# ============================================================================
# 全局常量定义 (对齐 Go 的全局配置)
# ============================================================================
MIN_HIT_RATE_DELTA = 0.05  # 假设的 Delta 抑制阈值（如 5% 变化才上报）


# ============================================================================
# 业务逻辑实现 (The Core Logic)
# ============================================================================

class CacheMonitor:
    """对应 Go 的 CacheMonitor 结构体，维护缓存命中率的历史状态"""
    def __init__(self):
        self.last_hit_rate: float = 0.0
        self.has_reported: bool = False  # 🌟 核心修复点：使用明确的布尔值区分“首次上报”与“命中率为 0”


def parse_vllm_hit_rate(reader: io.StringIO) -> Tuple[float, bool]:
    """
    解析 vLLM 暴露的 Prometheus 文本格式指标，计算 KV Cache 命中率。
    返回: (hit_rate, is_success) -> 用 bool 代替 Go 的 error 机制进行流控
    """
    hit_tokens = 0.0
    total_tokens = 0.0
    has_seen_any_metric = False
    
    lines = reader.read().splitlines()
    
    for line in lines:
        line = line.strip()
        # 跳过注释行和空行
        if not line or line.startswith("#"):
            continue
            
        # 简单解析 Prometheus 行: "metric_name{labels} value" 或 "metric_name value"
        parts = line.split()
        if len(parts) != 2:
            # 遭遇非合法 Prometheus 文本格式（例如 HTML 报错网页）
            return 0.0, False
            
        metric_name_with_labels, val_str = parts[0], parts[1]
        
        # 提取指标名，去掉可能存在的大括号标签
        metric_name = metric_name_with_labels.split("{")[0]
        
        try:
            val = float(val_str)
        except ValueError:
            return 0.0, False

        # 🌟 关键增强：使用后缀匹配（Suffix Match）兼容各种前缀变体，防止指标名静默解析失败
        if metric_name.endswith("prompt_tokens_hit_total"):
            hit_tokens = val
            has_seen_any_metric = True
        elif metric_name.endswith("prompt_tokens_total"):
            total_tokens = val
            has_seen_any_metric = True

    # 边界情况 1：完全没抓到相关指标（旧版本或刮错端点），视作无流量冷启动，不触发崩溃
    if not has_seen_any_metric:
        return 0.0, True

    # 边界情况 2：总流量为 0（实例刚启动，尚未处理任何请求），防止除以零崩溃
    if total_tokens == 0.0:
        return 0.0, True

    return hit_tokens / total_tokens, True


# ============================================================================
# 单元测试部分 (The Go Test Porting)
# ============================================================================

class TestParseVllmHitRate(unittest.TestCase):

    def test_parse_vllm_hit_rate_normal_case(self):
        input_data = (
            "\n"
            "# HELP vllm:prompt_tokens_hit_total Prefix cache hits\n"
            "# TYPE vllm:prompt_tokens_hit_total counter\n"
            "vllm:prompt_tokens_hit_total{model=\"llama-3\"} 896\n"
            "# HELP vllm:prompt_tokens_total Total prompt tokens\n"
            "# TYPE vllm:prompt_tokens_total counter\n"
            "vllm:prompt_tokens_total{model=\"llama-3\"} 1000\n"
        )
        rate, success = parse_vllm_hit_rate(io.StringIO(input_data))
        self.assertTrue(success, "应该成功解析正常数据")
        
        # 浮点数断言，防精度丢失误差（对应 Go 的 1e-9 差值断言）
        self.assertAlmostEqual(rate, 0.896, delta=1e-9)

    def test_parse_vllm_hit_rate_zero_traffic(self):
        # vLLM 刚启动，无任何流量，双计数器为 0
        input_data = (
            "\n"
            "vllm:prompt_tokens_hit_total{model=\"llama-3\"} 0\n"
            "vllm:prompt_tokens_total{model=\"llama-3\"} 0\n"
        )
        rate, success = parse_vllm_hit_rate(io.StringIO(input_data))
        self.assertTrue(success)
        self.assertEqual(rate, 0.0, "零流量时命中率应当安全返回 0")

    def test_parse_vllm_hit_rate_missing_metrics(self):
        # 目标指标缺失，侧车应当优雅返回 0 视作无信号，禁止 Crash 报错
        input_data = (
            "\n"
            "# HELP some_other_metric Unrelated\n"
            "some_other_metric 42\n"
        )
        rate, success = parse_vllm_hit_rate(io.StringIO(input_data))
        self.assertTrue(success, "目标指标缺失时不应该抛错")
        self.assertEqual(rate, 0.0, "指标缺失时默认命中率为 0")

    def test_parse_vllm_hit_rate_malformed_prometheus_text(self):
        # 刮错端点返回了 HTML 网页，必须明确拦截并返回失败状态
        input_data = "<html><body>404 Not Found</body></html>"
        _, success = parse_vllm_hit_rate(io.StringIO(input_data))
        self.assertFalse(success, "面对非 Prometheus 文本格式应该报错拦截")

    def test_parse_vllm_hit_rate_metric_name_prefix_variants(self):
        # 验证后缀匹配看门狗：前缀变化时依然能够精准提取出数据
        input_data = (
            "\n"
            "my_custom_prefix_prompt_tokens_hit_total 450\n"
            "my_custom_prefix_prompt_tokens_total 500\n"
        )
        rate, success = parse_vllm_hit_rate(io.StringIO(input_data))
        self.assertTrue(success)
        self.assertAlmostEqual(rate, 0.9, delta=1e-9)


class TestCacheMonitorDeltaSuppression(unittest.TestCase):

    def test_cache_monitor_delta_suppression_distinguishes_zero_from_never_reported(self):
        cm = CacheMonitor()

        # 模拟历史状态：曾经上报过，且上一次的值刚好是 0.0（冷启动完成状态）
        cm.last_hit_rate = 0.0
        cm.has_reported = True

        # 新一轮采集到的值依然是 0.0
        new_rate = 0.0
        
        # 判定是否需要跳过更新（Delta 抑制检查机制）
        should_skip = cm.has_reported and abs(cm.last_hit_rate - new_rate) < MIN_HIT_RATE_DELTA
        
        # 🌟 核心断言：因为有 has_reported 护航，重复的 0 必须能够被成功抑制，阻止无意义的集群频繁 Patch
        self.assertTrue(should_skip, "重复的 0 读数应该被成功抑制跳过")

    def test_cache_monitor_delta_suppression_first_report_never_skipped(self):
        cm = CacheMonitor() # has_reported 默认为 False，代表盘古开天辟地第一次上报

        new_rate = 0.0
        
        should_skip = cm.has_reported and abs(cm.last_hit_rate - new_rate) < MIN_HIT_RATE_DELTA
        
        # 🌟 核心断言：无论读数是多少（即使是 0），开局第一次数据绝对不能被抑制，必须放行 Patch
        self.assertFalse(should_skip, "集群初次运行的报告绝对不允许被静默抑制")


if __name__ == "__main__":
    unittest.main()
