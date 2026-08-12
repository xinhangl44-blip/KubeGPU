import torch
import time
import sys

if not torch.cuda.is_available():
    print("Error: CUDA 在当前 WSL2 环境不可用！")
    sys.exit(1)

device = torch.device("cuda")
print(f"成功连接显卡: {torch.cuda.get_device_name(0)}")

# 分配 512*1024*1024*4 字节 = 约 2GB 显存
try:
    dummy_tensor = torch.empty((512, 1024, 1024), dtype=torch.float32, device=device)
    print("🌟 成功常驻 2GB 显存！按 Ctrl+C 可以退出释放。")
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("\n显存已释放。")
