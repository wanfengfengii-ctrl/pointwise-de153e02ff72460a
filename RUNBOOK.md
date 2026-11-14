# 分段入湖：受管载荷配额运行与恢复说明

## 安装（每个用户一次）

```sh
python3 /绝对路径/ops.py build /绝对路径/发布包
python3 /绝对路径/ops.py install /绝对路径/发布包 /绝对路径/安装目录 --python /绝对路径/python3
```

发布包必须与源码分离：`build` 生成自包含的 `app.pyz`（内嵌 quota.py 等 CORE 模块）
与 `manifest.json`，验收时移走源码后从安装入口运行。同源码重复构建字节一致。

`install` 校验清单哈希与包内容后才会改动安装目录；默认在
`~/Library/LaunchAgents/<label>.plist` 写入用户级自启动项（RunAtLoad + KeepAlive，
`ProgramArguments` 为 `[ABS, DEST/app.pyz, "run", ROOT]`）并 `launchctl bootstrap
gui/<uid>` 加载。`--no-load` 只写 `DEST/launch-agent.plist`，不加载、不算上线通过。
`DEST/data` 即 ROOT，数据库为 `ROOT/state.sqlite`，载荷为 `ROOT/blobs/<sha256>.json`。

## 首次设置额度（新 ROOT 必须先做）

```sh
/绝对路径/python3 DEST/app.pyz quota ROOT 614
```

`quota` 返回与 `status` 相同的对象。只允许首次设置或在原值上提高；相同值重试是允许的。
下降返回 `QUOTA_DECREASE`，非正整数返回 `QUOTA_INVALID`，首次设置低于磁盘上已有载荷
字节返回 `QUOTA_INIT_STATE`，失败都不改原配置。后台进程已启动但尚未接纳数据不影响首次设置。
未配置时 `limit` 为 `null`，保持原业务行为。

## 日常操作

```sh
DEST/app.pyz submit ROOT 批次.json   # 接纳；额度不足时非零退出并显示 QUOTA_PAUSED
DEST/app.pyz read   ROOT             # 已完成批次（行 + 完成回执）
DEST/app.pyz status ROOT             # pending/completed/required/inventory/limit/published_bytes/reserved_bytes
DEST/app.pyz run    ROOT --once      # 单步；配额暂停时输出 {"progressed": false}
DEST/app.pyz run    ROOT             # 常驻，由 LaunchAgent 拉起（重复启动报 WORKER_BUSY）
python3 manual.py <命令> ROOT ...    # 手工核账，同一 ROOT 布局，含 quota/status
```

- `published_bytes`：`blobs` 中已发布对象的去重内容字节和（含不再被检查点引用的历史对象）。
- `reserved_bytes`：已持久建立、尚未转为已发布的完整对象预留量（不是临时文件已写字节）。
- `inventory`：摘要 → 实际内容字节数，可与 `ls -l ROOT/blobs` 逐项核对。
- `pending[].pause_reason`：`QUOTA_PAUSED` 表示该批因额度暂停，重开进程后仍然可见。

## 额度不足时会发生什么

- `submit` 不写 jobs、不发接纳回执，原输入可直接重试。多段批次此前已发布的对象保留并计额度。
- 已接纳批次在 `run` 中需要新载荷而额度不足：不写检查点、不发完成回执，
  持久记录 `QUOTA_PAUSED`；进程不退出，其它能复用已有载荷的批次继续推进。
- 重复提交同一批次仍返回原接纳回执，与剩余额度无关。

## 扩容续跑

```sh
DEST/app.pyz quota ROOT 4096
```

常驻进程在下一轮自动清除暂停原因并继续，结果与完成回执与未暂停时相同；无需重启、
无需改库。暂停期间可以正常查询 `status`，也不会被活写者阻塞。

## 崩溃恢复（自动，无需人工介入）

- 预留后被杀：每个对象身份上有一把 `flock`，进程死亡时内核自动释放；重开时 `Quota`
  构造函数核对 `ROOT/blobs` 实际文件，先删除孤儿临时文件、再释放对应预留。
- 发布后被杀：已完成 `os.replace` 与目录 fsync 的对象按实际文件计入 `published_bytes`，
  不会被漏计，也不会产生重复副本。
- 暂停中的活写者持有锁，其预留不会因 PID 或超时被回收（本实现根本不看 PID/超时）。
- 未响应的提交用原批次重试即可，结果和回执语义不变。

## 交付与验收

```sh
python3 releasecheck.py        # 交付校验（verify）
python3 releasecheck.py demo   # 展示真实安装结果、暂停与扩容完成（demo）
python3 checks.py normal       # 基线业务；不证明本轮配额
python3 checks.py disk
```

`releasecheck.py` 在真实安装产物上运行，先删除源码树；真实 LaunchAgent 上线验证只在有
图形登录会话的 macOS 上执行，否则输出中如实标记 `"executed": false` 并说明原因。

```sh
DEST/app.pyz status ROOT   # 与 ls/du 对照 blobs 内容字节
```

对账要点：`published_bytes == sum(inventory.values())`；`published_bytes + reserved_bytes ≤ limit`；
`inventory` 的键即 `ROOT/blobs` 下的对象文件名（去掉 `.json`），值为实际文件字节数。
安装产物回归覆盖：不同摘要容量竞争、同摘要共享、预留后/发布后的真实 SIGKILL 恢复、
孤儿临时文件清理、常驻进程扩容续跑、随机时刻杀进程。
