# 分段入湖作业：受管载荷容量发布

## 已有业务和环境
Python 3.9+标准库与SQLite；macOS 13+普通登录用户，提供可执行Python的绝对路径，不捆绑解释器，不需管理员权限、在线账号或外部服务。路径可能含空格和中文，运行目录与源码目录不同；上线验收会移走源码。不使用Docker或Compose。

pipeline.py中的validate、normalize、merge、summarize、encoded、digest是已有业务核，保留行为。Engine、LocalBlobs及运维编排可调整并增加运维表；jobs现有列、检查点形状和<sha256>.json载荷内容保持兼容。数据库留在ROOT/state.sqlite，载荷留在ROOT/blobs；不得把载荷移到日志或其他目录逃避计量。manual.py是手工核账入口；正式发布通过安装后的app.pyz运行，不重写业务计算。

输入为UTF-8 JSON对象：{"batch":"批次ID","segments":[[{"id":"样本ID","rev":1,"sensor":"站点","ts":61,"value":120,"deleted":false}]]}。
每批1～16段，每段0～2000行。字符串非空，rev是正整数，ts与value是非布尔整数，deleted是布尔。每个样本取最高修订，允许换站、改时、撤销和复活；同修订异内容整批拒绝。未删除样本按(sensor,ts//60)汇总sum/count，保留负值和零。批次ID同内容重放返回原接纳回执，异内容为BATCH_CONFLICT且不改账。
samples/a.json的rows为[{"sensor":"s1","minute":-1,"sum":7,"count":1},{"sensor":"s2","minute":1,"sum":-30,"count":2}]。
samples/b.json的rows为[{"sensor":"s1","minute":0,"sum":100,"count":1}]。
samples/shared.json只含a第一段按id排序后的两行，完成rows为[{"sensor":"s1","minute":0,"sum":140,"count":2}]。
read列表按batch排序，每项含batch、rows、receipt；完成回执为{"batch":ID,"sha256":规范输入摘要,"groups":分组数,"samples":未删除样本数}。结果与完成回执一起提交，未完成批次不出现在read中。接纳回执为{"batch":ID,"sha256":规范输入摘要}，完成后重投仍相同。

## 已有发布入口
源码入口python3 ops.py；成功输出一个JSON值，错误非零退出并说明原因。路径分别作为单独argv元素传递：
- build OUT：生成OUT/app.pyz和manifest.json，清单为{"sha256":"app.pyz的SHA-256"}，输出{"package":OUT绝对路径}。同源码重复构建字节相同。
- install PACKAGE DEST --python ABS [--no-load]：校验后安装DEST/app.pyz，以DEST/data为ROOT，返回{"app":绝对路径,"root":绝对路径,"plist":绝对路径,"label":标签}。坏包不得替换已安装程序或启动配置。安装时没有作业进程，不涉及运行中升级。
- submit ROOT INPUT：读取输入文件并接纳，返回接纳回执。
- run ROOT --once：执行一次Engine.step，返回{"progressed":布尔}；run ROOT为常驻循环。worker.lock拒绝第二个加工者，提交者不占该长驻锁。
- read ROOT：返回已完成批次；status ROOT：保留pending、completed、required、inventory，inventory为已发布摘要到内容字节数的映射。

安装入口为ABS DEST/app.pyz，支持install、submit、run、read、status；本题增加quota。运行不能依赖源码、当前目录、PYTHONPATH或shell激活。新增模块须纳入ops.py的CORE打包清单。默认install把RunAtLoad与KeepAlive的plist写入用户Library/LaunchAgents，并用launchctl bootstrap gui/<uid>加载；ProgramArguments为[ABS,安装app.pyz绝对路径,"run",ROOT绝对路径]，日志留在DEST。--no-load只生成DEST/launch-agent.plist。agentcheck.py的--live核对真实launchctl状态。现有releasecheck.py仅验证基础发布和启动配置，不包含配额或真实LaunchAgent上线验证。

## 本轮容量契约
仅限定一个工作者和一个提交者，可以同时查询状态、提高额度。先在尚无载荷和待办的新ROOT执行quota ROOT BYTES；BYTES为正整数字节数。未配置时保持原业务行为，status的limit为null；配置后不能撤销。首次配置后只允许相同值重试或提高，下降或非法值须拒绝且不改原配置。quota返回与status相同的对象。后台已启动但尚未接纳数据不妨碍首次配置。

status增加limit、published_bytes、reserved_bytes；pending每项增加pause_reason，未暂停为null，配额暂停为"QUOTA_PAUSED"，且暂停原因在重开后仍可见。published_bytes是所有已发布对象内容的去重字节和，包括尚未入账或已不再被检查点引用的对象；本题不删除已发布历史对象。reserved_bytes是尚未转为已发布占用的完整对象预留量，不能只按临时文件已写字节计量。同摘要已发布对象与其尚未结清的预留不能重复计数。status须给出相互一致的容量快照，不能等待暂停中的活写者结束才能查询或扩容。

任何载荷内容落盘前须获得完整对象大小的额度；始终满足published_bytes+reserved_bytes≤limit，真实已发布及临时载荷内容总字节也不得超过limit。SQLite、锁文件、预留元数据、日志和文件系统开销不计。相同摘要只保留一份载荷；已有对象重用不申请新容量，不得先写重复临时副本再去重。相同摘要正在写入时可等待唯一写者，发布后重用，不能仅因其预留占满容量误报额度不足或提交缺载荷的引用。不同对象的载荷I/O不能持全局协调锁，也不能把全部提交和加工串行化。

额度不足时submit非零退出并说明QUOTA_PAUSED，不提交接纳回执或jobs记录；该批可用原输入重试。多段接纳失败可留下此前已成功发布的对象，它们仍计额度。已接纳批次在需要新载荷而额度不足时，run --once返回{"progressed":false}，原检查点不变，不发布完成回执，持久记录配额暂停。常驻进程保持运行，提高额度后无需重启或人工修改状态即可继续并清除暂停原因。已接纳批次的重复提交仍返回原接纳回执，不受当前剩余额度影响；无新载荷的推进不能因额度恰满而被拒绝。非配额异常不得伪装为QUOTA_PAUSED。

进程在预留后或对象持久发布后被SIGKILL，重开须核对真实文件恢复占用：活写者的预留不得仅凭PID或超时释放，已发布对象不得漏计，死写者的孤儿临时文件须清理并恢复可用额度。清理临时文件前不得先释放其额度。恢复不要求人工删文件或改库；未响应的提交以原批次重试，结果和回执保持原语义。仅验证进程终止，不要求断电、磁盘损坏、真实文件系统满盘、降额、在线升级或已发布对象垃圾回收。

## 字节样例与观察点
encoded按UTF-8紧凑排序键序列化，额度按该内容长度计算，不按输入文件缩进计算。
- {"batch":"empty","segments":[[]]}在新ROOT的limit=1时不能接纳；提高到2后可接纳并完成。载荷[]恰为2字节，归一化与合并须重用它；完成后仍计2字节，重复接纳返回原回执。
- 接纳a的三个不同源对象分别为137、204、136字节，共477字节。第一步归一化产生不同摘要的137字节对象；limit=614恰能容纳该预留。此时b重用a第一源对象，但还需要68字节的新对象，容量不足必须拒绝接纳且不改变a检查点。扩容到4096后两批均能完成并保持原结果。
- shared的源对象恰好与a第一步归一化输出同摘要、同为137字节。在a已接纳、limit=614且该输出预留后暂停时，另一提交进程可等待同摘要写者；放行后shared应接纳，不能多占137字节或被误判为配额不足。

Gate由POINTWISE_PAUSE指定一次性事件、POINTWISE_GATE指定目录，命中后写ready.json并等待continue文件。已有before_object_publish、object_ready、before_accept、checkpoint_loaded、before_checkpoint、after_checkpoint、before_finish、after_finish。
为本题增加quota_reserved和quota_published：分别在完整预留已持久建立且尚未写入内容时、对象已校验并持久发布且put尚未返回时触发，detail含oid和bytes。观察点只规定可复现的边界，不规定元数据存储或锁实现。暂停时可以查询status、提高额度并让不同对象的另一进程尝试写入。

## 发布回归和交付
保留checks.py原断言。扩展releasecheck.py，通过安装产物核对不同摘要容量竞争、同摘要共享、预留后和发布后的真实SIGKILL恢复、孤儿临时文件清理以及扩容续跑；独立核对载荷实际字节和容量状态，检查结果、检查点及重复提交回执。不能只在源码入口验证，不能用异常模拟代替杀进程，也不能只看最终结果而漏检中间超额。

真实上线使用默认install加载用户级LaunchAgent，移走源码，从安装入口接纳a，在额度不足时记录暂停状态，提高额度后让该常驻进程独立完成，并核对read和原接纳回执。记录launchctl状态和实际核账输出，结束时bootout本次测试标签并清理测试plist；没有登录会话时如实标记未执行，--no-load不算上线通过。

交付简短操作恢复说明和可重跑实际输出。最终.pointwise-runtime.json的verify为["python3","releasecheck.py"]，demo为["python3","releasecheck.py","demo"]；demo展示真实安装结果、配额暂停和扩容后的完成结果。基线运行清单的normal/demo只证明已有业务可运行，不代表本轮配额已实现。
