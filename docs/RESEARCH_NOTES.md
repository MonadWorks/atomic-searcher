# Research record / 研究记录

## Outcome

The working hypothesis was that callback-financed atomic routes could capture
short-lived dislocations on Base without holding the full trade principal.
The implementation exercised discovery, native quotes, whole-cycle probes,
deployed-executor checks, transaction submission and reconciliation.

The retained financial journal at publication had **no verified profitable
settlement**. Some simulated routes returned positive gross surplus. They are not
reported here as realized returns, reproducible historical winnings or a projected
income stream. Some recorded attempts were cancelled; a historical failed attempt
incurred a fee. This release contains no account journal or operator transaction
identifiers.

## Findings that influenced the implementation

### Receiving a block is not seeing its opportunity in time

Historical HTTP batches sometimes delivered both the event that changed a price
and a later trade that consumed the resulting discrepancy. Event order in such a
batch does not establish when the searcher could have known or acted. The later
implementation separates fresh activity from historical recovery and preserves
prepared-route push priority.

### Quotes and executability diverged

Token transfer deductions, dynamic fees, missing depth, pool-specific behavior and
state changes could invalidate a nominal price spread. Read-only native quotes
filter candidates; whole-cycle probes model the financed trade and its callbacks;
the final call checks the deployed executor's actual transaction parameters.

### Local work can spend the entire opportunity lifetime

Extra pool-fee requests, repeated gas trials, broad screening before prepared
routes and periodic dispatch sleeps introduced avoidable delay. Reuse and direct
handoff removed specific waits. A later live sample reached the final deployed
check after a positive whole-cycle quote, but that check reverted. Its subsequent
quote was negative. The final RPC response did not identify the exact revert
cause, so that record does not prove a particular competitor won.

### More strategies do not establish an economic advantage

Liquidation, conversion, auction and venue adapters expanded the implementation.
Coverage alone did not demonstrate that the searcher could receive a useful
signal, construct the right transaction and win inclusion with positive net
returns under the available infrastructure.

### Iteration artifacts became an operational cost

Old session snapshots, historical RPC caches and copied local releases grew far
larger than the source. The public release excludes them. Regression fixtures are
small, inspectable inputs; two formerly compressed fixture blobs were expanded to
JSON so reviewers can inspect their contents. Raw account and deployment records
remain outside the repository.

## What local tests establish

- Arithmetic, route indexing and selected protocol/venue behaviors.
- Expected rejection of stale, unprofitable or invalid candidates.
- Callback execution, signing and receipt/recovery paths on synthetic local chains.
- Isolation of accounting and local lifecycle operations.

They do not establish current provider performance, universal protocol support,
mainnet inclusion probability or future net income. A meaningful next trading
experiment would need timestamped first-seen evidence, a complete executable
transaction, actual inclusion and independently reconciled net proceeds.

## 中文记录

最初的假设是：用回调融资或闪电流动性，在不持有整笔本金的情况下捕获 Base 上短暂的价差。
程序实现了发现、报价、完整闭环检查、实际执行器预检、提交与账务恢复。

发布时保留的账本没有已验证的盈利结算。正的模拟毛利没有被记作真实收入；被取消的尝试与
实际消耗手续费的失败也分开记录。公开仓库不包含个人账本或操作者交易标识。

关键经验是：必须知道信息何时真正可得；不能把对手已经成交后的状态当成可捕获的机会；
整笔交易的转账和回调行为必须纳入检查；每次网络往返都会消耗机会寿命；增加策略数量本身
并不构成竞争优势。后期修复消除了一些具体等待，但仍未证明持续盈利能力。

这份仓库保留实现和可复现的回归测试，删除原始采样、旧版本副本和个人部署资料。
本地测试证明的是合成环境中的程序行为，真正的经济验证仍需要及时发现、真实成交与净收入闭环。
