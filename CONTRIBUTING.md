# Contributing / 贡献说明

Keep each change focused on an observable behavior. Explain the failure or
limitation, the intended result and the evidence that demonstrates the change.

1. Start with a regression or a narrowly specified behavior.
2. Run `forge build`, `forge test`, and `python -m pytest -q tests`.
3. Run `python tools/check_publication.py` and Gitleaks before sharing changes.
4. Document whether evidence comes from synthetic tests, historical state,
   live read-only data, submitted transactions or canonical receipts.

Do not contribute wallet files, provider keys, operating logs, account journals or
raw signed transactions. Fixtures should contain the minimum public or synthetic
data needed for a regression, with their origin explained. Keep them readable.
Do not replace a failing financial check with a wider limit merely to make a test
pass; explain any change in execution or fee policy explicitly.

The release is organized around a curated implementation, portable validation,
and a bilingual research record. Changes should extend those purposes rather than
reintroduce local release copies or one-off production scripts.

每次提交聚焦一个可观察行为，说明问题、改动目的与验证证据。区分合成测试、历史状态、
真实网络读取、交易提交和规范链回执。测试夹具使用最小的公开或合成数据，保持可审阅；
不要提交钱包、节点凭证、运行日志、账本或原始签名交易。
