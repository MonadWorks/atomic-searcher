# Security / 安全说明

This is a research prototype. Its publication review checks the exported source
and history for disclosure; it is **not a professional contract audit** or a
guarantee that using the software with funds is safe.

## Publication boundary

- Only curated source, public configuration and reviewed test fixtures are tracked.
- Wallets, encrypted keystores, private keys, seed phrases, provider credentials,
  account journals, raw signed transactions and personal deployment metadata are
  excluded. Build products, runtime data and old releases are ignored.
- The default owner is a dummy address. Financial commands reject it before wallet
  access. No default beneficiary or personal executor deployment is supplied.
- Atlas participation defaults to disabled with a zero bond target and no
  additional-deposit authorization. Configure auction funding separately.
- Git commits use the project's contributor identity. The original local history
  is not imported.
- Gitleaks scans content and history. Its only project allowlist covers complete
  **20-byte public EVM addresses**, not private keys or whole test directories.

## Working with credentials

Put local settings in ignored `config/local.json` or `config/node.local.json`.
Provide secrets through the local environment or hidden terminal input. Never put
a private key or seed phrase in an issue, command-line argument, README or fixture.
An encrypted keystore is still private: do not commit it.

Before publishing changes, inspect `git diff --cached`, run
`python tools/check_publication.py`, and run Gitleaks on both the working tree and
Git history. Review changed binary or encoded data explicitly. Redaction in a UI
does not remove an underlying file or a previous Git commit.

`launch`, `setup-executor` and `trade` can spend funds. Use a dedicated account and
review current contracts, endpoints and fee limits before any live operation.
Keep account journals when cleaning generated data: they are required for
transaction recovery and reconciliation.

The observation setting `policy.mode = "shadow"` does not override an explicit
financial command. Treat the command, owner, wallet and enabled deployment/funding
options as the live-operation boundary.

## Reporting

For a potential secret disclosure, use the repository's **Security → Report a
vulnerability** facility when enabled. Do not paste the credential in a public
issue. If a credential was published, revoke or rotate it; deleting the current
file alone does not remove exposure from Git history or copies.

## 中文

此仓库是研究原型。开源前的泄露检查针对导出的文件和提交历史，不等于专业合约审计。

个人钱包、加密钱包文件、私钥、助记词、节点密钥、交易账本、原始签名交易和个人部署资料
都不属于公开内容。默认 owner 是示例地址，涉及钱包和资金的命令会先拒绝它。
Atlas 拍卖默认关闭、押金目标为零，不附带追加押金授权。`shadow` 配置不会拦截显式交易命令，
资金边界由命令、账户、钱包以及启用的部署和押金选项共同决定。

个人设置使用被忽略的 `config/local.json` 或 `config/node.local.json`；密码只在本机隐藏输入。
不要把凭证写入命令参数、issue、文档或测试夹具。清理缓存时保留交易账本，以便恢复和对账。

发布前检查暂存内容，运行仓库检查和 Gitleaks，并同时检查提交历史。发现凭证泄露后应撤销
或轮换，单纯删掉当前文件无法清除历史副本。安全报告应通过仓库的私密漏洞报告入口提交。
