"""Eligibility of transfers supported by the deployed exact-receipt executor.

Entries identify observed incompatible tokens, not estimated tax percentages.
Prices or larger gas limits cannot make this executor accept a receipt shortfall.
"""


def blocked_tokens(config, tokens):
    excluded = config.get('unsupported_transfer_tokens', {})
    return tuple(sorted({token for token in tokens if token in excluded}))


def eligible_pools(config, pools):
    return [p for p in pools if not blocked_tokens(config, (p.token0, p.token1))]
