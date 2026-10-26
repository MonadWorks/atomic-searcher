"""Executor ABI kinds. V2 means authenticated Uniswap V2 at fixed 30 bps."""
KINDS = {"v3": 1, "slipstream": 2, "v2": 3}

def kind_id(name):
    return KINDS[name]
