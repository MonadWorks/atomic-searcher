from dataclasses import dataclass

from .core import USDC, WETH, check
from .math import ceildiv


@dataclass(frozen=True)
class CostBudget:
    gas_limit: int
    max_fee_per_gas: int
    priority_fee: int
    l1_fee_allowance: int
    total_fee_wei: int
    settlement_cost_raw: int


def quote_profit_floor(config, settlement, fee_in_settlement=0):
    """The quote-stage portion of the executor's unchanged net-profit policy."""
    ratio = config['policy'].get('min_net_profit_fee_bps', 0)
    check(isinstance(ratio, int) and ratio >= 0 and fee_in_settlement >= 0, 'invalid profit-to-fee floor')
    floor = max(int(settlement['min_net_raw']), (fee_in_settlement*ratio+9999)//10000)
    if settlement['address'] == USDC:
        floor = max(floor, int(config['policy'].get('min_net_profit_usdc_raw', 0)))
    return floor


def cost_budget(policy, settlement, basefee, hops, crossings=0, priority_fee=None):
    priority = policy["min_priority_fee_wei"] if priority_fee is None else priority_fee
    check(priority >= 0 and basefee >= 0, "invalid fee")
    gas_limit = policy["gas_overhead"] + hops*policy["gas_per_hop"] + crossings*policy["gas_per_tick_crossing"]
    check(gas_limit <= policy["max_gas"], "gas budget exceeded")
    max_fee = 2*basefee + priority
    # A unit price says nothing about the total liability of this route.
    # Bound gas * price + L1 charges, then require sufficient net profit.
    l1 = int(policy["l1_fee_allowance_wei"])
    total = gas_limit*max_fee + l1
    check(total <= int(policy["max_total_fee_wei"]), "per-attempt fee ceiling exceeded")
    if settlement == WETH:
        cost = total
    elif settlement == USDC:
        cost = ceildiv(total * policy["eth_usdc_ceiling"], 10**12)
    else:
        raise ValueError("unsupported settlement asset")
    return CostBudget(gas_limit, max_fee, priority, l1, total, cost)


def expected_value(gross_surplus, success_fee, failure_fee, win_probability_ppm):
    check(0 <= win_probability_ppm <= 1_000_000, "invalid win probability")
    # All units must be the same settlement asset. No invented default win rate.
    return (win_probability_ppm*(gross_surplus-success_fee) -
            (1_000_000-win_probability_ppm)*failure_fee) // 1_000_000


def competitive_bid(policy, settlement, context, gross, gas, basefee, minimum_priority, absolute_floor):
    """Bound a recent observed tip by profit retention AND existing loss caps.

    The observed percentile is a price reference, never a win-rate estimate.
    No guessed probability, own capital increase or uncapped priority auction.
    """
    observed = int(context.get('observed_priority_fee', minimum_priority))
    target = max(minimum_priority, (observed*110+99)//100)
    retain_ratio = int(policy.get('min_net_profit_fee_bps', 0))
    allowed = min(gross*int(policy['bid_profit_share_bps'])//10_000,
                  max(0, gross-absolute_floor), gross*10_000//(10_000+retain_ratio))
    if settlement == WETH:
        native = allowed
    else:
        check(settlement == USDC and context['usdc_at_native_cap'] > 0, 'USDC bid conversion unavailable')
        native = min(allowed*context['native_cap']//context['usdc_at_native_cap'],
                     allowed*10**12//policy['eth_usdc_ceiling'])
    native = min(native, int(policy['max_total_fee_wei']), context['balance'])
    fee_ceiling = max(0, native-context['extra_fee'])//gas
    priority = min(target, max(0, fee_ceiling-2*basefee))
    # The ordinary floor is checked by execution's complete cost guards.
    priority = max(minimum_priority, priority)
    return priority, {'observed_priority_fee_wei':observed, 'target_priority_fee_wei':target,
                      'selected_priority_fee_wei':priority, 'profit_fee_budget_raw':allowed,
                      'maximum_affordable_fee_per_gas_wei':fee_ceiling,
                      'maximum_native_fee_budget_wei':native,
                      'scope':'recent included transaction fee percentile; not an inclusion guarantee'}
