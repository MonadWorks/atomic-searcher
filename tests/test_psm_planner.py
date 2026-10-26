from searcher.psm_planner import proposals
from eth_abi import decode

def state(price_numerator=1,price_denominator=1,liquidity=10**18,usdc=10**12,susds=10**24):
    from math import isqrt
    # Conversion of raw sUSDS to raw USDC at one dollar per token.
    sqrt=isqrt((1<<192)*price_numerator//(10**12*price_denominator))
    return (sqrt,500,liquidity,10**27,usdc,susds)

def test_both_directions_and_later_pool_are_compared():
    pools=['0x'+'11'*20,'0x'+'22'*20]
    rows=proposals(pools,[state(99,100),state(102,100)],10**12,100000)
    assert rows and rows[0][0].pools==(pools[1],)
    assert {decode(['address','bool'],route.arguments)[1] for route,_,_ in rows}=={True,False}
    assert all(amount<=10**12 for _,amount,_ in rows)

def test_small_inventory_and_bad_pool_do_not_poison_valid_pool():
    pools=['0x'+'11'*20,'0x'+'22'*20,'0x'+'33'*20]
    rows=proposals(pools,[(0,0,0,0,0,0),state(99,100,usdc=1),state(102,100)],10**10,100000)
    assert rows and all(route.pools==(pools[2],) for route,_,_ in rows)

def test_no_fee_adjusted_edge_has_no_proposals():
    assert proposals(['0x'+'11'*20],[state()],10**12,100000)==[]


def test_reverse_exact_output_size_stays_below_pool_reserve():
    row=state(4,1,liquidity=10**15)
    choices=proposals(['0x'+'11'*20],[row],10**30,1)
    sqrt,fee,liquidity,rate,_,_=row;reserve=liquidity*sqrt//(1<<96)
    # Exact-output profit is zero at the depletion boundary. The optimum for
    # a fourfold edge is close to half the virtual output reserve, not all of it.
    assert choices and all(decode(['address','bool'],r.arguments)[1] for r,_,_ in choices)
    assert reserve*49//100<choices[0][1]<reserve*51//100
