from types import SimpleNamespace
import threading
from searcher.protocol_screen import ProtocolScreen
from searcher.core import USDC,WETH,load_config
from searcher.protocol_scanners import Scanners

C='0x'+'44'*20;P='0x'+'55'*20;Q='0x'+'66'*20;COMET='0x'+'77'*20
def setup(loan=WETH,clock=lambda:1):
    a=((P,C,loan,1),);b=() if loan==USDC else ((Q,loan,USDC,1),)
    reports=[];sizes=[]
    rb=SimpleNamespace(record=lambda *x:None,ranked=lambda start,end,amount,limit:(sizes.append(amount) or [b]))
    scanner=SimpleNamespace(rpc=None,u=SimpleNamespace(route_book=rb),paths=lambda *args,**kw:[(a,b)])
    return ProtocolScreen(load_config(),lambda **v:reports.append(v),clock),scanner,a,b,reports,sizes

def test_nonusdc_dust_is_screened_by_actual_surplus_not_decimals():
    f,s,a,b,reports,sizes=setup();outputs=iter([[11],[0]])
    f.quote=lambda *args:next(outputs)
    # Unknown rounding-to-zero gets just one diagnostic full call.
    assert len(f.compound(s,COMET,C,WETH,[(10,100)]))==1
    outputs=iter([[11],[0]])
    assert f.compound(s,COMET,C,WETH,[(10,100)])==[]
    assert sizes==[1,1]

def test_tiny_principal_with_large_discount_is_not_blanket_filtered():
    f,s,a,b,reports,_=setup(USDC);f.quote=lambda *_:[2_000_001]
    assert f.compound(s,COMET,C,USDC,[(1,2_000_001)])==[(a,b,1)]

def test_conversion_is_quoted_at_loan_surplus_and_each_amount_is_independent():
    f,s,a,b,reports,sizes=setup();requests=[]
    def quote(_,rows):
        requests.append(rows)
        return [105,130] if len(requests)==1 else [1,500_000]
    f.quote=quote
    assert f.compound(s,COMET,C,WETH,[(100,10**18),(120,2*10**18)])==[(a,b,120)]
    assert sizes==[5,10] and requests[1]==[(b,5),(b,10)]
    assert reports[-1]['screen_is_execution_evidence'] is False

def test_nonpositive_unwind_does_not_request_usdc_conversion():
    f,s,*_=setup();f.quote=lambda *_:[99]
    assert f.compound(s,COMET,C,WETH,[(100,1000)])==[]

def test_unknown_quotes_keep_bounded_rotating_audits_and_can_recover():
    now=[1];f,s,a,b,_,_=setup(clock=lambda:now[0]);second=((Q,C,WETH,1),)
    s.paths=lambda *args,**kw:[(a,b),(second,b)]
    f.quote=lambda *_:(_ for _ in ()).throw(TimeoutError('temporary timeout'))
    assert f.compound(s,COMET,C,WETH,[(100,1000)])==[(a,b,100)]
    assert not f.compound(s,COMET,C,WETH,[(100,1000)])
    now[0]+=31
    assert f.compound(s,COMET,C,WETH,[(100,1000)])==[(second,b,100)]
    outputs=iter([[200,200],[500_000,500_000]]);f.quote=lambda *_:next(outputs)
    assert len(f.compound(s,COMET,C,WETH,[(100,1000)]))==2

def test_shared_unwind_and_conversion_pool_keeps_complete_call_audit():
    f,s,a,b,_,_=setup();b=((P,WETH,USDC,1),);s.u.route_book.ranked=lambda *args:[b];s.paths=lambda *args,**kwargs:[(a,b)]
    outputs=iter([[110],[1]]);f.quote=lambda *_:next(outputs)
    assert f.compound(s,COMET,C,WETH,[(100,1000)])==[(a,b,100)]

def test_all_protocol_paths_rank_conversion_in_loan_units():
    c=load_config();a=((P,C,WETH,1),);b=((Q,WETH,USDC,1),);requests=[]
    def ranked(start,end,amount,limit):requests.append((start,end,amount));return [a] if start==C else [b]
    u=SimpleNamespace(route_book=SimpleNamespace(ranked=ranked,estimate=lambda p,a:12_000_000),lock=threading.RLock())
    s=Scanners(None,None,c,u,{},lambda **v:None,threading.Event());s.discovered[(C,WETH)]=10**30
    assert s.paths(C,WETH,10**18,loan_amount=10_000_000)==[(a,b)]
    assert requests[-1]==(WETH,USDC,2_000_000)
