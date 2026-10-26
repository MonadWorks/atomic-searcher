import json
from pathlib import Path
from searcher.auction_evidence import unsigned_notice,save
from searcher.structural_runner import BorrowerBook


def test_evidence_never_copies_solver_authorizations_and_retention_is_bounded(tmp_path):
    n={'auction_id':'../outside','received_at':1,'solver':{'signature':'SECRET'},
       'partial_user_operation':{'chainId':8453,'signature':'SECRET',
          'hints':{'rawReport':'0x1234','solverSignature':'SECRET'}}}
    for i in range(4):
        a={'notice':unsigned_notice({**n,'auction_id':str(i)})}
        save(tmp_path,a,{'status':'quoted_only','unsigned_solver':{'signature':'SECRET'}},limit=2)
    files=list((tmp_path/'auction-evidence').glob('*.json'))
    assert len(files)==2 and all('SECRET' not in f.read_text() for f in files)
    assert json.loads(files[0].read_text())['notice']['partial_user_operation']['hints']=={'rawReport':'0x1234'}


def test_one_identity_read_per_market_does_not_drop_unknown_borrowers():
    b=BorrowerBook();addresses=['0x'+f'{i:040x}' for i in range(5)]
    b.add(addresses)
    assert [b.batch(1) for _ in addresses]==[[x] for x in addresses]
    assert b.batch(1)==[]
