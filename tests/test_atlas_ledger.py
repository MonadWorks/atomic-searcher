import json
import pytest
from searcher.ledger import RiskLedger
from searcher.core import Reject

def test_auction_and_transaction_share_daily_cap_and_pending_lock(tmp_path):
    l=RiskLedger(tmp_path/'ledger.sqlite',100,3)
    try:
        l.reserve('auction:g',30,now=100,kind='auction')
        with l.db:l.db.execute("UPDATE attempts SET state='submitted' WHERE id='auction:g'")
        with pytest.raises(Reject,match='unresolved'):l.reserve('tx',10,now=100)
        l.extend('auction:g',60,now=100)
        with pytest.raises(Reject,match='ceiling'):l.extend('auction:g',101,now=100)
        assert l.db.execute("SELECT reserved FROM attempts WHERE id='auction:g'").fetchone()[0]=='60'
        l.settle('auction:g',True,55,True,now=101)
        with pytest.raises(Reject,match='ceiling'):l.reserve('tx',46,now=102)
        l.reserve('tx',45,now=102)
    finally:l.close()

def test_ordinary_attempt_cannot_be_expanded_as_auction(tmp_path):
    l=RiskLedger(tmp_path/'ledger.sqlite',100,3)
    try:
        l.reserve('tx',30);l.submitted('tx','0x'+'11'*32)
        with pytest.raises(Reject,match='auction group'):l.extend('tx',60)
    finally:l.close()
