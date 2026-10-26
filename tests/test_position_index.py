import re
from types import SimpleNamespace
from searcher.morpho_universe import all_position_pages
from searcher.structural_runner import BorrowerBook

class Index:
    def __init__(self,rows):self.rows=rows;self.offsets=[]
    def post(self,url,json):
        q=json['query'];skip=int(re.search(r'skip:(\d+)',q)[1]);size=int(re.search(r'first:(\d+)',q)[1]);self.offsets.append(skip)
        assert skip<=10000
        rows=self.rows
        for field in ('borrowShares','collateral'):
            for op in ('gte','lte'):
                found=re.findall(field+'_'+op+r':"(\d+)"',q);assert len(found)<=1
                if found:
                    v=int(found[0]);rows=[r for r in rows if (int(r['state'][field])>=v if op=='gte' else int(r['state'][field])<=v)]
        order=re.search(r'orderBy:(\w+)',q)[1]
        if order!='HealthFactor':rows=sorted(rows,key=lambda r:int(r['state']['borrowShares' if order=='BorrowShares' else 'collateral']))
        page={'data':{'marketPositions':{'items':rows[skip:skip+size],'pageInfo':{'countTotal':len(rows)}}}}
        return SimpleNamespace(raise_for_status=lambda:None,json=lambda:page)

def row(i,shares,collateral):return {'user':{'address':f'0x{i+1:040x}'},'market':{'marketId':'0x'+'11'*32},'state':{'borrowShares':shares,'borrowAssets':10,'collateral':collateral}}

def test_entire_index_survives_offset_ceiling_and_boundary_ties():
    client=Index([row(i,1+i//3,1000) for i in range(12503)])
    pages=list(all_position_pages(client));assert sum(len(p) for p,_ in pages)==12503
    assert pages[-1][1]['reached_end'] and not pages[-1][1]['snapshot_complete']
    assert max(client.offsets)<1000

def test_huge_share_tie_partitions_by_collateral_then_resumes():
    client=Index([row(i,1,i+1) for i in range(12001)]+[row(12002,2,500)])
    pages=list(all_position_pages(client));assert sum(len(p) for p,_ in pages)==12002
    assert pages[-1][1]['reached_end'] and max(client.offsets)<=10000

def test_index_ranking_cannot_overwrite_authoritative_position():
    book=BorrowerBook();a='0x'+'22'*20
    hint={'borrowShares':100,'borrowAssets':100,'collateral':100};book.seed(a,hint,800000000000000000)
    assert book.snapshot()[0]['state_source']=='index_hint'
    actual={'borrower':a,'shares':0,'debt':0,'collateral':0,'boundary':0};book.update([actual]);book.seed(a,hint,800000000000000000)
    assert book.snapshot()==[actual] and book.batch()==[a]
