"""Exact, indexed cycle combinations without materializing their Cartesian product."""
from bisect import bisect_right
from collections import defaultdict
from collections.abc import Sequence
from itertools import accumulate
from math import prod
from array import array


class Family:
    def __init__(self, legs, pair=False):
        self.legs = tuple(tuple(sorted(leg)) for leg in legs)
        self.pair = pair
        self.size = len(self.legs[0]) * (len(self.legs[0])-1)//2 if pair else prod(map(len, self.legs))

    def at(self, index):
        if self.pair:
            leg = self.legs[0]
            # n-i-1 pairs have first index i; binary search the triangular sum.
            n, lo, hi = len(leg), 0, len(leg)-1
            while lo+1 < hi:
                mid = (lo+hi)//2
                if mid*(2*n-mid-1)//2 <= index: lo = mid
                else: hi = mid
            return (leg[lo], leg[lo+1+index-lo*(2*n-lo-1)//2])
        group = []
        for leg in reversed(self.legs):
            index, offset = divmod(index, len(leg))
            group.append(leg[offset])
        return tuple(sorted(group))

    def rank(self, group):
        if len(group) != (2 if self.pair else len(self.legs)): return None
        if self.pair:
            leg = self.legs[0]
            if not all(a in leg for a in group): return None
            i, j = sorted(leg.index(a) for a in group)
            return i*(2*len(leg)-i-1)//2+j-i-1 if i != j else None
        rank = 0
        for leg in self.legs:
            matches = [a for a in group if a in leg]
            if len(matches) != 1: return None
            rank = rank*len(leg)+leg.index(matches[0])
        return rank


class IndexedFamilies(Sequence):
    def __init__(self, families):
        self.families = tuple(f for f in families if f.size)
        self.ends = tuple(accumulate(f.size for f in self.families))

    def __len__(self): return self.ends[-1] if self.ends else 0

    def __getitem__(self, index):
        if isinstance(index, slice):
            return [self[i] for i in range(*index.indices(len(self)))]
        if index < 0: index += len(self)
        if not 0 <= index < len(self): raise IndexError(index)
        family = bisect_right(self.ends, index)
        return self.families[family].at(index-(self.ends[family-1] if family else 0))

    def __eq__(self, other):
        if isinstance(other, IndexedFamilies):
            return [(f.legs,f.pair) for f in self.families] == [(f.legs,f.pair) for f in other.families]
        return len(self) == len(other) and set(self) == set(other)


class CycleGroups(IndexedFamilies):
    def __init__(self, pools, settlements, max_hops):
        pairs, adjacent, kinds = defaultdict(list), defaultdict(set), {}
        for p in pools:
            pair = tuple(sorted((p.token0, p.token1)))
            if pair[0] == pair[1]: continue
            pairs[pair].append(p.address); kinds[p.address] = p.kind
            adjacent[p.token0].add(p.token1); adjacent[p.token1].add(p.token0)
        settlements = set(settlements)
        families = [Family([addresses], pair=True) for pair,addresses in sorted(pairs.items())
                    if max_hops >= 2 and settlements.intersection(pair) and len(addresses) >= 2]
        # Compact family spans represent economic token cycles. A four-hop
        # cycle's disjoint first-V2 partitions are one scheduling unit, not up
        # to four votes. Never allocate one object per pool combination.
        self.economic_spans = {h: array('I') for h in (2, 3, 4)}
        for i in range(len(families)):
            self.economic_spans[2].extend((i, i+1))
        cycles = set()
        # Walk token edges, not every parallel fee-tier/pool combination.
        for start in sorted(settlements):
            def walk(path):
                token = path[-1]
                if len(path) >= 3 and start in adjacent[token]:
                    # Starting at the least settlement and one orientation
                    # enumerates each unoriented token cycle exactly once.
                    if path[1] < path[-1]: cycles.add(path)
                if len(path) >= max_hops: return
                for nxt in sorted(adjacent[token]):
                    if nxt not in path and not (nxt in settlements and nxt < start):
                        walk(path+(nxt,))
            walk((start,))
        for cycle in sorted(cycles):
            first_family = len(families)
            legs = [pairs[tuple(sorted((a,b)))] for a,b in zip(cycle, cycle[1:]+cycle[:1])]
            if len(cycle) < 4:
                families.append(Family(legs))
            else:
                # Partition by first V2 leg. Products are disjoint and exclude
                # exactly the four-hop, all-CL routes the executor does not use.
                for first in range(4):
                    parts = [[a for a in leg if (kinds[a] != 'v2' if i < first else
                             kinds[a] == 'v2' if i == first else True)] for i,leg in enumerate(legs)]
                    if all(parts): families.append(Family(parts))
            if len(families) > first_family:
                self.economic_spans[len(cycle)].extend((first_family, len(families)))
        super().__init__(families)
        members = defaultdict(list)
        for i,family in enumerate(self.families):
            for leg in family.legs:
                for address in leg: members[address].append(i)
        self.members = dict(members)

    def containing(self, pool):
        subsets = []
        for index in self.members.get(pool, ()):
            family = self.families[index]
            if family.pair:
                subsets.append(Family([(pool,), tuple(a for a in family.legs[0] if a != pool)]))
            else:
                subsets.append(Family([(pool,) if pool in leg else leg for leg in family.legs]))
        return IndexedFamilies(subsets)

    def index(self, group):
        for i in self.members.get(group[0], ()):
            rank = self.families[i].rank(group)
            if rank is not None: return (self.ends[i-1] if i else 0)+rank
        raise ValueError('cycle no longer present')


class PoolGroups:
    """Build a pool's constrained product only when an activity hint needs it."""
    def __init__(self, groups):
        self.groups = groups

    def get(self, pool, default=()):
        return self.groups.containing(pool) if pool in self.groups.members else default


class EconomicRotation:
    """Bounded cold exploration, independent of Cartesian-product size.

    Shorter cycles receive two turns each for each four-hop turn: they require
    fewer swaps and reads. This is an exploration allocation, never evidence
    of profitability. Each token cycle gets a turn before its next variant.
    The ordinary page retains its separate linear completeness cursor.
    """
    schedule = (2, 3, 2, 3, 4)

    def __init__(self, groups):
        self.groups = groups
        self.turn = 0
        self.positions = {h: 0 for h in (2, 3, 4)}

    def next(self):
        for _ in self.schedule:
            hops = self.schedule[self.turn % len(self.schedule)]
            self.turn += 1
            spans = self.groups.economic_spans[hops]
            count = len(spans)//2
            if not count:
                continue
            step = self.positions[hops]
            self.positions[hops] += 1
            cycle = step % count
            start, end = spans[2*cycle:2*cycle+2]
            size = sum(self.groups.families[i].size for i in range(start, end))
            variant = (step//count) % size
            for i in range(start, end):
                family = self.groups.families[i]
                if variant < family.size:
                    return family.at(variant)
                variant -= family.size
        return None

    def resume(self, previous):
        # New token cycles may shift positions. Progress survives refresh;
        # the separate stable-group linear cursor guarantees full coverage.
        self.turn = previous.turn
        self.positions = dict(previous.positions)


def hot_groups(groups, last_seen, maximum=128):
    result, seen = [], set()
    for pool in sorted(groups.members, key=lambda a:(-last_seen.get(a, 0), a)):
        choices = groups.containing(pool)
        for group in choices[:2]:
            if group not in seen: result.append(group); seen.add(group)
        if len(result) >= maximum: break
    return result[:maximum]
