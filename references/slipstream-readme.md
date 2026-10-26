# Slipstream

This repository contains the smart contracts for the Slipstream Concentrated Liquidity contracts. It contains
the core concentrated liquidity contracts, adapted from UniswapV3's core contracts. It contains the higher level
periphery contracts, adapted from UniswapV3's periphery contracts. It also contains gauges designed to operate
within the Velodrome ecosystem.

See `SPECIFICATION.md` and `CHANGELOG.md` for more information.

## Installation

This repository is a hybrid hardhat and foundry repository.

Install hardhat dependencies with `yarn install`.
Install foundry dependencies with `forge install`.

Run hardhat tests with `yarn test`.
Run forge tests with `forge test`.

## Testing

### Invariants

To run the invariant tests, echidna must be installed. The following instructions require additional installations (e.g. of solc-select).

```
echidna test/invariants/E2E_mint_burn.sol --config test/invariants/E2E_mint_burn.config.yaml --contract E2E_mint_burn
echidna test/invariants/E2E_swap.sol --config test/invariants/E2E_swap.config.yaml --contract E2E_swap
```

## Licensing

As this repository depends on the UniswapV3 `v3-core` and `v3-periphery` repository, the contracts in the
`contracts/core` and  `contracts/periphery` folders are licensed under `GPL-2.0-or-later` or alternative
licenses (as indicated in their SPDX headers).

Files in the `contracts/gauge` folder are licensed under the Business Source License 1.1 (`BUSL-1.1`).

## Bug Bounty
Velodrome has a live bug bounty hosted on ([Immunefi](https://immunefi.com/bounty/velodromefinance/)).

## Deployments

### Initial Deployment
Initial deployment of Slipstream contracts on Base.

| Name               | Address                                                                                                                               |
| :----------------- | :------------------------------------------------------------------------------------------------------------------------------------ |
| GaugeFactory               | [0xD30677bd8dd15132F251Cb54CbDA552d2A05Fb08](https://basescan.org/address/0xD30677bd8dd15132F251Cb54CbDA552d2A05Fb08#code) |
| GaugeImplementation               | [0xF5601F95708256A118EF5971820327F362442D2d](https://basescan.org/address/0xF5601F95708256A118EF5971820327F362442D2d#code) |
| MixedQuoter               | [0x0A5aA5D3a4d28014f967Bf0f29EAA3FF9807D5c6](https://basescan.org/address/0x0A5aA5D3a4d28014f967Bf0f29EAA3FF9807D5c6#code) |
| NonfungiblePositionManager               | [0x827922686190790b37229fd06084350E74485b72](https://basescan.org/address/0x827922686190790b37229fd06084350E74485b72#code) |
| NonfungibleTokenPositionDescriptor               | [0x01b0CaCB9A8004e08D075c919B5dF3b59FD53c55](https://basescan.org/address/0x01b0CaCB9A8004e08D075c919B5dF3b59FD53c55#code) |
| PoolFactory               | [0x5e7BB104d84c7CB9B682AaC2F3d509f5F406809A](https://basescan.org/address/0x5e7BB104d84c7CB9B682AaC2F3d509f5F406809A#code) |
| PoolImplementation               | [0xeC8E5342B19977B4eF8892e02D8DAEcfa1315831](https://basescan.org/address/0xeC8E5342B19977B4eF8892e02D8DAEcfa1315831#code) |
| QuoterV2               | [0x254cF9E1E6e233aa1AC962CB9B05b2cfeAaE15b0](https://basescan.org/address/0x254cF9E1E6e233aa1AC962CB9B05b2cfeAaE15b0#code) |
| CustomSwapFeeModule               | [0xF4171B0953b52Fa55462E4d76ecA1845Db69af00](https://basescan.org/address/0xF4171B0953b52Fa55462E4d76ecA1845Db69af00#code) |
| CustomUnstakedFeeModule               | [0x0AD08370c76Ff426F534bb2AFFD9b5555338ee68](https://basescan.org/address/0x0AD08370c76Ff426F534bb2AFFD9b5555338ee68#code) |
| SwapRouter               | [0xBE6D8f0d05cC4be24d5167a3eF062215bE6D18a5](https://basescan.org/address/0xBE6D8f0d05cC4be24d5167a3eF062215bE6D18a5#code) |
| SugarHelper               | [0x0AD09A66af0154a84e86F761313d02d0abB6edd5](https://basescan.org/address/0x0AD09A66af0154a84e86F761313d02d0abB6edd5#code) |
| DynamicSwapFeeModule               | [0x090b2A6bb475c00e2256e2095A60887cD710803b](https://basescan.org/address/0x090b2A6bb475c00e2256e2095A60887cD710803b#code) |

### Gauge Caps Deployment
Deployment with gauge emission cap enforcement and redistributor functionality to manage and reallocate excess emissions.

**Key Changes:**
- **Gauge Caps**:
    - `CLGauge` enforces emission caps to limit rewards per gauge.
    - The emission cap is set as a percentage of total weekly emissions.
    - When a gauge exceeds its cap, excess emissions are automatically transferred to the `Redistributor`.
- **Redistributor**:
    - Manages redistribution of excess emissions when gauges exceed their caps.
    - Collects excess emissions from capped gauges and redistributes them to other eligible gauges proportionally to their voting weight.

| Name               | Address                                                                                                                               |
| :----------------- | :------------------------------------------------------------------------------------------------------------------------------------ |
| GaugeFactory               | [0xB630227a79707D517320b6c0f885806389dFcbB3](https://basescan.org/address/0xB630227a79707D517320b6c0f885806389dFcbB3#code) |
| GaugeImplementation               | [0xC0d2086B6f70C0C40423626167096c6196cFA0c8](https://basescan.org/address/0xC0d2086B6f70C0C40423626167096c6196cFA0c8#code) |
| MixedQuoter               | [0x49540630A4d2CE67d54450D007D634F4c45B4f4f](https://basescan.org/address/0x49540630A4d2CE67d54450D007D634F4c45B4f4f#code) |
| NonfungiblePositionManager               | [0xa990C6a764b73BF43cee5Bb40339c3322FB9D55F](https://basescan.org/address/0xa990C6a764b73BF43cee5Bb40339c3322FB9D55F#code) |
| NonfungibleTokenPositionDescriptor               | [0xf632031B94D72deE0D99DeF846c9b6211041337f](https://basescan.org/address/0xf632031B94D72deE0D99DeF846c9b6211041337f#code) |
| PoolFactory               | [0xaDe65c38CD4849aDBA595a4323a8C7DdfE89716a](https://basescan.org/address/0xaDe65c38CD4849aDBA595a4323a8C7DdfE89716a#code) |
| PoolImplementation               | [0x942e97a4c6FdC38B4CD1c0298D37d81fDD8E5A16](https://basescan.org/address/0x942e97a4c6FdC38B4CD1c0298D37d81fDD8E5A16#code) |
| Quoter               | [0x3d4C22254F86f64B7eC90ab8F7aeC1FBFD271c6C](https://basescan.org/address/0x3d4C22254F86f64B7eC90ab8F7aeC1FBFD271c6C#code) |
| SwapFeeModule               | [0x5264Eeeab16037A7A7AF15Ff69A470af6e2a2223](https://basescan.org/address/0x5264Eeeab16037A7A7AF15Ff69A470af6e2a2223#code) |
| SwapRouter               | [0xcbBb8035cAc7D4B3Ca7aBb74cF7BdF900215Ce0D](https://basescan.org/address/0xcbBb8035cAc7D4B3Ca7aBb74cF7BdF900215Ce0D#code) |
| UnstakedFeeModule               | [0xCCC21f4750E8B3E9C095BCB5d2fF59247A2CCD35](https://basescan.org/address/0xCCC21f4750E8B3E9C095BCB5d2fF59247A2CCD35#code) |
| DynamicSwapFeeModule               | [0xF4Ecd78EBEB6d36CF7f80B5B6B41453515fe2785](https://basescan.org/address/0xF4Ecd78EBEB6d36CF7f80B5B6B41453515fe2785#code) |
| Redistributor               | [0x11a53f31Bf406de59fCf9613E1922bd3E283A4B4](https://basescan.org/address/0x11a53f31Bf406de59fCf9613E1922bd3E283A4B4#code) |

### Gauges V3 Deployment
This is the current latest deployment of the gauges. Existing gauges are still in use, but all new gauges will be deployed from here.

**Key Changes:**
- **Minimum Stake Time**: Per-pool configurable minimum stake duration.
- **Early Unstake Penalty**: Penalty rate applied to both `getReward` and `withdraw` if called before the minimum stake time has elapsed.
- **Dynamic Swap Fee Module**: Replaces `CustomSwapFeeModule` with dynamic fee scaling based on tick crossings.
- **MixedRouteQuoterV3**: New quoter supporting 3 CL factories via bitmask encoding.
- **Redistributor**: Updated to support dual CL gauge factories.

| Name               | Address                                                                                                                               |
| :----------------- | :------------------------------------------------------------------------------------------------------------------------------------ |
| DynamicSwapFeeModule               | [0x87D8f999BBa9343E8099552426775B51C338E8CB](https://basescan.org/address/0x87D8f999BBa9343E8099552426775B51C338E8CB#code) |
| GaugeFactory               | [0x385293CaE378C813F16f0C1334d774AdDDf56AbB](https://basescan.org/address/0x385293CaE378C813F16f0C1334d774AdDDf56AbB#code) |
| GaugeImplementation               | [0x434BCcaB043311a20b16021C137EA81702790f7B](https://basescan.org/address/0x434BCcaB043311a20b16021C137EA81702790f7B#code) |
| MixedQuoter               | [0x9951FF0b830E46ef0e7Ce34d9117e3214B1F0b5a](https://basescan.org/address/0x9951FF0b830E46ef0e7Ce34d9117e3214B1F0b5a#code) |
| MixedQuoterV2               | [0xb4A9E5Fc0727BEF09D819fcfc5ece8CA9bCf09EB](https://basescan.org/address/0xb4A9E5Fc0727BEF09D819fcfc5ece8CA9bCf09EB#code) |
| MixedQuoterV3               | [0xCd2A7D98e82D6107eac1828ce8DeAA6acB65b555](https://basescan.org/address/0xCd2A7D98e82D6107eac1828ce8DeAA6acB65b555#code) |
| NonfungiblePositionManager               | [0xe1f8cd9AC4e4A65F54f38a5CdAfCA44f6dD68b53](https://basescan.org/address/0xe1f8cd9AC4e4A65F54f38a5CdAfCA44f6dD68b53#code) |
| NonfungibleTokenPositionDescriptor               | [0xc85C126442bb5B654792A70135805a9778C8e3fE](https://basescan.org/address/0xc85C126442bb5B654792A70135805a9778C8e3fE#code) |
| PoolFactory               | [0xf8f2eB4940CFE7d13603DDDD87f123820Fc061Ef](https://basescan.org/address/0xf8f2eB4940CFE7d13603DDDD87f123820Fc061Ef#code) |
| PoolImplementation               | [0xc770898522D2A9c8Da7A10D63989b6b58305B665](https://basescan.org/address/0xc770898522D2A9c8Da7A10D63989b6b58305B665#code) |
| Quoter               | [0x514c8B5f54112481E28028F1166Bd78501089259](https://basescan.org/address/0x514c8B5f54112481E28028F1166Bd78501089259#code) |
| Redistributor               | [0xEe5b3C7b333e2870B746b3B2b168EF0958e55e15](https://basescan.org/address/0xEe5b3C7b333e2870B746b3B2b168EF0958e55e15#code) |
| SwapRouter               | [0x698Cb2b6dd822994581fEa6eA4Fc755d1363A92F](https://basescan.org/address/0x698Cb2b6dd822994581fEa6eA4Fc755d1363A92F#code) |
| UnstakedFeeModule               | [0xc2cc3256434AfbC36Bb5e815e1Bb2151310a1a0b](https://basescan.org/address/0xc2cc3256434AfbC36Bb5e815e1Bb2151310a1a0b#code) |
