# Third-party notices

Atomic Searcher is distributed as **GPL-2.0-or-later**. Individual files retain
their existing SPDX identifiers; a file marked MIT remains available under MIT.
The GPL text is in [LICENSE](LICENSE), and the MIT text is in
[LICENSES/MIT.txt](LICENSES/MIT.txt).

| Material | Source | License / treatment |
| --- | --- | --- |
| Tick constants and integer swap arithmetic in `searcher/math.py` | [Uniswap v3-core](https://github.com/Uniswap/v3-core) | GPL-2.0-or-later derivation, as documented in the file |
| `references/v3-TickMath.sol` | [Uniswap TickMath](https://github.com/Uniswap/v3-core/blob/main/contracts/libraries/TickMath.sol) | Original GPL-2.0-or-later header retained |
| `references/slipstream-*`, `references/slip-iquoter.sol` | [Aerodrome Slipstream](https://github.com/aerodrome-finance/slipstream) | Original file-level GPL-2.0-or-later notices retained; reference material, outside the build source directory |
| `references/v3-license.txt` | Uniswap v3-core historical license | Original historical BSL text retained as provenance; it names GPL v2 or later as the change license |
| Python dependencies | Packages listed in `pyproject.toml` and `requirements.lock` | Installed separately; their own licenses apply |

Public pool addresses, protocol interfaces and on-chain state used by regression
fixtures identify third-party protocols and markets. They are not an endorsement
or an assertion of authorship of those protocols.

This repository does not include downloaded third-party repositories, personal
deployment records, provider credentials, or the original operator's Git history.
