# BTC Official Panel Calibration

## Decision

`0xRouter` / Arkham remains a Tier C seed source. The two independent official
sample panels below improve calibration coverage, but do **not** satisfy the
condition for real alert suppression: there is only one cryptographically
verified source class, and the direct-issuer panel has an unresolved
entity-scope difference. No `quant_crypto` code, worker, threshold, ownership
semantics rule, or email decision was changed.

## Direct Address Evidence Search

No usable Gemini or VanEck address-level proof was found for the two contested
BTC labels.

- Gemini's SEC-filed agreement says a Gemini `Company Wallet` is solely
  controlled by the company or its custodian, but Exhibit A redacts the address.
  This is not importable address evidence. See the [SEC agreement](https://www.sec.gov/Archives/edgar/data/2055592/000205559226000048/gemini-spamay2026exhibit.htm).
- Gemini's [Signature Test guidance](https://support.gemini.com/hc/en-gb/articles/45221273098907-Verifying-my-wallet-using-Signature-Test-or-Small-Deposit-Test-EU-EEA)
  concerns customer self-hosted wallets, not an exchange custody-address
  disclosure.
- VanEck's [HODL FAQ](https://www.vaneck.com/us/en/investments/bitcoin-etf-hodl/hodl-faq.pdf)
  names custodians but does not publish a BTC address. The reviewed VanEck SEC
  materials likewise provide no address or address signature.

The contested Gemini/VanEck candidates therefore remain `lookup_only` and
must not be used as suppression inputs. Non-disclosure is not negative
ownership evidence; it merely prevents promotion.

## Calibration Method

Each official evidence group was seeded as its own candidate source reference,
then queried through the discovery-only endpoint at 20 requests/minute. Raw
provider bodies remain restricted content-addressed objects; this report
contains only aggregate statistics. The two panels are intentionally not
pooled, because their evidence tiers and entity scopes differ.

| Panel | Official basis | Evidence tier | Addresses | Provider outcome |
| --- | --- | --- | ---: | --- |
| OKX | Validated BTC multisig PoR signatures | A | 50 | 50 success |
| BITB | Current issuer-published BTC address list | B | 65 | 65 success |

## Results

### OKX Signed-PoR Panel

Of 50 verified PoR addresses, Arkham returned an entity name for 46 and no
entity/label/tag attribution for four. All 46 comparable entity names matched
the independently verified OKX entity; there were zero strict entity conflicts.
Twenty addresses had an additional provider tag. No formal wallet-role field
was supplied for any address.

This is evidence that Arkham entity naming was accurate on this narrow,
signature-verified OKX sample. It does not establish wallet role, ownership
intent, or general precision for every exchange/entity population.

### BITB Issuer-Publication Panel

All 65 issuer-published BITB addresses returned both a provider entity and an
address label; none were empty, and no formal wallet-role field was supplied.
The provider entity name was `Bitwise`, while the official issuer evidence is
`Bitwise Bitcoin ETF (BITB)`; strict entity-name comparison consequently gives
0 matches and 65 scope differences. The provider's address label was `BITB
Bitcoin ETF` for all 65.

That pattern is consistent with a manager/brand entity paired with a
fund-specific product label, but the panel has not independently proved that
the two names represent the same legal control entity. It is therefore a
useful product-label coverage sample, not an entity-control precision pass.

## Operational Result

The bounded run completed with 65/65 successful BITB discovery responses and
the existing 50/50 OKX responses remained fresh. Both panels retain raw-payload
hash/retention metadata for replay. No response was promoted beyond Tier C and
no resolver export or consumer replay was rebuilt as part of this calibration.

## Next Evidence Gate

Keep all provider-derived labels out of real suppression until both conditions
hold:

1. A second independent **address-level strong evidence** class (for example,
   an additional valid signed proof or a regulator/issuer disclosure with a
   direct address and unambiguous legal entity) is available; and
2. The relevant provider entity maps to that exact legal entity without a
   product-versus-manager scope ambiguity.

At that point, run a fresh source-scoped panel and a read-only `quant_crypto`
replay before proposing any consumer-policy change.
