# BTC Second Official Source Review

## Decision

No address-level official or signed proof was found for the disputed Gemini or
VanEck labels. Those claims remain contested and lookup-only.

The Bitwise Bitcoin ETF (BITB) issuer page does publish a current set of its
Bitcoin wallet addresses. The identity ledger imported its 65 active addresses
as Tier B issuer-publication evidence. This is a second independent official
source class alongside the signed OKX PoR importer, but it is not a signature
proof and cannot by itself drive consumer suppression.

## Gemini And VanEck Review

Gemini's public [Signature Test guidance](https://support.gemini.com/hc/en-gb/articles/45221273098907-Verifying-my-wallet-using-Signature-Test-or-Small-Deposit-Test-EU-EEA)
describes how a customer proves control of a self-hosted address. It is not a
disclosure of a Gemini-owned Bitcoin address or a signature by Gemini for a
custody address.

VanEck's [HODL FAQ](https://www.vaneck.com/us/en/investments/bitcoin-etf-hodl/hodl-faq.pdf)
identifies Gemini Trust Company and Coinbase Custody as custodians, and the
[SEC-filed Gemini Clearing Agreement](https://www.sec.gov/Archives/edgar/data/1838028/000093041324000044/c106800_ex10-9.htm)
documents the relationship. Neither source discloses a particular Bitcoin
address or a signature binding an address to VanEck.

Therefore the prior disputed candidates stay unchanged:

| Address suffix | Candidate labels | Official address-level result | Ledger action |
| --- | --- | --- | --- |
| `3MgEA...rP5pgd` | OKEx, Gemini | No direct Gemini or signed address proof found | contested, lookup-only |
| `3FM9v...UxhbJ3` | OKEx, VanEck | No direct VanEck or signed address proof found | contested, lookup-only |

Absence of a disclosure is not negative ownership evidence. It only prevents a
promotion to a higher evidence tier.

## Bitwise BITB Issuer Publication

The public [BITB issuer page](https://bitbetf.com/) exposed 65 active Bitcoin
addresses at its reported update time of `2026-07-22T14:15:00.516Z`. The import
recorded:

- source authority: `official`
- evidence tier: `B`
- assertion: `entity_control` for `Bitwise Bitcoin ETF (BITB)`
- importer: `direct_issuer_publication`
- 31-day expiry from the issuer-reported update time
- 65 inserted evidence rows, 0 duplicates
- sanitized snapshot SHA-256:
  `bd9fa09d61005dd5c0ab8903e6ccd44f36f7ce2d5ef10f247d08c1f4ad5166c4`

The page can contain short-lived report links. The importer parses the page in
memory and persists only a canonical snapshot containing the active addresses,
source URL, issuer update timestamp, retrieval timestamp, and the page hash.
It does not retain HTML, report links, query strings, credentials, or response
headers.

This direct issuer publication supports a reviewed entity-control claim after
the normal Tier B review flow. It does not assert a hot/cold wallet role,
ownership-transfer intent, or an alert suppression rule.

## Operating Boundary

No 0xRouter/Arkham request was made for this second-source import. The new
official address set may later be used as a separate calibration panel, but a
provider match does not promote a provider label and must not change
`quant_crypto` ownership semantics or email delivery.

The next direct-evidence priority remains a Gemini or VanEck address-level
artifact: a signed message, a disclosed address list, or a regulator-hosted
filing that names a specific address. Until then, do not resolve the two
contested addresses in favor of any candidate.
