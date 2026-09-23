# TLS configuration

The SDK verifies server TLS using system roots or `NZOVU_CA_FILE`. Optional mTLS
uses both `NZOVU_CERT_FILE` and `NZOVU_KEY_FILE`; the server must trust their CA.
API-key authentication uses `NZOVU_API_KEY` independently of TLS.

Certificates need a matching server hostname/SAN. For current Nzovu mTLS, the
chain must be X.509v3; CA certificates require certificate-signing usage and
client leaves require digital-signature/client-auth usage. Use SHA-256 or stronger
signatures and distinct certificate subjects along the chain. Keep private keys
private; clients receive CA certificates, never the CA private key.

For repeatable test certificates and positive/negative handshake tests, use the
SDK's `scripts/run_live_ownership.py` harness. Its generated keys are fixture-only.
See [SDK transport details](../../docs/AUTH_OWNERSHIP.md).

Plaintext is explicit: `NZOVU_TLS_ENABLED=false`. The Compose example uses it
only for the loopback development setup.
