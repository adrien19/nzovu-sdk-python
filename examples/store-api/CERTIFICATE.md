
## Certificates Management in Nzovu Service:

### Nzovu Service requires security certificates for secure access and communication.

Nzovu Service access is secured by the mutual Transport Layer Security (mTLS) protocol, which requires a CA certificate from the user.

A Worker Process requires a CA certificate and private key to connect to Nzovu Service. Nzovu Service does not require an exchange of secrets; only the certificates produced by private keys are used for verification.


### CA certificates provided to Nzovu Service must meet the following requirements:

* CA certificates - A CA certificate is a type of X.509v3 certificate used for secure communication and authentication. In Nzovu Service, CA certificates are required for configuring mTLS.

    CA certificates must meet the following criteria:

    1. The certificates must be X.509v3.
    2. Each certificate in the bundle must be either a root certificate or issued by another certificate in the bundle.
    3. Each certificate in the bundle must include CA: true.
    4. A certificate cannot be a well-known CA (such as DigiCert or Let's Encrypt) unless the user also specifies certificate filters.
    5. The signing algorithm must be either RSA or ECDSA and must include SHA-256 or stronger message authentication. SHA-1 and MD5 cannot be used.
    6. The certificates cannot be generated with a passphrase.

* End-entity (client) certificates - An end-entity certificate is a type of X.509v3 certificate used by clients to authenticate themselves. Nzovu Service lets you limit access to specific end-entity certificates by using certificate filters.

    An end-entity (client) certificate must meet the following criteria:

    1. The certificate must be X.509v3.
    2. Basic constraints must include CA: false.
    3. The key usage must include Digital Signature.
    4. The signing algorithm must be either RSA or ECDSA and must include SHA-256 or stronger message authentication. SHA-1 and MD5 cannot be used.
    5. When a client presents an end-entity certificate, and the whole certificate chain is constructed, each certificate in the chain (from end-entity to the root) must have a unique Distinguished Name.

### How to issue root CA and end-entity certificates:
Nzovu Service authenticates a client connection by validating the client certificate against one or more CA certificates that are configured for the specified Nzovu Service instance.




### Note:
1. Ensure that you distribute the CA certificate (not the key, just the certificate) to all worker processes. They will need this to trust the server's certificate.
2. Each worker process should also have its own unique end-entity certificate signed by the CA. The private key of this certificate should be securely stored and not shared.
3. The server (Nzovu Service) should be configured with its own certificate and private key, and it should also be aware of the CA certificate to validate incoming client (worker) certificates.
