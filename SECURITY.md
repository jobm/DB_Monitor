# Security Policy

## Supported Versions

| Version | Supported |
|---------|-----------|
| 0.0.x   | ✅ Pre-release — security issues addressed on a best-effort basis |

## Reporting a Vulnerability

We take the security of DB Monitor seriously. If you believe you have found a
security vulnerability, please report it to us as follows:

**Do not** report security vulnerabilities through public GitHub issues.

Instead, please report them via email:

**security@db-monitor.dev** (placeholder — replace with actual address)

Alternatively, use GitHub's private vulnerability reporting feature at:

https://github.com/jobm/DB_Monitor/security/advisories

### What to include

- A brief description of the issue.
- Steps to reproduce (PoC or minimal example preferred).
- Affected versions and configurations.
- Any suggested fix, if known.

### What to expect

- **Acknowledgment:** Within 48 hours of your report.
- **Initial assessment:** Within 5 business days, we will triage and determine
  severity and impact scope.
- **Fix timeline:** Critical issues will be addressed within 14 days of
  confirmation. Lower-severity issues will be scheduled in a regular release
  cycle.
- **Disclosure:** We will coordinate disclosure with you. Public disclosure is
  typically made after a fix is released.

## Security Practices

### Authentication

- API keys are hashed with Argon2 before storage — plaintext keys are never
  stored.
- JWT secrets should be rotated regularly using the `JWT_SECRET_NEXT`
  mechanism for zero-downtime rotation.
- Short-lived bearer tokens (15 min default) limit exposure from token leaks.
- WebSocket session tokens use a 60-second TTL and are only valid during the
  connection upgrade handshake.

### Deployment

- Production deployments should run with `APP_ENV=production` which rejects
  default credentials and insecure defaults.
- Secrets should be provided via mounted files (`*_FILE` env vars) rather than
  inline environment variables.
- The Helm chart ships with security defaults: non-root user, read-only root
  filesystem, capability drops, and `no-new-privileges`.

### Dependencies

- Dependencies are managed via `uv` with a locked `uv.lock` file.
- Release workflows verify signed tags and produce SBOM-attested builds.

## Disclosure Timeline

We aim to follow a 90-day disclosure deadline for confirmed vulnerabilities.
If a fix is released before the deadline, we will disclose at that time.

## Responsible Disclosure

We request that researchers follow coordinated disclosure practices. Please do
not post vulnerability details publicly before a fix has been released and
adopters have had reasonable time to update.
