# ddm-portal
DDM Project

## Environment Setup

1. Copy `.env.example` to `.env`
2. Fill in your actual credentials
3. Never commit `.env` to version control

### Required Environment Variables

- `DATABASE_URL`: PostgreSQL connection string
- `MOSMS_SP_ID`: SMS service provider ID
- `MOSMS_PASSWORD`: SMS API password
- `JWT_SECRET`: JWT signing secret
- And others listed in `.env.example`

## Security hygiene (mandatory)

- Create `.env` from `.env.example` and keep it local.
- Add `.env`, `*.key`, `*.pem` to `.gitignore`.
- Never commit credentials, secrets, passwords, or tokens.
- After history rewrite:
  - rotate all credentials immediately (DB, JWT, API keys, SSH)
  - verify with:
    - `git log --all --grep='thevardsub|7vW4w8s6Y1rk|DXHoVfKQGjS3EKUk8eGrfzTZ4'`
    - `git log --all -p -- openresty-lualib/mosms.lua | grep -E ...`
- Optional: use secret scan tools (GitGuardian, truffleHog).
