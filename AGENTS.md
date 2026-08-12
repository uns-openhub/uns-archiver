# Agent guidance

## Start here

- Read `package.json`, `README.md`, `config.schema.json`, and
  `src/config/project.config.extension.ts`.
- Keep the three neutral runtime profiles committed: `config-development-host.json`,
  `config-development-podman.json`, and `config-production.json`. Do not put
  deployment-specific hosts, credentials, or topic hierarchies in them.
- Read `src/index.ts` for subscription, buffering, mapping publication, and control
  API behavior.
- Read `src/writers/questDbWriter.ts` and the tests before changing QuestDB identity
  or table ingestion.

## Boundaries

- Never commit `config.json`, `.env*`, `active_topics_cache.json`, or
  `event_storage/`.
- Keep environment-specific UNS topics, hosts, credentials, and generated metadata
  out of the public repository.
- Preserve absolute counter states as the source of truth; derived deltas must be
  rebuildable.
- Use JWKS or `UNS_API_JWT_SECRET` for the control API. Do not add a default secret.
- Keep the `unsDatahub` manifest and release tag aligned with `package.json`.

## Verification

Run `pnpm run verify` before committing. If configuration types change, regenerate
`config.schema.json` and `src/config/app-config.ts`, then rerun verification.
