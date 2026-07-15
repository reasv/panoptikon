# Inferio external inputs

## Purpose

Some inference IDs depend on values which are intentionally not stored in the
registry TOML itself. API credentials are the common case, but the mechanism is
generic: an implementation or one of its Python libraries may consume any
environment-backed value.

External inputs are part of an inference ID's deployment requirements. They are
not per-request inference arguments and are not stored per database. A value is
shared by every inference ID bound to the same external source on one Inferio
installation.

The system has four goals:

1. registry authors declare requirements explicitly, without deriving them from
   `${...}` templates;
2. reusable definitions avoid repeating labels and descriptions across models;
3. clients can explain and validate the requirements of selected inference IDs;
4. every newly spawned Python worker receives values resolved immediately before
   spawn, so changing Desktop's managed `.env` never requires restarting
   Panoptikon.

## Registry schema

Reusable definitions live in the top-level `external_inputs` table. Its keys are
registry-local identifiers, not environment-variable names:

```toml
[external_inputs.jina_api_key]
label = "Jina API key"
description = "Authenticates requests to Jina-hosted inference models."
secret = true
required = true

[external_inputs.jina_api_key.source]
type = "environment"
variable = "JINA_API_KEY"
```

`label` and `description` are user-facing. `secret` controls presentation and
redaction. `required` is the default for references to the definition.

Inference IDs reference definitions under their own `external_inputs` table:

```toml
[group.clip.inference_ids.jina-clip-v2.external_inputs.jina_api_key]
```

A reference may override the reusable definition's default requirement and add
a model-specific explanation:

```toml
[group.clip.inference_ids.jina-free.external_inputs.jina_api_key]
required = false
description = "Optional for this model; supplying it increases rate limits."
```

Group-level references are merged under each inference ID in the same direction
as group `config`: the inference-ID reference wins. Definitions are reusable
across groups and arbitrary numbers of inference IDs.

The initial source type is `environment`. Other source types may be added later
without changing inference-ID references.

### Relationship to templates

Declarations and templates are independent:

- a declared environment input is passed to the worker even when it is never
  templated into constructor configuration;
- a registry value may use `${NAME}` without declaring an external input;
- validation examines declarations only and never scans configuration for
  placeholders.

Registry TOML is parsed and merged without substituting environment templates.
Only the selected inference ID's merged constructor configuration is substituted
when Inferio is about to spawn its worker.

## Resolution and precedence

Inferio takes one environment snapshot immediately before each worker spawn. The
same snapshot is used for required-input validation, TOML substitution, and the
worker command's explicit environment.

Ordinary Server and standalone Inferio retain command-line/container semantics:

```text
.env < inherited process environment
```

Desktop-managed local inference treats its managed file as the explicit UI
configuration surface:

```text
inherited process environment < <Desktop Server root>/.env
```

The resolver reads `.env` just in time and does not depend on the copy loaded
into Panoptikon's process environment at startup. It does not mutate the
process-global environment.

An already running worker is not replaced when a value changes. Its environment
cannot be mutated safely, and automatic replacement creates undesirable
in-flight and cache-lifecycle edge cases. A worker spawned after the edit always
receives the new value; workers that could not previously be created because of
a missing requirement therefore work immediately after it is supplied.

## Inferio API

`GET /api/inference/external-inputs` reports reusable definitions, per-model
references, and presence only. It never returns values. Requiredness is resolved
per inference ID after applying reference overrides.

Inferio rejects an attempted load of an inference ID when one of its required
inputs is absent or empty. The error names the inference ID, the user-facing
input label, and its environment binding, but never any supplied value.

## Desktop management API

Desktop-managed Panoptikon additionally exposes:

- `GET /api/desktop/external-inputs` for definitions, usages, and managed-file
  presence;
- `PUT /api/desktop/external-inputs` to set or remove declared
  environment-backed values in `<Desktop Server root>/.env`.

These routes are absent outside `--desktop-managed` mode. Writes accept only
environment-variable names declared by the active local Inferio registry. They
preserve unrelated `.env` entries, serialize concurrent updates, replace the
file atomically, apply owner-only Unix permissions, and never echo values.

The normal status read does not expose secret values. Existing secrets appear
as configured; an empty edit means keep the current value, while removal is an
explicit operation. A separately invoked reveal route may return one declared
value to the local Desktop page so the user can deliberately unhide it. It is
never included in bulk status or diagnostics. Non-secret values are returned by
the Desktop management read for ordinary editing.

## User interface

The database wizard inserts a conditional **Additional configuration** step
after model selection when selected models reference external inputs. It groups
usages by reusable definition/source, shows every selected model which uses the
value, and evaluates requiredness over the selected usages. Missing required
values block continuation. Values saved on this step are installation-wide and
remain configured if the database wizard is later abandoned.

The model-selection cards display requirement/configuration status. Desktop also
provides an installation-wide Additional configuration page for later changes.

The Scan page prevents running or scheduling a model whose required inputs are
missing. In Desktop mode it links to the management page. With remote Inferio it
shows the declared binding and directs the operator to configure it on the
Inferio host.

Backend load-time validation remains authoritative; UI checks are explanatory
and prevent predictable failures, not a security boundary.

## Security

- Values are never returned by Inferio status APIs or included in registry
  metadata. Desktop returns non-secret managed values and exposes a single-value
  secret reveal only after an explicit user action.
- Secret inputs use password fields and are not pre-populated in browser state.
- Logs and errors include identifiers and presence only.
- `.env` writes use an atomic same-directory replacement.
- Unix files are mode `0600`; platform-appropriate owner-only protection should
  be retained or added where supported.
- Diagnostics must redact `.env` contents and external-input write bodies.
