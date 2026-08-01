# Model identity, jobs, and execution profiles — design

How inference models are represented: what an inference ID names, how a
setter relates to it, how one model runs against multiple corpora, how
query-time embedding may execute differently from indexing, and how
embedding spaces are related across all of it. Settled 2026-07-31 over
an extended design dialogue. Not implemented.

Replaces the `clip`/`tclip` duplication and every string-convention
derived from it. Touches the inference registry, the extraction job
system, the worker protocol, PQL query preprocessing, the vector-quant
space grouping, preload/prewarm, and the UI's model selectors.

---

## Part I — Premises

### 1. The two purposes of an inference ID

Everything in this design is constrained by the two purposes inference
IDs serve. They are load-bearing and any change must preserve both:

1. **Opaque-function abstraction.** Inference IDs hide all
   model-specific configuration so panoptikon core and the frontend can
   treat models as opaque, generalized functions behind an extremely
   generic API. Panoptikon is a platform for experimenting with AI
   indexing and search: new models — and new *kinds* of models — must
   slot in as registry entries without per-model exceptions in core or
   UI code.

2. **Setter = pure-function provenance.** Every piece of extracted data
   is stamped with the setter ID that produced it, identifying exactly
   where it came from and how it was created. This is what makes the
   index **immutable and reproducible**: the index is
   `inference_id(user's files)` and can be recreated at any time by
   re-running the right batch jobs. The consequences are definitional,
   not incidental:
   - data from different setters coexists without overwriting, and is
     selected at search time;
   - each setter runs **at most once** per data point (the run-once /
     skip-processed rule);
   - outputs are never manually editable — editing would contradict the
     paradigm.

   Provenance here is **nominal**, not cryptographic. The system has
   never verified that weights bits match the registry entry; dtype is
   not part of identity (fp16/fp32 negotiation happens outside the ID),
   and GPU kernels are nondeterministic. The stamp means "this
   *declared function* produced this," and declaring what counts as the
   same function is a trust decision made by whoever authors the
   registry entry. This observation does real work later (execution
   profiles, §8).

### 2. How the system works today

The registry is TOML (`python/inferio/config/inference.toml` built-in,
`config/inference/*.toml` user-side), organized as **groups**. A full
inference ID is `group/name`, split on the first `/`
(`panoptikon/src/inferio/registry.rs:249`). A group carries:

- `config.impl_class` — which Python worker implementation runs it;
- `metadata.target_entities` — `["items"]`, `["text"]`, or `["files"]`;
- `metadata.output_type` — `tags`, `text`, `clip`, `text-embedding`;
- `metadata.input_spec.handler` — `image_frames`, `extracted_text`,
  `audio_tracks`, …;
- per-ID entries whose `config` merges under the group's.

Three structurally different mechanisms hang off this metadata:

- **Work queries.** `target_entities` selects the shape of the query
  that enumerates job work (`jobs/extraction.rs:949`): `["items"]`
  yields an item-per-item query; `["text"]` yields an
  item-data-entry-per-entry query (join through `item_data` on
  `data_type = 'text'`, cursor/dedup on `data_id` instead of
  `item_id`). This is not a modality difference — it is a *role*
  difference: a text-consuming job processes data produced for each
  item by *other* jobs, so it may legitimately encounter the same item
  many times (once per extracted text), where an item job runs exactly
  once per item.
- **Cron ordering.** `cron_phase` (`jobs/cron.rs:138`) sorts
  `items`/`files` groups into the Source phase and `text` groups into
  the Derived phase, so text-producing jobs (OCR, whisper, captioners,
  taggers) run before text-consuming jobs (text embeddings).
- **Output handling.** `output_type` picks the output handler;
  `"text-embedding"` is the only branch that threads
  `source_data_id` (`output_handlers/mod.rs:68`), which is what lets
  text embeddings point back at the text row they came from.

**Setter identity is the full inference ID string.** The `setters`
table stores it as an opaque name; PQL filters, the processed-by rule,
job logs, quant coverage, and the UI all address setters by this
string.

**Query-time embedding** happens in PQL preprocessing:
`embed_text_query` / `embed_image_query`
(`pql/preprocess.rs:821,859`) call the *same inference ID* the index
was built with. `embed_image_query` sends a `{"text": ...}` payload to
a `clip/` model and relies on the impl being bimodal — the openclip
impl (`python/inferio/impl/clip.py:124`) dispatches on input shape
(file bytes → `encode_image`, `{"text"}` → `encode_text`).

**Residency** has two mechanisms: full preload (models kept loaded via
a cron tick with a lease TTL, `jobs/cron.rs:576`) and the prewarm pool
(a parked warm process per impl class, `inferio/prewarm.rs`). Both
select "search-usable embedding setters" via one shared rule:
`data_type ∈ {clip, text-embedding} AND NOT name LIKE 'tclip/%'`
(`db/extraction_log.rs:48`).

### 3. The tclip problem

CLIP can encode both media and text into one shared embedding space,
and the impl handles mixed batches fine. But because job shape is
*group* metadata, one model cannot serve both the items job and the
text job. The workaround: every CLIP model is defined **twice** —
`[group.clip]` (items, image_frames, output `clip`) and
`[group.tclip]` (text, extracted_text, output `text-embedding`) — with
byte-identical per-ID `config`. The relationship between the halves is
a **naming convention**: `tclip/X` is the "xmodal text sibling" of
`clip/X`, reconstructed wherever the pairing is needed:

- `xmodal_text_sibling_name` = `format!("t{model}")`
  (`db/vector_quants.rs:48`), used by quant space grouping
  (`group_spaces`, `vector_quants.rs:477` — which needs a
  dim-equality *sanity check* precisely because the link is
  non-authoritative), quant profile resolution
  (`pql/preprocess.rs:354`), and the explain harness;
- two raw `format!("t{}")` sites in the query builders widening the
  setter condition under `clip_xmodal`
  (`pql/builder/filters/item_similarity.rs:222`,
  `image_embeddings.rs:208`);
- the `tclip/` prefix exclusion in `filter_search_embedding_setters`
  (`db/extraction_log.rs:60`);
- five prefix filters in the UI
  (`SearchTypeSelector.tsx`, `TextEmbeddingSearch.tsx`,
  `TextSimilarItems.tsx`, `similaritySearchOptions.tsx`,
  `itemSimilarity.tsx`) plus a literal `` `t${model}` `` in
  `VectorIndexModeSelector.tsx:44`.

The purpose of tclip is the cross-modal item-to-item similarity
search: comparing one item's extracted-text vectors against another
item's image vectors in the shared space. A niche (and expensive)
feature — but the *cost* of representing it is systemic:

- every CLIP model defined twice (~80 duplicated TOML lines added by
  the PE-Core/SigLIP2 batch alone);
- `clip/X` and `tclip/X` are **two independently loaded workers
  holding the same weights** — double VRAM for the crime of wanting
  both jobs;
- the pairing is scattered across ~15 sites in two languages;
- text embeddings from tclip must be *excluded* from normal text
  search (they would tank quality and speed — there is an order of
  magnitude more text than items), which is what the prefix filters do
  — a UI-quality judgment hardcoded as string sniffing.

### 4. Adjacent inconsistencies

- **Audio has no modality concept.** CLAP ships
  `output_type = "clip"` — audio embeddings are stored in the same
  data_type as image embeddings — and the UI tells them apart by
  `name.startsWith("clap")` (`ui/lib/embeddingModels.ts` admits in a
  comment that the prefix is the only signal). There is no `tclap`; no
  way to index text into the CLAP space.
- **`data_type` conflates output kind with modality** (`clip` really
  means "media-side embedding", `text-embedding` "text-side").
- **Asymmetric retrievers can't state their contract.** nemotron
  (`llama-nemotron-embed-vl`) embeds text, images, or text+images as
  corpus, *and* has a query-vs-passage prefix mode. The registry has
  no way to express any of that, so the 2026-07-30 integration
  (a62836f) shipped a stopgap policy — image-embedding entry only, all
  bare text treated as queries — enforced by nothing but a prose
  warning in the description ("do not pair it with a tclip run").
- **stella** needed a one-off `query_prompt_name_map` config key for
  its query prompt modes — a per-impl half-form of the missing
  role signal.
- **jina-clip-api** (an API-served CLIP) is pasted into *three* groups
  (`textembed`, `clip`, `tclip`) with identical config, because API
  execution had no other way to exist.

### 5. The case inventory

Every case below must be representable. They accumulated as separate
annoyances; the design must absorb them as one structure, not as seven
features.

1. **Separate image/text encoder models, one space.** Some embedding
   spaces ship distinct image and text encoding models, both good in
   their own right. Parking the text encoder in tclip would restrict
   it to xmodal similarity — and the image model *cannot encode
   queries*, so image-only indexing becomes unsearchable. Current
   workaround: one impl loads both on demand and pretends to be CLIP.
2. **Omnimodal models**, including audio. Audio embeddings exist but
   were a last-minute shove-in with no representation of their
   modality.
3. **Lightweight query-side models.** Query embedding is VRAM- and
   latency-sensitive: the encoder must be online at search time —
   either resident (fixed VRAM cost) or loaded on demand (slow when
   large). Smaller/quantized query-side models exist for exactly this.
4. **Partial loading.** openclip models can load just the text tower
   (~683 MiB vs ~1.3 GB for PE-Core-L; ~0.7 GB vs ~2 GB for DFN5B) —
   an ideal query-time default (text queries are the primary path),
   with the full model needed only for batch work and hypothetical
   image queries. Unrepresentable today. Unified VLM embedders
   (qwen3-vl, nemotron) *cannot* split — the LLM serves both
   modalities — so tower-splitting can't be assumed structural.
5. **API execution.** A model may be hosted behind an API. Two real
   deployment shapes: (a) index locally on a GPU, query via API for
   latency (and privacy — sending a query is far less sensitive than
   sending the corpus); (b) index via API entirely on GPU-less
   servers. The panoptikon.dev deployment hit this: with jina split
   into its own entries, there was no way to bulk-index locally and
   transfer the DB — the whole corpus had to be indexed through the
   API, slower and costlier — though query-via-API (the reason for the
   setup) works. **Requirement: local and API execution of the same
   nominal model must share one setter identity**, so a corpus indexed
   locally can be *continued* via API (daily increments, GPU no longer
   available) with no rename and no reindex.
6. **Query/corpus asymmetry in the model itself.** Some models need
   different modes or prompt prefixes (or conceivably different
   weights) for embedding queries vs corpus. nemotron's
   query/passage prefixes; stella's prompt map. The worker must know
   which side it is embedding — and the caller *always* knows.
7. **Combined text+image input.** Some models embed a document
   together with its OCR text — a genuinely new job type (not a new
   modality). Out of scope to design fully (see Non-goals), but the
   structure must leave the slot open: at both job-configuration and
   search time the user must be able to distinguish the
   text+image setter from the just-text and just-image setters, even
   though all three live in one space. Sketch of the eventual shape:
   one embedding per (text, image) pair in the item, associated with
   the specific text row — preserving text provenance and filtering.
8. **Weight revisions.** An updated checkpoint arrives,
   space-compatible with its predecessor. There is reason to re-run it
   over everything — but stamping it with the *same* setter is
   impossible under run-once, and today the only path is manually
   deleting old embeddings and re-running under a name that now means
   something else. Legal, but forced and unprincipled. A revision is a
   *different function in the same space* and needs to be expressible
   as exactly that.

Underlying several of these: "the ID is the embedding space" is
*almost* a workable abstraction — if every model targeting a space
produces roughly equivalent results, search-side machinery can treat
the space as the unit. But it disconnects identity from how data was
produced (violating purpose 2) and collapses under case 8. The space
is real, but it is a *comparability claim about functions*, not a
function identity.

### 6. Non-goals

- **Mutable external sources.** The immutable run-once model breaks
  for extractors that query mutable sources (saucenao, the danbooru
  tag finder): a not-found result is placeholder-stamped and never
  re-evaluated; skipping instead would retry forever; and found
  results never update when the source gains new tags. This gap stems
  from the immutability paradigm itself, is orthogonal to embeddings,
  and is **deliberately out of scope** — but the setter-identity
  choices here must not foreclose a future re-evaluation design
  (nothing below does: it only adds structure to setter identity,
  never weakens the stamp).
- **Full design of the items+text job** (case 7). The slot is
  reserved; the details (pairing rules, provenance of multi-text
  items) are deferred.
- **Editing stored `data_type` values.** `clip` / `text-embedding`
  remain as stored strings (renaming them ripples through all of PQL
  for zero user value); they become *derived legacy encoding*
  (§9).

---

## Part II — Design

### 7. Vocabulary

Five concepts, each with exactly one home. Every mechanism in the rest
of the document is an instance of one of these.

| Concept | What it is | Where it appears |
|---|---|---|
| **Entry** (inference ID) | The **declared function**: "the model" in the trust sense — weights + canonical config, hiding implementation. `group/name`. | Registry; the base of every setter; worker identity. |
| **Job** (`@` qualifier) | Which **corpus** the function ran over: `items` (media), `text` (derived text), `files`; open set (`items+text` reserved). | Setter stamps, DB, provenance, job config, cron phases. |
| **Profile** (`:` qualifier) | An **execution backend** of the same declared function: full local, text-tower-only, API client. May override `impl_class`/config wholesale; may **not** change the function. | Registry declarations, run-time backend selection, worker cache keys, VRAM ledger, config pins. **Never** in setter stamps. |
| **Role** | Whether a request embeds **corpus or query**. A per-request field, not part of any ID: the caller always knows it from its own code path. | Worker protocol; impls map it to prefixes/modes (nemotron, stella) or ignore it (openclip). |
| **Space** | A **declared comparability label** across entries. Defaults to the entry's own ID; declared explicitly only when several entries share a space (weight revisions, dedicated query models). | Registry; join/pairing validation; query-encoder candidate widening. Never merges setter identity. |

**Setter = entry `@` job**, with the default job elided (§8.1).
`clip/DFN5B` is the media-embedding setter; `clip/DFN5B@text` is the
same model run over extracted text. The setter remains the immutable
provenance atom and fully determines the function: entry fixes weights
and canonical config, job fixes corpus and input pipeline. Run-once,
coexistence, and reproducibility carry over unchanged — per setter,
exactly as today.

Resolved conceptual points, recorded because they were genuinely
contested during design:

- *Are encode-image and encode-text different functions?* Not per se —
  they are typed inputs of one model (polymorphism), and the model is
  the caching/loading unit, so splitting them into separate IDs
  doubles memory and breaks query association. The **job** is where
  function identity splits, because the job changes what the data
  *is*. That is why the setter is `entry@job` and not
  `entry-per-modality`.
- *Is API execution a different function?* No — it is **declared** to
  be the same function by the registry author (that's what registering
  it as a profile means). If it is not believed to be the same model,
  it gets its own entry. Stamping the venue into the setter would be
  false precision that destroys corpus continuity (case 5). Provenance
  was always nominal (§1); profiles make the trust boundary explicit
  and guard it with checks (§8.3).
- *Is "the ID is the space" wrong?* It is the correct **default**
  (space defaults to the entry ID; the common case is zero-ceremony)
  and the wrong **primitive** (case 8 needs several entries in one
  space; search addressing must stay setter-precise).

### 8. Identity grammar

```
group/name            entry; also the setter for the entry's default job
group/name@text       setter for a non-default job
group/name:tower      a declared execution profile of the entry
```

`@` and `:` are banned characters in group and entry names, enforced
at registry load (currently unenforced; cheap now, painful later).
Qualifiers are only meaningful if declared on the entry.

#### 8.1 Default-job elision

To preserve current setter names and keep names short, the default
job's `@` is omitted — always, canonically:

- A single-source entry's default is its sole job — no declaration
  needed. This covers every currently shipped model: bare `clip/X` is
  the media job, bare `textembed/X` is the text job, bare tagger/OCR
  setters are unchanged.
- Only multi-job entries (the multimodal embedders this design exists
  for) declare `default_job` explicitly; the convention is `items`
  (media is panoptikon's first-class input, and it matches how clip
  names already read).
- **One canonical serializer, normalizing at boundaries.** An explicit
  default (`clip/DFN5B@items`) arriving via API normalizes to the bare
  form; emission always elides the default. The `setters` table
  carries (entry, job) as structured columns — the columns are truth,
  the name is generated serialization for display and transport.
- **The default job is part of the entry's identity contract.**
  Changing it is a migration-level act, never a registry edit: if a
  default silently flipped (e.g. a text-only entry later gains an
  items encoder), the bare name would come to mean a different
  function and collide with stamped rows. Defenses: (a) the rule
  itself, documented; (b) a stamp-time tripwire — refuse to write a
  bare-named setter whose job disagrees with an existing bare-named
  row for the entry, turning silent provenance corruption into a loud
  error demanding a migration.

Migration consequence: the **only rename in the entire system** is
`tclip/X` → `clip/X@text` (§13).

#### 8.2 Jobs

An extraction job is configured as **(entry, job)** — the job chosen
per job-configuration from what the entry supports, instead of being
frozen into group metadata. Everything currently keyed off
`target_entities` re-keys off the job:

- work-query shape (`build_job_pql`, `work_query_keys`) — unchanged
  logic, new key;
- cron phase (`items`/`files` → Source, `text` → Derived) — the
  careful "text producers before text consumers" ordering is
  preserved automatically;
- output handling: job `text` threads `source_data_id` and stamps
  data_type `text-embedding`; job `items` stamps `clip`. The
  `output_type` metadata key retires for embedding groups; the stored
  strings stay (§6).

Validation: the job's required input kinds must be within the entry's
declared encoders (§9). One worker serves all of an entry's jobs and
queries — the impls already handle mixed batches; the clip/tclip
double-load disappears.

#### 8.3 Profiles

A profile declares an alternative way to *execute* the entry's
function:

```toml
[group.clip.inference_ids.DFN5B.profiles.tower]
config.text_tower_only = true
metadata.description = "Text tower only — ~0.7 GB, suitable for keeping resident for queries"

[group.clip.inference_ids.DFN5B.profiles.api]
config.impl_class = "openai-embed-api"        # profiles may swap the impl wholesale
config.base_url   = "..."
external_inputs.api_key = { required = true }
metadata.description = "API-served — instant queries, no VRAM; sends inputs to provider X"
```

Rules:

- **Same declared function.** A profile may change code, venue,
  loading footprint — never the function. Different weights, a
  quantized model, a provider's distinct model → separate entry
  (+ shared space, §10).
- **Provenance-invisible.** Profiles never appear in setter stamps.
  Which profile executed a run is recorded in job logs (debugging,
  auditability) but does not contaminate identity. This is what makes
  case 5 work: bulk-index on `:local` (implicit base profile) with a
  GPU, run tomorrow's incremental cron on `:api`, same setter — no
  rename, no reindex, DB freely transferable between GPU and GPU-less
  hosts.
- **Selectable per run** — for jobs and queries alike. Job configs and
  the query-resolution policy (§11) may pin a profile; `auto` is the
  default.
- **Worker cache key = (entry, profile).** Since inferio keys workers
  by ID string, the suffixed form `clip/DFN5B:tower` *is* the cache
  key. This is also the granularity the VRAM grant ledger needs; API
  profiles cost ~0.
- **The equivalence contract is checked, not assumed.** Every declared
  profile must embed a fixture set and pass a cosine gate against the
  entry's reference profile — at declaration/first-load time. API
  profiles additionally re-check on load (or periodically): a provider
  can silently swap weights out from under the declared identity, and
  the system should detect drift and warn loudly. Nominal trust,
  verified where possible.
- **`external_inputs`** (API keys etc.) attach at the profile level,
  where they are consumed — the existing machinery
  (`docs/inferio-external-inputs.md`) carries label/description/secret
  metadata for the UI prompt.
- Exact instantiation details that don't change the function may ride
  on profiles freely — tower-only provably computes the identical text
  path (same weights, visual tower simply not instantiated; verified
  cos≈1.0 for PE-Core-L and applicable to all CLIP-lineage entries),
  so the backend may serve the `@text` *job* from the tower profile
  when declared, as an invisible optimization.

#### 8.4 Role

Every embed request carries `role: corpus | query`. Extraction always
sends `corpus`; the PQL query paths always send `query`. The worker
maps the role to whatever the model needs — nemotron's
query/passage prefixes, stella's prompt names (subsuming the one-off
`query_prompt_name_map`), a no-op for openclip. This is a worker
protocol addition (`docs/inferio-worker-protocol.md`), and it unwinds
the a62836f stopgap: nemotron's text-as-corpus capability (and later
image+text corpus) becomes configurable instead of forbidden-by-prose.

Role is deliberately **not** in the ID grammar: information the caller
always has contextually belongs in the request, not the name; and
everything that needs to *name* mechanisms (config pins, explain,
cache keys, ledger) uses entry+profile directly.

### 9. Capability metadata

An entry declares what it can consume:

```toml
[group.clip.inference_ids.DFN5B.metadata]
encoders = ["image", "text"]           # open set; "audio", "image+text" later
# input pipeline per encoder (moves from group level):
input_spec.image.handler = "image_frames"
input_spec.image.opts.max_frames = 4
input_spec.text.handler  = "extracted_text"
```

- Job validation: `items` needs at least one media encoder
  (image/audio); `text` needs `text`; `items+text` (future) needs
  `image+text`.
- Query validation: the query's modality must be among the encoders.
- The UI derives everything it currently sniffs from prefixes:
  media-search selectors = setters with job `items`, split image/audio
  by the encoder that feeds them (killing `startsWith("clap")`);
  text-side setters = job `text`.
- **Text-search selectability** (the generalization of the `tclip/`
  exclusion): a per-entry flag, defaulting from capabilities —
  text-only entries opt in, multimodal spaces default out (CLIP text
  towers are poor text-to-text searchers and there's an order of
  magnitude more text than items) — but **overridable**, because the
  default would misclassify models like qwen3-vl whose text side is
  genuinely a strong text embedder.
- Groups survive, demoted to what they actually are: UI tabs and
  shared-config containers. `target_entities`, group-level
  `input_spec`, and `output_type` (for embedding groups) retire from
  group metadata. The `clip` group's name/description are rewritten
  since it now covers both jobs; `tclip` is deleted.

Backward compatibility for user TOMLs: old-style keys degrade
naturally — a group with `target_entities`/`input_spec` defines
single-source entries with one encoder and no profiles. New keys are
opt-in.

### 10. Spaces

`space = "<label>"` in entry metadata; **default = the entry's own
ID**. Declared explicitly only when several entries genuinely share a
space:

- weight revisions: `clip/DFN5B-v2` declares `space = "clip/DFN5B"`.
  Re-running everywhere falls out of run-once for free (a new setter
  has processed nothing); old embeddings are removed by ordinary
  setter deletion; both setters coexist and are individually
  addressable during the transition;
- a dedicated (different-weights) query-side model: its entry declares
  the space and is marked query-capable; it may never index anything
  and never needs a setter;
- a provider's distinct-but-compatible model.

The space is consulted on exactly **two surfaces**, and nowhere else:

1. **Internal join/pairing validation** — which embeddings may be
   JOINed in similarity/xmodal search and grouped into one quant
   space. Replaces `group_spaces`' t-prefix + dim-equality heuristic
   (`vector_quants.rs:477`) with: same space (via setter → entry →
   space), still discriminated by what the embedding is *from* (job /
   data_type), with dim equality demoted from matching heuristic to
   assertion.
2. **Query-encoder resolution** (§11) — which entries' profiles are
   candidates to embed a query against a setter.

The space **never merges setter identity**: searches always name
setters; distances are never compared across setters the user didn't
explicitly co-select. Space is resolved at runtime from the registry
(it is comparability metadata, not provenance — declaring `v2` into a
space later must not require rewriting stored stamps); a setter whose
entry has vanished from the registry falls back to space = its own
entry ID.

### 11. Query-time resolution

The unifying requirement (cases 3–6): **query embedding on a space may
execute differently from corpus embedding** — different request
framing (role), different footprint (tower), different weights
(dedicated query model), different venue (API). One seam serves all
four; the mechanisms are parameters, not features.

Resolution runs **server-side in PQL preprocessing** (where worker
residency, external-input keys, and the registry all live):

```
query(modality M) against setter S
  → S.entry → space
  → candidates = profiles of S.entry able to encode M
              ∪ profiles of other space members declared query-capable
  → policy picks one; request carries role=query
```

- **Capability vs policy.** The registry declares what *exists*
  (profiles, query-capable space members, their trade-offs). System
  config decides what is *used*: per space,
  `query_encoder = "auto"` (default) or a pinned `entry[:profile]`.
  Policy is deployment/user-owned — it depends on VRAM, key
  availability, privacy tolerance.
- **`auto` is deterministic and boring**: resident binding → declared
  query-preferred → the setter's own entry (base profile). **API
  profiles and API entries are never auto-selected** — for queries
  *or* job backends — without explicit opt-in: sending text off-box is
  a privacy decision, sending the corpus more so. A key being present
  is not consent.
- **Residency policy attaches to the resolved binding** — "keep this
  binding warm" is the unit. Preload/prewarm selection changes from
  "embedding setters minus `tclip/`" (`extraction_log.rs:48`) to "the
  resolved query bindings of search-usable setters, deduped by
  (entry, profile)" — which is how the 0.7 GB tower stays resident
  while the 2 GB full model loads only for batch work, and how the
  API-instant path in case 5 is kept instant.
- **Transparent**: the chosen binding is reported in explain output
  (and available next to the existing `EmbedArgs` knobs as a
  per-request override for experimentation).

Addressing rules (settled explicitly):

- **Filters stay setter-addressed.** A space is not a function; "rough
  equivalence" must not be silently load-bearing in a ranking. Where
  multi-setter search is wanted, the filter takes an explicit setter
  *list*; the space powers the UI grouping that makes building the
  list trivial.
- **No silent widening to space siblings.** Precedent: `clip_xmodal`
  is already an explicit opt-in for exactly this ("include the
  same-space text-derived embeddings"), because silent widening tanks
  quality and speed. Weight-revision overlap makes silence worse:
  items indexed by both v1 and v2 would double-report with subtly
  different distance scales, and ranking would quietly prefer
  whichever model's distances run smaller. Widening is always a
  visible act — a flag or an explicit list, deterministic expansion.
- `clip_xmodal`'s implementation changes from name-widening
  (`name = m OR name = 't'||m`) to structural: same entry (or space),
  job `text` — same semantics, no string surgery.

### 12. Discoverability

The user must not have to *know* that a cheaper query mode, a
loadable text tower, or an API-key-for-queries option exists (case 5's
"how do we make this configurable but easy to understand"). Declaring
capabilities in the registry makes the options **enumerable data**:

- The settings UI for an active embedding space asks the server what
  bindings the space's entries declare and renders them: "Full model
  (default) — 2 GB, loaded on demand", "Text tower — 0.7 GB, can stay
  resident", "API — instant, needs a key, sends query text to
  provider X". API-key entry rides the existing `external_inputs`
  prompt machinery.
- Each declared profile carries a human-facing description with its
  trade-off (VRAM, latency, privacy implication) — the existing
  VRAM-in-descriptions convention, made structured.
- Current state is visible: which binding is resident now, which
  served the last query (explain).
- `/api/search/stats` starts returning **structured setter records**
  (entry, job, encoder modality, space, flags) instead of bare
  `(data_type, name)` pairs — the five client-side re-derivations of
  model classification collapse into rendering.

This is purpose 1 doing its job: a new model ships its query options
as metadata; the UI surfaces them with zero per-model frontend work.

### 13. Migration

Per the settings-retirement rule (CLAUDE.md): retiring `tclip` and the
old metadata keys is done by **active migrations that rewrite stored
state at upgrade time** — never load-time aliases alone, never
commit-path rejection.

**Registry** (`python/inferio/config/inference.toml` is shipped code,
not user-seeded — rewrite freely):

- delete `[group.tclip]`; every CLIP entry gains
  `encoders = ["image", "text"]` (+ per-encoder input_spec) in one
  definition;
- collapse the jina triplication into one entry whose base profile is
  the API client;
- nemotron: `encoders = ["image", "text"]`, role-aware impl (the
  encode_queries/encode_documents split already exists in the impl —
  it re-keys off the request role);
- CLAP: `encoders = ["audio", "text"]` (a text job over the CLAP
  space — today's impossible "tclap" — becomes configurable for free,
  if anyone wants cross-modal audio similarity);
- declare `:tower` profiles on CLIP-lineage entries.

**Per-index-DB migration** (one migration, the int8-remap precedent):

- `setters` gains `entry` and `job` columns; backfill: `tclip/X` →
  (`clip/X`, `text`) with **name rewritten** to `clip/X@text`; every
  other row keeps its exact name (bare = default job);
- quant coverage rows keyed by setter follow the rename;
- stored job/cron config referencing `tclip/...` models rewrites to
  (`clip/...`, job `text`).

**Full rename over columns-only** was decided deliberately: keeping
`tclip/X` as a frozen legacy name would leave the old strings in URLs,
logs, and API payloads forever; pre-1.0 with migration machinery in
hand, the prefix should die rather than go dormant.

**What dies** (the deletion inventory):

| Hack | Site |
|---|---|
| `xmodal_text_sibling_name` | `db/vector_quants.rs:48` |
| `format!("t{}")` widening ×2 | `item_similarity.rs:222`, `image_embeddings.rs:208` |
| dim-equality pairing heuristic | `group_spaces`, `vector_quants.rs:477` |
| `tclip/` prefix exclusion | `extraction_log.rs:60` |
| UI prefix filters ×5 + `` `t${model}` `` | see §3 |
| `clap` name-sniffing | `SearchTypeSelector.tsx`, `embeddingModels.ts` |
| duplicate CLIP registry entries | every model ×2, jina ×3 |
| nemotron prose-warning contract | `inference.toml` description |
| `query_prompt_name_map` one-off | stella entries (subsumed by role) |
| clip/tclip double weight loading | worker cache, ~2 GB per CLIP model |

### 14. Open questions

- Concrete TOML key names and the exact shape of per-encoder
  `input_spec` (deep-merge semantics with the existing group/id merge
  need care — `extraction.rs:1237` already special-cases input_spec).
- PQL surface for setter lists (extend existing filters vs a wrapper),
  and URL-encoding of `@` in client query params (benign — it
  percent-encodes — but worth a test).
- Policy config placement: per-DB `system_config` vs server TOML for
  `query_encoder` pins and residency (leaning per-DB, since setters
  and search settings are per-DB).
- Fixture set for the profile equivalence gate (reuse the
  quant-recall fixtures?) and the API drift-check cadence.
- Whether the job-log record of "which profile executed this run"
  should be queryable surface or logs-only.
- The items+text job design (deferred, slot reserved: job qualifier
  `@itemtext` or similar, `image+text` encoder key, one embedding per
  (text, image) pair associated with the text row).
