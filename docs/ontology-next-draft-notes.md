# Ontology Next Draft Notes

Date: 2026-03-28

This note documents the non-applied ontology draft in [E:\n8n to python\data\beecore.next-draft.ttl](E:\n8n%20to%20python\data\beecore.next-draft.ttl). It was prepared while the live re-ingest was running and does not affect the current runtime.

## Basis

The draft was derived from:

- the current live ontology in [E:\n8n to python\data\beecore.ttl](E:\n8n%20to%20python\data\beecore.ttl)
- the prior revision notes in [E:\n8n to python\docs\ontology-revision-notes.md](E:\n8n%20to%20python\docs\ontology-revision-notes.md)
- a read-only corpus term scan across the workspace PDFs

## High-frequency corpus concepts missing from the live ontology

The scan showed repeated use of the following concepts that are not modeled explicitly enough in the live file:

- `queen cell`
- `sugar syrup`
- `robbing`
- `ventilation`
- `brood nest`
- `honey flow`
- `candy`
- `supersedure`
- `bee escape`
- `drone comb`
- `bee bread`
- `section box`
- `section honey`
- `bee space`
- `queen cup`
- `mating flight`
- `orientation flight`
- `honey house`
- `absconding`

## Draft direction

The draft is intentionally additive and separated from the live ontology.

It adds four groups of concepts:

### 1. Feeding and stored resources

- `SupplementalFeed`
- `SugarSyrup`
- `CandyFeed`
- `BeeBread`
- `SectionHoney`

These appear often in management and feeding passages and help disambiguate:

- colony-gathered resources
- processed hive products
- beekeeper-supplied feed

### 2. Hive structure and equipment

- `BroodNest`
- `DroneComb`
- `QueenCup`
- `SectionBox`
- `BeeEscape`
- `HoneyHouse`
- `BeeSpace`

These are repeatedly described in practical manuals and are useful for both chunk ontology tagging and KG extraction.

### 3. Colony processes and events

- `ColonyProcess`
- `HoneyFlow`
- `RobbingEvent`
- `AbscondingEvent`
- `SupersedureEvent`
- `OrientationFlight`
- `MatingFlight`

These are not intended for immediate live use because the current graph is still relation-centric, but they are the right vocabulary if the event layer is reintroduced later.

### 4. Missing management actions

- `VentilationManagement`
- `PreventingRobbing`

These map cleanly to the current management-action branch and should be usable even before any event-layer expansion.

## Draft properties

The draft also proposes a small set of properties that fill real gaps in the current schema:

- `feedsWith`
- `usesComponent`
- `occupiesBroodNest`
- `storesResource`
- `maintainsBeeSpace`
- `occursInColony`
- `occursDuringSeason`
- `involvesBeeType`

## Recommended adoption order

If this draft is accepted later, the safest order is:

1. merge only the feeding, hive-structure, and management-action additions
2. map the KG validator/canonicalizer to those additions
3. keep the colony-process/event classes disabled in extraction prompts until event persistence exists again
4. only then consider making the event branch first-class in KG output

## Why this is separate

The current live re-ingest is running on the existing ontology. This draft is intentionally separate so ontology changes do not mutate the active corpus build mid-run.
