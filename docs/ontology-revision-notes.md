# Ontology Revision Notes

Date: 2026-03-25

This pass updated the working beekeeping ontology in [E:\n8n to python\data\beecore.ttl](E:\n8n%20to%20python\data\beecore.ttl) to better match the local book corpus without starting a new ingestion run.

## Books sampled

- `4-H-1059-W.pdf`
- `A_Practical_Manual_of_Beekeeping.pdf`
- `Beekeeping-Basics.pdf`
- `DadantFirstLessonsInBeekeeping.pdf`
- `TheBeekeepersHandbok.pdf`
- `cb5353en.pdf`
- additional title and heading checks across the other workspace PDFs

## Main ontology additions

### Colony and bee forms

- `NucleusColony`
- `PackageBees`
- `Swarm`
- `HoneyBee`
- `StinglessBee`

### Hive types and components

- `HiveType`
- `LangstrothHive`
- `MovableFrameHive`
- `Skep`
- `TopBarHive`
- `BottomBoard`
- `EntranceReducer`
- `InnerCover`
- `OuterCover`
- `Super`
- `Foundation`
- `CombCell`
- `QueenCell`
- `WorkerCell`
- `DroneCell`
- `QueenExcluder`

### Forage and products

- `FloralSource`
- `Water`
- `PropolisResin`
- `CombHoney`
- `CreamedHoney`
- `BeeVenom`
- `BeePollen`

### Health and management

- `Predator`
- `EuropeanFoulbrood`
- `WaxMoth`
- `Requeening`
- `Splitting`
- `Harvesting`
- `Wintering`
- `InstallingPackage`
- `UnitingColonies`
- `ProtectiveGear`
- `Feeder`
- `Smoker`
- `HiveTool`
- `Extractor`
- `Veil`
- `Gloves`
- `QueenCage`
- `Season`
- `Spring`
- `Summer`
- `Autumn`
- `Winter`

## Added object properties

- `locatedAt`
- `hasQueen`
- `hasBrood`
- `containsCellType`
- `containsProduct`
- `collects`
- `foragesOn`
- `performedDuring`
- `targets`
- `prevents`
- `rears`

## Ontology loader changes

The runtime loader in [E:\n8n to python\src\bee_ingestion\kg.py](E:\n8n%20to%20python\src\bee_ingestion\kg.py) now reads `skos:altLabel` and folds those labels into:

- ontology tagging for chunks
- canonicalization aliases for entity normalization

This means the revised ontology is usable immediately by the KG and metadata layers once ingestion or reprocessing is run later.

## Not done in this pass

- no ingestion was started
- no KG replay was started
- no ontology-to-book gold evaluation set was created yet

The next ontology step should be to compare one or two candidate alternative ontologies against this revised `beecore.ttl` and then choose the final control vocabulary before a broader ingest.
