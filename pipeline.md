```mermaid
%%{ init: { 'flowchart': { 'curve': 'monotoneX' } } }%%
graph LR;
CSV_data_source[CSV data source] -->|csv-data|Data_enrichment[Data enrichment];
Data_enrichment[Data enrichment] -->|enriched-data|Console_logger[Console logger];
Lookup_data_ingestion[Lookup data ingestion]

```