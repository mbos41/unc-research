# UNC Chapel Hill Research

## Descriptions:

### bank_merge_script
* For each date merges several sources of agent information onto call log file
* Some tables are aggregated prior to merging (# of actions for given agent prior to call)
* Takes into account overnight shifts 

### concat_year.py
* Concatenates all csv files for a single year within each month's directory

### create_dirs.py
* Creates directory structure with a new folder for each month April 2007 - June 2009

### create_tables.py
* Reads in collection of Microsoft Access database files (one for each date)
* Creates csv files for each date/table combination and master csv files for each table

