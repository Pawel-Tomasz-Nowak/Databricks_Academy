"""LAB 4 Source Modules

Python modules for Silver Layer data pipeline with SCD Type I/II support.

Modules:
    config: Configuration and table name definitions
    data_generators: Synthetic streaming event data generation
    data_cleaner: Data quality transformations
    data_mergers: Delta Lake merge operations for SCD patterns
    preprocessing: End-to-end ETL orchestration

Author: Paweł Nowak
Date: 2026-08-10
"""

__version__ = "1.0.0"
__all__ = ["config", "data_generators", "data_cleaner", "data_mergers", "preprocessing"]
