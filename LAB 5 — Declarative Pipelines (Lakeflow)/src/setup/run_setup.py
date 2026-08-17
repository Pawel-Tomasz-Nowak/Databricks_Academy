"""Job entrypoint for UC setup bootstrap."""

from etl_package.setup.music_pipeline_setup import bootstrap_infrastructure


bootstrap_infrastructure()