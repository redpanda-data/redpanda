# Type stubs for prometheus_client

# Re-export commonly used items
from .metrics_core import Metric as Metric
from .parser import text_string_to_metric_families as text_string_to_metric_families

__all__ = ["Metric", "text_string_to_metric_families"]
