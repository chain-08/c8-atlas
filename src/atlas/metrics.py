from prometheus_client import Counter, Gauge, Histogram

ATLAS_INCIDENTS = Counter("atlas_incidents_total", "Total incidents seen by Atlas", ["env", "type"])
ATLAS_FAILED_DAGS = Gauge("atlas_failed_dags", "Number of DAGs with failures", ["env"])
ATLAS_LATE_DAGS = Gauge("atlas_late_dags", "Number of DAGs with late runs", ["env"])
ATLAS_POLL_LATENCY = Histogram("atlas_airflow_poll_latency_ms", "Airflow poll latency (ms)", buckets=[50,100,250,500,1000,2000,5000])
