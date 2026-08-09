"""
OpenTelemetry setup and helpers for context-helpers.

All instrumentation goes through get_tracer() / get_meter() so that
when OTEL is disabled the no-op stubs absorb all calls silently.
"""
from __future__ import annotations

import contextlib
import logging
import subprocess
import time

_tracer_provider = None
_meter_provider = None
logger = logging.getLogger(__name__)


def setup_telemetry(
    service_name: str,
    version: str,
    endpoint: str,
    enabled: bool,
    environment: str = "production",
) -> None:
    """Initialise SDK once. Safe to call multiple times — only first call acts."""
    global _tracer_provider, _meter_provider
    if not enabled or not endpoint or _tracer_provider is not None:
        return

    try:
        from opentelemetry import metrics, trace
        from opentelemetry._logs import set_logger_provider
        from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
        from opentelemetry.exporter.otlp.proto.http.metric_exporter import (
            OTLPMetricExporter,
        )
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
            OTLPSpanExporter,
        )

        # NOTE: the logs SDK lives under the underscore-prefixed module in
        # current opentelemetry releases; `opentelemetry.sdk.logs` does not
        # exist, and importing it here used to abort the ENTIRE telemetry
        # setup (no traces, no metrics) with only a startup warning.
        from opentelemetry.sdk._logs import LoggerProvider
        from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
        from opentelemetry.sdk.metrics import MeterProvider
        from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor

        resource = Resource.create({
            "service.name": service_name,
            "service.version": version,
            "deployment.environment": environment,
        })
        # An endpoint passed explicitly to the HTTP exporters is used as the
        # FULL request URL — the per-signal /v1/<signal> paths are only added
        # when configuring via environment variables. Passing the bare base
        # URL here made every export 404 silently.
        base = endpoint.rstrip("/")

        # Traces
        _tracer_provider = TracerProvider(resource=resource)
        _tracer_provider.add_span_processor(
            BatchSpanProcessor(OTLPSpanExporter(endpoint=f"{base}/v1/traces"))
        )
        trace.set_tracer_provider(_tracer_provider)

        # Metrics
        reader = PeriodicExportingMetricReader(
            OTLPMetricExporter(endpoint=f"{base}/v1/metrics"),
            export_interval_millis=15_000,
        )
        _meter_provider = MeterProvider(resource=resource, metric_readers=[reader])
        metrics.set_meter_provider(_meter_provider)

        # Logs — bridge all stdlib logging to OTEL
        from opentelemetry.sdk._logs import LoggingHandler  # type: ignore[attr-defined]
        log_provider = LoggerProvider(resource=resource)
        log_provider.add_log_record_processor(
            BatchLogRecordProcessor(OTLPLogExporter(endpoint=f"{base}/v1/logs"))
        )
        set_logger_provider(log_provider)
        handler = LoggingHandler(level=logging.INFO, logger_provider=log_provider)
        logging.getLogger().addHandler(handler)

        logger.info(
            "OTEL telemetry enabled",
            extra={"service": service_name, "endpoint": endpoint},
        )
    except ImportError as exc:
        logger.warning("OTEL packages not installed — telemetry disabled: %s", exc)


def get_tracer(name: str):
    if _tracer_provider is None:
        return _NoOpTracer()
    return _tracer_provider.get_tracer(name)


def get_meter(name: str):
    if _meter_provider is None:
        return _NoOpMeter()
    from opentelemetry import metrics
    return metrics.get_meter(name)


def capture_context():
    """Snapshot the current OTel context. Call from the spawning thread."""
    try:
        from opentelemetry import context as otel_context
        return otel_context.get_current()
    except ImportError:
        return None


@contextlib.contextmanager
def run_in_context(ctx):
    """Re-attach a captured context in a background thread."""
    if ctx is None:
        yield
        return
    try:
        from opentelemetry import context as otel_context
        token = otel_context.attach(ctx)
        try:
            yield
        finally:
            otel_context.detach(token)
    except ImportError:
        yield


@contextlib.contextmanager
def jxa_span(tracer, collector_name: str, script_desc: str):
    """Context manager for osascript/JXA subprocess calls."""
    with tracer.start_as_current_span("jxa.run") as span:
        span.set_attribute("collector.name", collector_name)
        span.set_attribute("jxa.script", script_desc)
        t0 = time.monotonic()
        try:
            yield span
        except subprocess.CalledProcessError as exc:
            span.set_attribute("jxa.exit_code", exc.returncode)
            snippet = (exc.stderr or b"")
            if isinstance(snippet, bytes):
                snippet = snippet.decode(errors="replace")
            span.set_attribute("jxa.stderr_snippet", snippet[:300])
            span.record_exception(exc)
            _set_error(span)
            raise
        except Exception as exc:
            span.record_exception(exc)
            _set_error(span)
            raise
        finally:
            span.set_attribute("jxa.duration_ms", round((time.monotonic() - t0) * 1000))


@contextlib.contextmanager
def subprocess_span(tracer, span_name: str, cmd_label: str, **attrs):
    """Context manager for general external subprocess calls (yt-dlp, whisper, etc.)."""
    with tracer.start_as_current_span(span_name) as span:
        span.set_attribute("subprocess.command", cmd_label)
        for k, v in attrs.items():
            span.set_attribute(k, str(v) if not isinstance(v, (bool, int, float)) else v)
        t0 = time.monotonic()
        try:
            yield span
        except subprocess.CalledProcessError as exc:
            span.set_attribute("subprocess.exit_code", exc.returncode)
            span.record_exception(exc)
            _set_error(span)
            raise
        except Exception as exc:
            span.record_exception(exc)
            _set_error(span)
            raise
        finally:
            span.set_attribute("subprocess.duration_ms", round((time.monotonic() - t0) * 1000))


def _set_error(span) -> None:
    try:
        from opentelemetry.trace import StatusCode
        span.set_status(StatusCode.ERROR)
    except ImportError:
        pass


# ---------------------------------------------------------------------------
# No-op stubs — used when OTEL packages are absent or telemetry is disabled
# ---------------------------------------------------------------------------

class _NoOpSpan:
    def __enter__(self): return self
    def __exit__(self, *_): pass
    def set_attribute(self, *_): pass
    def record_exception(self, *_): pass
    def set_status(self, *_): pass
    def add_event(self, *_, **__): pass

    @contextlib.contextmanager
    def start_as_current_span(self, *_, **__):
        yield self


class _NoOpTracer:
    @contextlib.contextmanager
    def start_as_current_span(self, *_, **__):
        yield _NoOpSpan()


class _NoOpMeter:
    def create_counter(self, *_, **__): return _NoOpInstrument()
    def create_histogram(self, *_, **__): return _NoOpInstrument()
    def create_observable_gauge(self, *_, **__): return _NoOpInstrument()


class _NoOpInstrument:
    def add(self, *_, **__): pass
    def record(self, *_, **__): pass
