use opentelemetry::KeyValue;
use opentelemetry::trace::TracerProvider;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider};
use std::time::Duration;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{Layer, Registry, filter};

/// This function declares an empty recipient for tracing events, because otherwise they go to the log
pub fn mute_tracing() {
    Registry::default().init();
}

/// Initializes OpenTelemetry and tracing infrastructure to enable tracing of query execution.
pub fn init_tracing(endpoint: &str) -> Option<SdkTracerProvider> {
    // Set service metadata for tracing.
    let resource = Resource::builder()
        .with_attribute(KeyValue::new("service.name", "vessel"))
        .build();

    // Configure an OTLP exporter to send tracing data.
    let Some(exporter) = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint) // Endpoint for OTLP collector.
        .with_timeout(Duration::from_secs(10))
        .build()
        .ok()
    else {
        return None;
    };

    log::info!("Enabled telemetry to {endpoint}");

    // Create a tracer provider configured with the exporter and sampling strategy.
    let tracer_provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(resource)
        .with_sampler(Sampler::AlwaysOn)
        .build();

    // Obtain a tracer instance for recording tracing information.
    let tracer = tracer_provider.tracer("datafusion-tracing-query");

    // Create a telemetry layer using the tracer to collect and filter tracing data at INFO level.
    let telemetry_layer = tracing_opentelemetry::layer()
        .with_tracer(tracer)
        .with_filter(filter::LevelFilter::INFO);

    let registry = Registry::default().with(telemetry_layer);

    #[cfg(feature = "bench-tracing")]
    let registry = registry.with(bench_tracing::StatisticsLayer::new());

    registry.init();

    // Return the configured tracer provider
    Some(tracer_provider)
}

#[cfg(feature = "bench-tracing")]
mod bench_tracing {
    use nohash_hasher::IntMap;
    use std::collections::HashMap;
    use std::fmt::Debug;
    use std::fs::OpenOptions;
    use std::io::Write;
    use std::marker;
    use std::ops::{AddAssign, DerefMut};
    use std::sync::Mutex;
    use std::time::{Duration, Instant};
    use tracing::field::{Field, Visit};
    use tracing::span::{Attributes, Record};
    use tracing::{Id, Subscriber};
    use tracing_subscriber::Layer;
    use tracing_subscriber::layer::Context;

    #[derive(Debug, Clone)]
    struct RecordedEvent {
        entered: Option<Instant>,
        name: String,
        recorded_duration: Option<Duration>,
    }

    pub(super) struct StatisticsLayer<S> {
        _registry: marker::PhantomData<S>,
        inner: Mutex<DataStore>,
    }

    struct CurrentQuery {
        id: Id,
        query: String,
        entered: Option<Instant>,
        recorded_duration: Option<Duration>,
    }

    #[derive(Default)]
    struct DataStore {
        current_query: Option<CurrentQuery>,
        current_recordings: IntMap<u64, RecordedEvent>,
        current_breakdown: HashMap<String, Duration>,
    }

    struct QueryExtractor(Option<String>);

    struct OtelNameExtractor(Option<String>);

    impl Visit for OtelNameExtractor {
        fn record_str(&mut self, field: &Field, value: &str) {
            if field.name() == "otel.name" && wants_event(value) {
                self.0 = Some(value.to_string());
            }
        }

        fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
            if field.name() == "otel.name" {
                let name = format!("{:?}", value);
                if wants_event(&name) {
                    self.0 = Some(name);
                }
            }
        }
    }

    impl Visit for QueryExtractor {
        fn record_str(&mut self, field: &Field, value: &str) {
            if field.name() == "query" {
                self.0 = Some(value.to_string());
            }
        }

        fn record_debug(&mut self, _: &Field, _: &dyn Debug) {}
    }

    fn wants_event(name: &str) -> bool {
        name == "sql_to_statements"
            || name == "analyze_logical_plan"
            || name == "optimize_logical_plan"
            || name == "do_create_physical_plan"
            || name == "mysql_query_time"
            || name == "decode_array_from_binary"
            || name == "decrypt_array"
            || name == "MySqlScanPlan"
            || name == "FilterExec"
            || name == "handle_query"
    }

    impl<S> StatisticsLayer<S> {
        pub(super) fn new() -> Self {
            Self {
                _registry: marker::PhantomData::default(),
                inner: Mutex::new(DataStore::default()),
            }
        }

        #[inline]
        fn event_name(attrs: &Attributes<'_>) -> Option<String> {
            if attrs.fields().field("otel.name").is_some() {
                let mut visitor = OtelNameExtractor(None);
                attrs.values().record(&mut visitor);
                visitor.0
            } else if wants_event(attrs.metadata().name()) {
                Some(attrs.metadata().name().to_string())
            } else {
                None
            }
        }
    }

    impl<S: Subscriber> Layer<S> for StatisticsLayer<S> {
        fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, _ctx: Context<'_, S>) {
            let Some(name) = Self::event_name(attrs) else {
                return;
            };

            let mut data = self.inner.lock().unwrap();
            if name == "handle_query" {
                data.current_breakdown.clear();
                data.current_recordings.clear();

                let mut query = QueryExtractor(None);
                attrs.values().record(&mut query);
                let Some(query) = query.0 else {
                    log::error!("Found a handle_query event with no query field");
                    return;
                };

                data.current_query = Some(CurrentQuery {
                    recorded_duration: None,
                    entered: None,
                    id: id.clone(),
                    query,
                });
            } else if data.current_query.is_some() {
                data.current_recordings.insert(
                    id.into_u64(),
                    RecordedEvent {
                        name: name.to_string(),
                        entered: None,
                        recorded_duration: None,
                    },
                );
            }
        }

        fn on_record(&self, id: &Id, values: &Record<'_>, _ctx: Context<'_, S>) {
            let mut visitor = OtelNameExtractor(None);
            values.record(&mut visitor);

            if let Some(name) = visitor.0 {
                let mut data = self.inner.lock().unwrap();

                if data.current_query.is_none() {
                    return;
                }

                data.current_recordings.insert(
                    id.into_u64(),
                    RecordedEvent {
                        name,
                        entered: None,
                        recorded_duration: None,
                    },
                );
            }
        }

        fn on_enter(&self, id: &Id, _ctx: Context<'_, S>) {
            let mut data = self.inner.lock().unwrap();
            let Some(current) = data.current_query.as_mut() else {
                return;
            };

            if id == &current.id && current.entered.is_none() {
                current.entered = Some(Instant::now());
                current.recorded_duration = None;
                return;
            }

            if let Some(rec) = data.current_recordings.get_mut(&id.into_u64())
                && rec.entered.is_none()
            {
                rec.entered = Some(Instant::now());
                rec.recorded_duration = None;
            }
        }

        fn on_exit(&self, id: &Id, _ctx: Context<'_, S>) {
            let mut data = self.inner.lock().unwrap();
            let Some(current) = data.current_query.as_mut() else {
                return;
            };

            if id == &current.id {
                let Some(entered) = current.entered.clone() else {
                    return;
                };

                current.recorded_duration = Some(entered.elapsed());
                return;
            }

            if let Some(rec) = data.current_recordings.get_mut(&id.into_u64()) {
                let Some(entered) = rec.entered.clone() else {
                    return;
                };

                if rec.name == "MySqlScanPlan" || rec.name == "FilterExec" {
                    // For these plans, we want to record only when they are entered (otherwise we double count a lot of time)
                    // So we immediately count them
                    let name = rec.name.clone();
                    rec.entered = None;
                    data.current_breakdown
                        .entry(name)
                        .or_default()
                        .add_assign(entered.elapsed());
                } else {
                    // For other steps, we record the time between first entrance and last seen exit (in `on_close`)
                    rec.recorded_duration = Some(entered.elapsed());
                }
            }
        }

        fn on_close(&self, id: Id, _ctx: Context<'_, S>) {
            let mut data = self.inner.lock().unwrap();
            let Some(current) = data.current_query.as_mut() else {
                return;
            };

            if id == current.id {
                // Record the query to the file and reset
                let mut file = OpenOptions::new()
                    .append(true)
                    .create(true)
                    .open("query_breakdown.txt")
                    .expect("failed to open query_breakdown.txt");

                writeln!(
                    file,
                    "Query: {}\n\
                    Timings (microseconds):\n\
                    Total time: {}",
                    current.query,
                    current
                        .recorded_duration
                        .expect("no recorded duration")
                        .as_micros()
                )
                .expect("failed to write to query_breakdown.txt");

                for (event, total_duration) in data.current_breakdown.iter() {
                    writeln!(file, "{event}: {}", total_duration.as_micros())
                        .expect("failed to write to query_breakdown.txt");
                }

                writeln!(file).unwrap();

                let _ = std::mem::replace(data.deref_mut(), DataStore::default());

                return;
            }

            if let Some(rec) = data.current_recordings.remove(&id.into_u64()) {
                data.current_breakdown
                    .entry(rec.name)
                    .or_default()
                    .add_assign(rec.recorded_duration.unwrap_or_default());
            }
        }

        fn on_id_change(&self, _old: &Id, _new: &Id, _ctx: Context<'_, S>) {
            let mut data = self.inner.lock().unwrap();
            let Some(current) = data.current_query.as_mut() else {
                return;
            };

            if &current.id == _old {
                current.id = _new.clone();
            }

            if let Some(entry) = data.current_recordings.remove(&_old.into_u64()) {
                data.current_recordings.insert(_new.into_u64(), entry);
            }
        }
    }
}
