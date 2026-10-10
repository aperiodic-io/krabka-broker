//! Plotly HTML graph rendering for the benchmark report.
//!
//! This module turns the cross-run aggregates from [`crate::aggregate`] into one
//! self-contained HTML page:
//!
//! - a grouped **bar chart per headline metric**, krabka against kafka across
//!   every scenario cell, with run-to-run sample-stddev error bars; and
//! - one **across-run-averaged time-series line chart** per
//!   `(scenario, metric)`, which shows how the metric moved over the run for
//!   each stack.
//!
//! The page loads `plotly.js` from the CDN and matches the version the `plotly`
//! crate itself targets. It embeds each figure inline, so the whole report is
//! a single `.html` file.

use std::{
    collections::BTreeMap,
    fmt::{Arguments, Write},
};

use krabka_units::prelude::*;
use plotly::{
    Bar, Layout, Plot, Scatter,
    common::{ErrorData, ErrorType, Line, Mode, Title},
    layout::{Axis, BarMode},
};

use crate::{
    aggregate::{
        CellAgg, CellKey, ScalarMetric, TsSeries, aggregate_cells, averaged_timeseries,
        scalar_metrics,
    },
    ids::TimeOffsetMs,
    numeric::mebibytes_f64,
    scenario::{RunOutput, Stack},
};

fn push_fmt(output: &mut String, args: Arguments<'_>) {
    output
        .write_fmt(args)
        .expect("writing formatted data to a String cannot fail");
}

/// The plotly.js version that the `plotly` crate (0.14) renders against. Pin the
/// same one, so the inline figures find a compatible global.
///
/// This is for the website fragment alone. The fragment is inlined into a page
/// the site serves, that page is read online, and a second copy of a 4.5 MB
/// bundle in every build of the site is not worth it. The standalone report
/// carries the bundle instead: see [`wrap_page`].
const PLOTLY_CDN: &str = "https://cdn.plot.ly/plotly-3.0.1.min.js";

/// Renders every run into one self-contained HTML report with the title
/// `title`.
#[must_use]
pub fn render_html(runs: &[RunOutput], title: &str) -> String {
    let cells = aggregate_cells(runs);
    let ts = averaged_timeseries(runs);

    let mut body = String::new();
    if cells.is_empty() {
        body.push_str("<p>No runs to report.</p>\n");
        return wrap_page(title, &body);
    }

    body.push_str("<h2>Summary — mean ± stddev across runs</h2>\n");
    for m in scalar_metrics() {
        let plot = bar_chart(&cells, &m);
        body.push_str("<div class=\"plot\">");
        body.push_str(&plot.to_inline_html(Some(&format!("bar-{}", m.key))));
        body.push_str("</div>\n");
    }

    if !ts.is_empty() {
        body.push_str("<h2>Over the run — averaged across runs</h2>\n");
        body.push_str(&timeseries_charts(&ts));
    }

    wrap_page(title, &body)
}

/// Renders an HTML *fragment* with no `<html>` wrapper, for the Zola website to
/// embed. The fragment holds the averaged scalar summary, and then throughput,
/// CPU, and memory over the run window for each scenario. Every individual run
/// is a faint line, and the across-run mean is a bold line. `tagged` pairs each
/// run with the `runNN` tag parsed from the result filename, so every per-run
/// trace carries a name.
#[must_use]
pub fn render_web_fragment(tagged: &[(String, RunOutput)]) -> String {
    let runs: Vec<RunOutput> = tagged.iter().map(|(_, r)| r.clone()).collect();
    let cells = aggregate_cells(&runs);
    if cells.is_empty() {
        return "<p>No benchmark runs available yet.</p>\n".to_string();
    }
    let ts = averaged_timeseries(&runs);

    let mut out = String::new();
    push_fmt(
        &mut out,
        format_args!("<script src=\"{PLOTLY_CDN}\" charset=\"utf-8\"></script>\n"),
    );

    out.push_str(
        "<h2 id=\"summary\">Summary — mean across runs (error bars = run-to-run stddev)</h2>\n",
    );
    for m in scalar_metrics() {
        let plot = bar_chart(&cells, &m);
        out.push_str("<div class=\"bench-plot\">");
        out.push_str(&plot.to_inline_html(Some(&format!("bench-bar-{}", m.key))));
        out.push_str("</div>\n");
    }

    out.push_str(
        "<h2 id=\"per-run\">Per run &amp; averaged — throughput, CPU and memory over the run</h2>\n",
    );
    // Group runs into cells, preserving each run's tag for the faint per-run lines.
    let mut by_cell: BTreeMap<CellKey, Vec<(&str, &RunOutput)>> = BTreeMap::new();
    for (tag, r) in tagged {
        by_cell
            .entry(CellKey::of(r))
            .or_default()
            .push((tag.as_str(), r));
    }
    for (key, cell_runs) in &by_cell {
        push_fmt(
            &mut out,
            format_args!("<h3>{}</h3>\n", escape(&key.label())),
        );
        for wm in &web_metrics() {
            let plot = per_run_chart(key, cell_runs, wm, &ts);
            let id = sanitize_id(&format!("bench-ts-{}-{}", key.label(), wm.avg_key));
            out.push_str("<div class=\"bench-plot\">");
            out.push_str(&plot.to_inline_html(Some(&id)));
            out.push_str("</div>\n");
        }
    }
    out
}

/// A website time-series metric. It holds the matching [`averaged_timeseries`]
/// key for the bold mean line, a label, and a way to pull a run's
/// `(t_offset_ms, value)` points for the faint per-run lines.
struct WebMetric {
    avg_key: &'static str,
    label: &'static str,
    per_run: fn(&RunOutput) -> Vec<(TimeOffsetMs, f64)>,
}

fn web_metrics() -> Vec<WebMetric> {
    vec![
        WebMetric {
            avg_key: "producer_msgs_per_sec",
            label: "Throughput (producer msgs/s)",
            per_run: |r| {
                r.samples
                    .iter()
                    .map(|s| (s.t_offset_ms, s.producer_rate.per_sec_f64()))
                    .collect()
            },
        },
        WebMetric {
            avg_key: "broker_cpu_cores",
            label: "Broker CPU (cores)",
            per_run: |r| {
                r.broker_samples
                    .iter()
                    .map(|b| (b.t_offset_ms, b.cpu_cores))
                    .collect()
            },
        },
        WebMetric {
            avg_key: "broker_mem_working_set_mb",
            label: "Broker memory (MiB)",
            per_run: |r| {
                r.broker_samples
                    .iter()
                    .map(|b| (b.t_offset_ms, mebibytes_f64(b.mem_working_set)))
                    .collect()
            },
        },
    ]
}

/// `(bold mean line colour, faint per-run line colour)` per stack.
fn stack_colors(s: Stack) -> (&'static str, &'static str) {
    match s {
        Stack::Krabka => ("rgb(234,88,12)", "rgba(234,88,12,0.20)"),
        Stack::Kafka => ("rgb(37,99,235)", "rgba(37,99,235,0.20)"),
    }
}

/// One chart for `wm` in one cell. Every run is a faint line that the legend
/// hides, and each stack also gets a bold across-run mean line.
fn per_run_chart(
    key: &CellKey,
    cell_runs: &[(&str, &RunOutput)],
    wm: &WebMetric,
    ts: &[TsSeries],
) -> Plot {
    let mut plot = Plot::new();
    for stack in [Stack::Krabka, Stack::Kafka] {
        let (mean_color, faint_color) = stack_colors(stack);

        for (tag, run) in cell_runs.iter().filter(|(_, r)| r.stack == stack) {
            let pts = (wm.per_run)(run);
            if pts.is_empty() {
                continue;
            }
            let x: Vec<f64> = pts.iter().map(|(t, _)| t.as_time().secs_f64()).collect();
            let y: Vec<f64> = pts.iter().map(|(_, v)| *v).collect();
            plot.add_trace(
                Scatter::new(x, y)
                    .mode(Mode::Lines)
                    .name(format!("{} {tag}", stack.as_str()))
                    .legend_group(stack.as_str())
                    .show_legend(false)
                    .line(Line::new().color(faint_color).width(1.0)),
            );
        }

        if let Some(series) = ts
            .iter()
            .find(|s| &s.key == key && s.stack == stack && s.metric == wm.avg_key)
        {
            let x: Vec<f64> = series
                .points
                .iter()
                .map(|p| p.t_offset_ms.as_time().secs_f64())
                .collect();
            let y: Vec<f64> = series.points.iter().map(|p| p.mean).collect();
            let n = series.points.iter().map(|p| p.n).max().unwrap_or(0);
            plot.add_trace(
                Scatter::new(x, y)
                    .mode(Mode::Lines)
                    .name(format!("{} (mean of {n})", stack.as_str()))
                    .legend_group(stack.as_str())
                    .line(Line::new().color(mean_color).width(3.0)),
            );
        }
    }
    plot.set_layout(
        Layout::new()
            .title(Title::with_text(format!("{} — {}", key.label(), wm.label)))
            .height(360)
            .x_axis(Axis::new().title(Title::with_text("seconds into run")))
            .y_axis(Axis::new().title(Title::with_text(wm.label))),
    );
    plot
}

/// One grouped bar chart for metric `m`. Each cell gets a krabka bar and a kafka
/// bar, and each bar carries the across-run sample-stddev as a symmetric error
/// bar.
fn bar_chart(cells: &[CellAgg], m: &ScalarMetric) -> Plot {
    let labels: Vec<String> = cells.iter().map(|c| c.key.label()).collect();

    let stack_series = |pick: fn(&CellAgg) -> &crate::aggregate::StackAgg| {
        let mut means = Vec::with_capacity(cells.len());
        let mut errs = Vec::with_capacity(cells.len());
        for c in cells {
            let s = pick(c).metrics.get(m.key).copied().unwrap_or_default();
            means.push(s.mean);
            errs.push(s.stddev);
        }
        (means, errs)
    };
    let (cy, ce) = stack_series(|c| &c.krabka);
    let (ky, ke) = stack_series(|c| &c.kafka);

    let mut plot = Plot::new();
    plot.add_trace(
        Bar::new(labels.clone(), cy)
            .name("krabka")
            .error_y(ErrorData::new(ErrorType::Data).array(ce)),
    );
    plot.add_trace(
        Bar::new(labels, ky)
            .name("kafka")
            .error_y(ErrorData::new(ErrorType::Data).array(ke)),
    );
    plot.set_layout(
        Layout::new()
            .title(Title::with_text(format!("{} ({})", m.label, m.unit)))
            .bar_mode(BarMode::Group)
            .height(420)
            .y_axis(Axis::new().title(Title::with_text(m.unit))),
    );
    plot
}

/// One line chart per `(scenario, metric)`. Each chart holds a krabka line and a
/// kafka line of the across-run-averaged value over the run.
fn timeseries_charts(ts: &[TsSeries]) -> String {
    let mut groups: BTreeMap<(&CellKey, &'static str), Vec<&TsSeries>> = BTreeMap::new();
    for s in ts {
        groups.entry((&s.key, s.metric)).or_default().push(s);
    }

    let mut out = String::new();
    for (&(key, metric), series) in &groups {
        let label = ts_metric_label(metric);
        let mut plot = Plot::new();
        for s in series {
            let x: Vec<f64> = s
                .points
                .iter()
                .map(|p| p.t_offset_ms.as_time().secs_f64())
                .collect();
            let y: Vec<f64> = s.points.iter().map(|p| p.mean).collect();
            plot.add_trace(Scatter::new(x, y).name(s.stack.as_str()).mode(Mode::Lines));
        }
        plot.set_layout(
            Layout::new()
                .title(Title::with_text(format!("{} — {label}", key.label())))
                .height(340)
                .x_axis(Axis::new().title(Title::with_text("seconds into run")))
                .y_axis(Axis::new().title(Title::with_text(label))),
        );
        let id = sanitize_id(&format!("ts-{}-{metric}", key.label()));
        out.push_str("<div class=\"plot\">");
        out.push_str(&plot.to_inline_html(Some(&id)));
        out.push_str("</div>\n");
    }
    out
}

/// The label a person reads for a time-series metric key. The keys are a closed
/// set. See [`crate::aggregate`]. The fallback is unreachable in practice.
fn ts_metric_label(key: &str) -> &'static str {
    match key {
        "producer_msgs_per_sec" => "Producer throughput (msgs/s)",
        "consumer_msgs_per_sec" => "Consumer throughput (msgs/s)",
        "producer_p99_ms" => "Producer ack p99 (ms)",
        "consumer_e2e_p99_ms" => "Consumer e2e p99 (ms)",
        "broker_cpu_cores" => "Broker CPU (cores)",
        "broker_mem_working_set_mb" => "Broker working set (MiB)",
        _ => "value",
    }
}

/// Reduces an arbitrary string to a safe HTML id of alphanumerics and dashes.
fn sanitize_id(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect()
}

/// Wrap the rendered charts in a page that carries everything it needs.
///
/// The report is handed around as an artifact: attached to a run, opened weeks
/// later, read on a machine behind a proxy. A `<script src>` to a CDN makes
/// every chart in it depend on that host still serving that file, so an
/// artifact that is otherwise complete opens with no plots at all.
/// [`plotly::Plot::offline_js_sources`] returns the same plotly.js the crate
/// renders against, as inline script tags. The page grows by about 6 MB and
/// stops depending on anything.
fn wrap_page(title: &str, body: &str) -> String {
    let t = escape(title);
    let scripts = Plot::offline_js_sources();
    format!(
        "<!doctype html>\n<html lang=\"en\">\n<head>\n<meta charset=\"utf-8\">\n\
<title>{t}</title>\n{scripts}\n\
<style>body{{font-family:system-ui,sans-serif;margin:24px;max-width:1100px}}\
h1{{font-size:1.6rem}}h2{{margin-top:2rem;border-bottom:1px solid #ddd;padding-bottom:4px}}\
.plot{{margin:10px 0}}</style>\n</head>\n<body>\n<h1>{t}</h1>\n{body}\n</body>\n</html>\n"
    )
}

/// Minimal HTML-text escaping for trusted scenario and metric labels.
fn escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::{
        ids::TimeOffsetMs,
        scenario::{
            Sample,
            fixture::{RunSetup, empty_run as run},
        },
    };

    fn sample(t: u64, producer_rate: Frequency) -> Sample {
        Sample {
            t_offset_ms: TimeOffsetMs(t),
            producer_rate,
            consumer_rate: producer_rate * 0.9,
            producer_p50: millis(1),
            producer_p99: millis(4),
            consumer_e2e_p99: millis(7),
        }
    }

    #[test]
    fn render_html_embeds_library_bars_and_timeseries() {
        let runs = vec![
            run(RunSetup {
                samples: vec![sample(0, per_sec(1000)), sample(2000, per_sec(1100))],
                p99: millis(5),
                ..Default::default()
            }),
            run(RunSetup {
                stack: Stack::Kafka,
                producer_rate: per_sec(800),
                samples: vec![sample(0, per_sec(800)), sample(2000, per_sec(820))],
                p99: millis(7),
                ..Default::default()
            }),
            run(RunSetup {
                scenario: "fan-out",
                producer_rate: per_sec(500),
                p99: millis(9),
                ..Default::default()
            }),
            run(RunSetup {
                scenario: "fan-out",
                stack: Stack::Kafka,
                producer_rate: per_sec(400),
                p99: millis(11),
                ..Default::default()
            }),
        ];
        let html = render_html(&runs, "Krabka vs Strimzi");

        // Valid page that embeds real figures, represents both scenarios and
        // both stacks, and carries a headline bar metric plus a time-series
        // chart.
        for needle in [
            "<html",
            "Plotly.newPlot",
            "Krabka vs Strimzi",
            "small-msg",
            "fan-out",
            "krabka",
            "kafka",
            "Producer throughput",
            "seconds into run",
        ] {
            assert2::assert!(html.contains(needle));
        }
        // The page carries plotly.js rather than a link to it, so it renders
        // with no network. The bundle's own text mentions the CDN, so the check
        // is on the `<script src>` tag and not on the host name.
        assert2::assert!(!html.contains("<script src=\"https://cdn.plot.ly"));
        assert2::assert!(html.len() > 1_000_000, "page is {} bytes", html.len());
    }

    #[test]
    fn render_html_empty_runs_is_a_valid_page() {
        let html = render_html(&[], "Empty");
        assert2::assert!(html.contains("<html") && html.contains("Empty"));
        assert2::assert!(html.contains("No runs"));
    }

    #[test]
    fn web_fragment_has_per_run_and_mean_for_cpu_mem_throughput() {
        use crate::scenario::BrokerSample;
        let mk = |stack, prod: Frequency, cpu: f64, mem: ByteSize| {
            let mut r = run(RunSetup {
                stack,
                producer_rate: prod,
                samples: vec![sample(0, prod), sample(2000, prod * 1.1)],
                p99: millis(5),
                ..Default::default()
            });
            r.broker_samples = vec![
                BrokerSample {
                    t_offset_ms: TimeOffsetMs(0),
                    cpu_cores: cpu,
                    mem_working_set: mem,
                },
                BrokerSample {
                    t_offset_ms: TimeOffsetMs(2000),
                    cpu_cores: cpu,
                    mem_working_set: mem,
                },
            ];
            r
        };
        let tagged = vec![
            (
                "run01".to_string(),
                mk(Stack::Krabka, per_sec(1000), 2.0, mebibytes(300)),
            ),
            (
                "run02".to_string(),
                mk(Stack::Krabka, per_sec(1200), 2.2, mebibytes(320)),
            ),
            (
                "run01".to_string(),
                mk(Stack::Kafka, per_sec(800), 3.5, mebibytes(2000)),
            ),
            (
                "run02".to_string(),
                mk(Stack::Kafka, per_sec(820), 3.6, mebibytes(2100)),
            ),
        ];
        let html = render_web_fragment(&tagged);
        // The fragment is inlined into a page the site serves online, so it
        // keeps the CDN reference the standalone report drops. It charts
        // throughput/CPU/memory per run for both stacks, and labels the bold
        // mean line with the run count ("mean of 2").
        for needle in [
            "<script src=\"https://cdn.plot.ly/plotly-3.0.1",
            "Plotly.newPlot",
            "Per run",
            "small-msg",
            "Throughput",
            "Broker CPU",
            "Broker memory",
            "mean of 2",
            "krabka",
            "kafka",
        ] {
            assert2::assert!(html.contains(needle));
        }
    }

    #[test]
    fn web_fragment_empty_is_graceful() {
        assert2::assert!(render_web_fragment(&[]).contains("No benchmark runs"));
    }
}
