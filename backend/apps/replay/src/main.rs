use std::collections::BTreeMap;
use std::env;
use std::error::Error;
use std::io;
use std::path::PathBuf;

use engine::MarketDataEngine;
use recorder::{ValidatedCapture, ValidationOptions};

fn main() -> Result<(), Box<dyn Error>> {
    let arguments = ReplayArguments::parse(env::args().skip(1))?;
    let capture = ValidatedCapture::open(
        &arguments.capture_directory,
        ValidationOptions {
            allow_incomplete: arguments.allow_incomplete,
        },
    )?;
    let manifest = capture.manifest().clone();
    let resolved_configuration =
        resolved_configuration(&manifest.configuration, &arguments.configuration_overrides);

    println!(
        "validated capture {} ({:?}), {} segments and {} events",
        manifest.capture_id,
        manifest.capture_status,
        manifest.segments.len(),
        capture.event_count()
    );
    if !arguments.configuration_overrides.is_empty() {
        println!(
            "explicit configuration overrides: {}",
            arguments
                .configuration_overrides
                .iter()
                .map(|(key, value)| format!("{key}={value}"))
                .collect::<Vec<_>>()
                .join(",")
        );
    }

    let mut engine = MarketDataEngine::new();
    let mut engine_error = None;
    capture.for_each_event(|event| {
        if engine_error.is_none() {
            engine_error = engine.process(event.capture_sequence, &event.event).err();
        }
    })?;
    if let Some(error) = engine_error {
        return Err(Box::new(error));
    }
    let report = engine.report();
    println!(
        "replay complete: events={} last_sequence={:?} digest={}",
        report.events_processed, report.last_capture_sequence, report.event_digest_sha256
    );
    println!(
        "resolved configuration entries={}",
        resolved_configuration.len()
    );
    for book in report.order_books {
        println!(
            "{} {} status={:?} source_sequence={:?} best_bid={:?} best_ask={:?}",
            book.venue,
            book.market_coin,
            book.status,
            book.source_sequence,
            book.best_bid,
            book.best_ask
        );
    }
    Ok(())
}

#[derive(Debug, Eq, PartialEq)]
struct ReplayArguments {
    capture_directory: PathBuf,
    allow_incomplete: bool,
    configuration_overrides: BTreeMap<String, String>,
}

impl ReplayArguments {
    fn parse(arguments: impl IntoIterator<Item = String>) -> Result<Self, io::Error> {
        let mut capture_directory = None;
        let mut allow_incomplete = false;
        let mut configuration_overrides = BTreeMap::new();
        let mut arguments = arguments.into_iter();
        while let Some(argument) = arguments.next() {
            match argument.as_str() {
                "--allow-incomplete" => allow_incomplete = true,
                "--config-override" => {
                    let assignment = arguments.next().ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "--config-override requires KEY=VALUE",
                        )
                    })?;
                    let (key, value) = assignment.split_once('=').ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "--config-override requires KEY=VALUE",
                        )
                    })?;
                    if key.is_empty() {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "configuration override key cannot be empty",
                        ));
                    }
                    if configuration_overrides
                        .insert(key.to_owned(), value.to_owned())
                        .is_some()
                    {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("duplicate configuration override: {key}"),
                        ));
                    }
                }
                value if value.starts_with('-') => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("unknown option: {value}"),
                    ));
                }
                value if capture_directory.is_none() => {
                    capture_directory = Some(PathBuf::from(value));
                }
                value => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("unexpected argument: {value}"),
                    ));
                }
            }
        }
        Ok(Self {
            capture_directory: capture_directory.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "usage: replay <capture-directory> [--allow-incomplete] [--config-override KEY=VALUE]",
                )
            })?,
            allow_incomplete,
            configuration_overrides,
        })
    }
}

fn resolved_configuration(
    captured: &BTreeMap<String, String>,
    overrides: &BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    let mut resolved = captured.clone();
    resolved.extend(overrides.clone());
    resolved
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arguments_require_explicit_incomplete_and_configuration_overrides() {
        let parsed = ReplayArguments::parse([
            "/data/capture".into(),
            "--allow-incomplete".into(),
            "--config-override".into(),
            "MODE=research".into(),
        ])
        .unwrap();

        assert!(parsed.allow_incomplete);
        assert_eq!(parsed.capture_directory, PathBuf::from("/data/capture"));
        assert_eq!(
            parsed
                .configuration_overrides
                .get("MODE")
                .map(String::as_str),
            Some("research")
        );
    }

    #[test]
    fn rejects_implicit_or_duplicate_overrides() {
        assert!(ReplayArguments::parse(["/data/capture".into(), "MODE=research".into()]).is_err());
        assert!(
            ReplayArguments::parse([
                "/data/capture".into(),
                "--config-override".into(),
                "MODE=a".into(),
                "--config-override".into(),
                "MODE=b".into(),
            ])
            .is_err()
        );
    }
}
