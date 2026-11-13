//! `cargo xtask support-window`: is the tested matrix still the set the vendors support?
//!
//! The matrix is `crates/rustcdc/tests/support/matrix.txt`, the one place the suites take their
//! versions from. The windows come from endoflife.date, which tracks each vendor's page. The
//! answer is a list of actions — add, drop, promote — and a non-zero exit when there are any, or
//! when a window could not be read: "could not check" must never print as "current".

use std::cmp::Ordering;
use std::process::Command;

const MATRIX_PATH: &str = "crates/rustcdc/tests/support/matrix.txt";
const MATRIX: &str = include_str!("../../rustcdc/tests/support/matrix.txt");

/// Which supported releases an engine's matrix must hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Must {
    /// Every release still inside its window.
    EverySupported,
    /// Every long-term release still inside its window; Innovation and rolling lines are not.
    EveryLts,
    /// Only the newest release (a sink, tested at one version).
    Newest,
}

struct Rule {
    engine: &'static str,
    product: &'static str,
    must: Must,
    vendor: &'static str,
}

const RULES: &[Rule] = &[
    Rule {
        engine: "postgres",
        product: "postgresql",
        must: Must::EverySupported,
        vendor: "https://www.postgresql.org/support/versioning/",
    },
    Rule {
        engine: "mysql",
        product: "mysql",
        must: Must::EveryLts,
        vendor: "https://dev.mysql.com/doc/refman/9.7/en/mysql-releases.html",
    },
    Rule {
        engine: "mariadb",
        product: "mariadb",
        must: Must::EveryLts,
        vendor: "https://mariadb.org/about/#maintenance-policy",
    },
    Rule {
        engine: "sqlserver",
        product: "mssqlserver",
        must: Must::EverySupported,
        vendor: "https://learn.microsoft.com/lifecycle/products/?terms=sql%20server",
    },
    Rule {
        engine: "kafka",
        product: "apache-kafka",
        must: Must::Newest,
        vendor: "https://kafka.apache.org/downloads",
    },
];

pub fn run(_args: &[String]) -> Result<(), String> {
    let today = today();
    let matrix = parse_matrix(MATRIX)?;
    println!("Support windows on {today}, from endoflife.date\n");

    let mut behind = false;
    for line in &matrix {
        let Some(rule) = RULES.iter().find(|rule| rule.engine == line.engine) else {
            println!(
                "  {:10} not checked: endoflife.date does not track it",
                line.engine
            );
            continue;
        };
        let verdict = fetch(rule.product).and_then(|body| {
            let releases = parse_releases(&body)?;
            Ok(compare(line, rule.must, &releases, &today))
        });
        match verdict {
            Ok(actions) if actions.is_empty() => {
                println!("  {:10} current   {}", line.engine, line.summary());
            }
            Ok(actions) => {
                behind = true;
                println!("  {:10} behind    {}", line.engine, line.summary());
                for action in actions {
                    println!("  {:10}   → {action}", "");
                }
                println!("  {:10}   vendor: {}", "", rule.vendor);
            }
            Err(error) => {
                behind = true;
                println!("  {:10} could not check: {error}", line.engine);
            }
        }
    }

    if behind {
        println!("\nEdit {MATRIX_PATH}, then run `cargo xtask policy-gate`.");
        Err("the tested matrix is not the supported one".into())
    } else {
        Ok(())
    }
}

// ── The matrix ───────────────────────────────────────────────────────────────

#[derive(Debug)]
struct MatrixLine {
    engine: String,
    full: String,
    tested: Vec<String>,
}

impl MatrixLine {
    fn summary(&self) -> String {
        let tested: Vec<_> = self
            .tested
            .iter()
            .map(|tag| release_of(&self.engine, tag))
            .collect();
        format!(
            "tested {} · full suites on {}",
            tested.join(" "),
            release_of(&self.engine, &self.full)
        )
    }
}

fn parse_matrix(text: &str) -> Result<Vec<MatrixLine>, String> {
    text.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(|line| {
            let fields: Vec<_> = line.split_whitespace().collect();
            match fields.as_slice() {
                [engine, full, tested @ ..] if !tested.is_empty() => Ok(MatrixLine {
                    engine: (*engine).into(),
                    full: (*full).into(),
                    tested: tested.iter().map(|tag| (*tag).into()).collect(),
                }),
                _ => Err(format!(
                    "{MATRIX_PATH}: `{line}` needs an engine, a full-suite tag and tested tags"
                )),
            }
        })
        .collect()
}

/// The release line a tag belongs to, as endoflife.date names it.
fn release_of(engine: &str, tag: &str) -> String {
    match engine {
        // `18-alpine` → `18`
        "postgres" => tag.split('-').next().unwrap_or(tag).to_string(),
        // `2025-CU10-ubuntu-24.04` → `2025` (compared with `releaseLabel`)
        "sqlserver" => tag.chars().take(4).collect(),
        // `4.4.0` → `4.4`
        "kafka" => tag.split('.').take(2).collect::<Vec<_>>().join("."),
        // `v26.2.6` → `26.2.6`; MySQL and MariaDB tags are the release line already
        _ => tag.trim_start_matches('v').to_string(),
    }
}

// ── The windows ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
struct Release {
    line: String,
    /// The last day of support, or `None` when the vendor has not set one.
    eol: Option<String>,
    lts: bool,
}

fn fetch(product: &str) -> Result<String, String> {
    let url = format!("https://endoflife.date/api/{product}.json");
    let output = Command::new("curl")
        .args([
            "-fsSL",
            "--max-time",
            "20",
            "-A",
            "rustcdc-support-window",
            &url,
        ])
        .output()
        .map_err(|error| format!("could not run curl: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "{url}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    String::from_utf8(output.stdout).map_err(|_| format!("{url}: not UTF-8"))
}

/// The releases in an endoflife.date answer. An answer that is not a non-empty array of
/// releases is an error, never an empty window.
fn parse_releases(body: &str) -> Result<Vec<Release>, String> {
    let Json::Array(items) = Json::parse(body)? else {
        return Err("the answer is not a list of releases".into());
    };
    let releases = items
        .iter()
        .map(|item| {
            let field = |name: &str| item.get(name);
            // SQL Server's cycles are build numbers (`17.0`); the year leads the release label,
            // which may carry more (`2022 '__CODENAME__'`, `2016 SP3`).
            let line = field("releaseLabel")
                .and_then(Json::as_str)
                .and_then(|label| label.split_whitespace().next())
                .or_else(|| field("cycle").and_then(Json::as_str))
                .ok_or("a release without a cycle")?
                .to_string();
            let eol = match field("eol") {
                Some(Json::Str(date)) => Some(date.clone()),
                Some(Json::Bool(true)) => Some("0000-00-00".into()),
                _ => None,
            };
            let lts = !matches!(
                field("lts"),
                None | Some(Json::Bool(false)) | Some(Json::Null)
            );
            Ok(Release { line, eol, lts })
        })
        .collect::<Result<Vec<_>, &str>>()?;
    if releases.is_empty() {
        return Err("the answer lists no releases".into());
    }
    Ok(releases)
}

/// What to change so `line` holds what `must` asks of the releases supported on `today`.
fn compare(line: &MatrixLine, must: Must, releases: &[Release], today: &str) -> Vec<String> {
    let supported: Vec<&Release> = releases
        .iter()
        .filter(|release| release.eol.as_deref().is_none_or(|eol| eol > today))
        .collect();
    let newest = |releases: &[&Release]| {
        releases
            .iter()
            .map(|release| release.line.clone())
            .max_by(|a, b| version_order(a, b))
    };
    let mut required: Vec<String> = match must {
        Must::EverySupported => supported
            .iter()
            .map(|release| release.line.clone())
            .collect(),
        Must::EveryLts => supported
            .iter()
            .filter(|release| release.lts)
            .map(|release| release.line.clone())
            .collect(),
        Must::Newest => newest(&supported).into_iter().collect(),
    };
    required.sort_by(|a, b| version_order(a, b));
    required.dedup();
    let tested: Vec<String> = line
        .tested
        .iter()
        .map(|tag| release_of(&line.engine, tag))
        .collect();

    let mut actions = Vec::new();
    let mut missing: Vec<_> = required
        .iter()
        .filter(|r| !tested.contains(r))
        .cloned()
        .collect();
    missing.sort_by(|a, b| version_order(a, b));
    if !missing.is_empty() {
        actions.push(format!("add {}", missing.join(", ")));
    }
    let stale: Vec<_> = tested
        .iter()
        .filter(|t| !supported.iter().any(|release| &release.line == *t))
        .cloned()
        .collect();
    if !stale.is_empty() {
        actions.push(format!("drop {} (out of support)", stale.join(", ")));
    }
    let required_refs: Vec<&Release> = supported
        .iter()
        .copied()
        .filter(|release| required.contains(&release.line))
        .collect();
    if let Some(newest) = newest(&required_refs)
        && release_of(&line.engine, &line.full) != newest
    {
        actions.push(format!("run the full suites on {newest}"));
    }
    actions
}

/// `9.7` < `26.10`, `10.11` > `10.6`: numeric by component.
fn version_order(a: &str, b: &str) -> Ordering {
    let parts = |v: &str| -> Vec<u64> { v.split('.').map(|p| p.parse().unwrap_or(0)).collect() };
    parts(a).cmp(&parts(b))
}

/// Today as `YYYY-MM-DD` (UTC), from the system clock.
fn today() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_secs());
    civil_from_days(i64::try_from(secs / 86_400).unwrap_or(0))
}

/// Howard Hinnant's `civil_from_days`: days since 1970-01-01 to a proleptic Gregorian date.
fn civil_from_days(days: i64) -> String {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!("{year:04}-{month:02}-{day:02}")
}

// ── Just enough JSON ─────────────────────────────────────────────────────────

/// The JSON xtask reads, without a dependency: xtask stays std-only (see Cargo.toml).
#[derive(Debug, Clone, PartialEq)]
enum Json {
    Null,
    Bool(bool),
    Num,
    Str(String),
    Array(Vec<Json>),
    Object(Vec<(String, Json)>),
}

impl Json {
    fn parse(text: &str) -> Result<Json, String> {
        let mut parser = Parser {
            bytes: text.as_bytes(),
            at: 0,
        };
        let value = parser.value()?;
        parser.skip_ws();
        if parser.at != parser.bytes.len() {
            return Err("trailing data after the JSON".into());
        }
        Ok(value)
    }

    fn get(&self, key: &str) -> Option<&Json> {
        match self {
            Json::Object(fields) => fields.iter().find(|(k, _)| k == key).map(|(_, v)| v),
            _ => None,
        }
    }

    fn as_str(&self) -> Option<&str> {
        match self {
            Json::Str(s) => Some(s),
            _ => None,
        }
    }
}

struct Parser<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl Parser<'_> {
    fn skip_ws(&mut self) {
        while self.bytes.get(self.at).is_some_and(u8::is_ascii_whitespace) {
            self.at += 1;
        }
    }

    fn expect(&mut self, literal: &str) -> Result<(), String> {
        if self.bytes[self.at..].starts_with(literal.as_bytes()) {
            self.at += literal.len();
            Ok(())
        } else {
            Err(format!(
                "not JSON: expected `{literal}` at byte {}",
                self.at
            ))
        }
    }

    fn value(&mut self) -> Result<Json, String> {
        self.skip_ws();
        match self.bytes.get(self.at) {
            Some(b'{') => self.object(),
            Some(b'[') => self.array(),
            Some(b'"') => self.string().map(Json::Str),
            Some(b't') => self.expect("true").map(|()| Json::Bool(true)),
            Some(b'f') => self.expect("false").map(|()| Json::Bool(false)),
            Some(b'n') => self.expect("null").map(|()| Json::Null),
            Some(b'-' | b'0'..=b'9') => {
                while self
                    .bytes
                    .get(self.at)
                    .is_some_and(|b| b.is_ascii_digit() || b"+-.eE".contains(b))
                {
                    self.at += 1;
                }
                Ok(Json::Num)
            }
            _ => Err(format!("not JSON at byte {}", self.at)),
        }
    }

    fn array(&mut self) -> Result<Json, String> {
        self.at += 1;
        let mut items = Vec::new();
        self.skip_ws();
        if self.bytes.get(self.at) == Some(&b']') {
            self.at += 1;
            return Ok(Json::Array(items));
        }
        loop {
            items.push(self.value()?);
            self.skip_ws();
            match self.bytes.get(self.at) {
                Some(b',') => self.at += 1,
                Some(b']') => {
                    self.at += 1;
                    return Ok(Json::Array(items));
                }
                _ => return Err(format!("not JSON: unterminated array at byte {}", self.at)),
            }
        }
    }

    fn object(&mut self) -> Result<Json, String> {
        self.at += 1;
        let mut fields = Vec::new();
        self.skip_ws();
        if self.bytes.get(self.at) == Some(&b'}') {
            self.at += 1;
            return Ok(Json::Object(fields));
        }
        loop {
            self.skip_ws();
            let key = self.string()?;
            self.skip_ws();
            self.expect(":")?;
            fields.push((key, self.value()?));
            self.skip_ws();
            match self.bytes.get(self.at) {
                Some(b',') => self.at += 1,
                Some(b'}') => {
                    self.at += 1;
                    return Ok(Json::Object(fields));
                }
                _ => return Err(format!("not JSON: unterminated object at byte {}", self.at)),
            }
        }
    }

    fn string(&mut self) -> Result<String, String> {
        self.expect("\"")?;
        let mut out = Vec::new();
        loop {
            match self.bytes.get(self.at) {
                Some(b'"') => {
                    self.at += 1;
                    return String::from_utf8(out).map_err(|_| "not UTF-8".into());
                }
                Some(b'\\') => {
                    let escaped = *self
                        .bytes
                        .get(self.at + 1)
                        .ok_or("not JSON: dangling escape")?;
                    // Release metadata is ASCII; a `\u` escape is kept as `?`, not decoded.
                    out.push(match escaped {
                        b'n' => b'\n',
                        b't' => b'\t',
                        b'u' => b'?',
                        other => other,
                    });
                    self.at += 2;
                }
                Some(&byte) => {
                    out.push(byte);
                    self.at += 1;
                }
                None => return Err("not JSON: unterminated string".into()),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Shaped like endoflife.date's answer on 2026-11-12.
    const POSTGRES: &str = r#"[
      {"cycle":"18","releaseDate":"2025-09-25","eol":"2030-11-14","latest":"18.7","lts":false},
      {"cycle":"17","eol":"2029-11-08","lts":false},
      {"cycle":"16","eol":"2028-11-09","lts":false},
      {"cycle":"15","eol":"2027-11-11","lts":false},
      {"cycle":"14","eol":"2026-11-12","lts":false},
      {"cycle":"13","eol":"2025-11-13","lts":false}
    ]"#;
    const MYSQL: &str = r#"[
      {"cycle":"26.10","eol":false,"lts":false},
      {"cycle":"9.7","eol":"2034-04-30","lts":true},
      {"cycle":"8.4","eol":"2032-04-30","lts":true},
      {"cycle":"8.0","eol":"2026-04-30","lts":false}
    ]"#;
    const SQLSERVER: &str = r#"[
      {"cycle":"17.0","releaseLabel":"2025","eol":"2036-01-06","lts":false},
      {"cycle":"16.0","releaseLabel":"2022 '__CODENAME__'","eol":"2033-01-11","lts":false},
      {"cycle":"13.0-sp3-acp","releaseLabel":"2016 SP3 Azure Connect Pack","eol":"2026-07-14"},
      {"cycle":"13.0-sp3","releaseLabel":"2016 SP3","eol":"2026-07-14"},
      {"cycle":"15.0","releaseLabel":"2019 '__CODENAME__'","eol":"2030-01-08","lts":false},
      {"cycle":"14.0","releaseLabel":"2017 '__CODENAME__'","eol":"2027-10-12","lts":false}
    ]"#;
    const TODAY: &str = "2026-11-12";

    fn line(engine: &str, full: &str, tested: &[&str]) -> MatrixLine {
        MatrixLine {
            engine: engine.into(),
            full: full.into(),
            tested: tested.iter().map(|t| (*t).into()).collect(),
        }
    }

    fn actions(line: &MatrixLine, must: Must, body: &str) -> Vec<String> {
        compare(line, must, &parse_releases(body).unwrap(), TODAY)
    }

    #[test]
    fn the_current_matrix_needs_nothing() {
        let pg = line(
            "postgres",
            "18-alpine",
            &["15-alpine", "16-alpine", "17-alpine", "18-alpine"],
        );
        assert!(actions(&pg, Must::EverySupported, POSTGRES).is_empty());
        let mysql = line("mysql", "9.7", &["8.4", "9.7"]);
        assert!(
            actions(&mysql, Must::EveryLts, MYSQL).is_empty(),
            "26.10 is Innovation, not owed"
        );
        let mssql = line(
            "sqlserver",
            "2025-CU10-ubuntu-24.04",
            &[
                "2017-CU31-ubuntu-18.04",
                "2019-CU32-ubuntu-20.04",
                "2022-CU27-ubuntu-22.04",
                "2025-CU10-ubuntu-24.04",
            ],
        );
        assert!(actions(&mssql, Must::EverySupported, SQLSERVER).is_empty());
    }

    #[test]
    fn a_supported_release_missing_from_the_matrix_is_named() {
        let pg = line(
            "postgres",
            "18-alpine",
            &["15-alpine", "16-alpine", "18-alpine"],
        );
        assert_eq!(actions(&pg, Must::EverySupported, POSTGRES), ["add 17"]);
    }

    /// A stale matrix: two out-of-support releases, two missing, the full suites on an old
    /// one. Its final release is "today", so 14 counts as out.
    #[test]
    fn the_matrix_this_check_was_written_for_fails_with_every_action() {
        let pg = line(
            "postgres",
            "16-alpine",
            &["12-alpine", "14-alpine", "15-alpine", "16-alpine"],
        );
        assert_eq!(
            actions(&pg, Must::EverySupported, POSTGRES),
            [
                "add 17, 18",
                "drop 12, 14 (out of support)",
                "run the full suites on 18"
            ]
        );
        let mysql = line("mysql", "8.0", &["8.0", "8.4"]);
        assert_eq!(
            actions(&mysql, Must::EveryLts, MYSQL),
            [
                "add 9.7",
                "drop 8.0 (out of support)",
                "run the full suites on 9.7"
            ]
        );
    }

    #[test]
    fn the_newest_rule_owes_only_the_newest() {
        let kafka = r#"[{"cycle":"4.4","eol":false},{"cycle":"4.3","eol":false}]"#;
        assert!(actions(&line("kafka", "4.4.0", &["4.4.0"]), Must::Newest, kafka).is_empty());
        assert_eq!(
            actions(&line("kafka", "4.3.2", &["4.3.2"]), Must::Newest, kafka),
            ["add 4.4", "run the full suites on 4.4"]
        );
    }

    #[test]
    fn an_answer_that_is_not_a_list_of_releases_fails_closed() {
        assert!(
            parse_releases("[]").is_err(),
            "empty is not \"nothing supported\""
        );
        assert!(parse_releases("<!DOCTYPE html><html>").is_err());
        assert!(parse_releases(r#"{"message":"not found"}"#).is_err());
        assert!(
            parse_releases(r#"[{"eol":false}]"#).is_err(),
            "a release without a cycle"
        );
    }

    #[test]
    fn versions_order_by_component() {
        assert_eq!(version_order("9.7", "26.10"), Ordering::Less);
        assert_eq!(version_order("10.11", "10.6"), Ordering::Greater);
    }

    #[test]
    fn days_since_the_epoch_become_a_calendar_date() {
        assert_eq!(civil_from_days(0), "1970-01-01");
        assert_eq!(civil_from_days(20_404), "2025-11-12");
        assert_eq!(civil_from_days(20_769), "2026-11-12");
        assert_eq!(civil_from_days(11_016), "2000-02-29");
    }

    #[test]
    fn the_committed_matrix_parses() {
        let matrix = parse_matrix(MATRIX).unwrap();
        assert!(matrix.iter().any(|line| line.engine == "postgres"));
        assert!(
            parse_matrix("postgres 18-alpine").is_err(),
            "a line without tested tags"
        );
    }
}
