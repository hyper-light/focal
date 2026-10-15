//! How focal's own pages look: help for every command, an action and a command, and
//! errors in a panel, in hyperlight's design on a colour terminal (focal-tui), and the
//! same words as plain text everywhere else.

use super::grammar::{self, Use};
use clap::{Arg, ArgAction, Command};
use focal_tui::page::{self, Entry, Opt, Page};
use focal_tui::terminal::{self, Stream};
use focal_tui::tokens::{self, Paint};
use std::io::Write;

/// The paint a page on `stream` is drawn with, and whether it is a colour terminal; off
/// one, the page is drawn with the site's paint and written without its colours.
fn paint_for(stream: Stream) -> (Paint, bool) {
    match terminal::styled(stream, true) {
        Some(paint) => (paint, true),
        None => (Paint::new(true), false),
    }
}

fn emit(page: Page, paint: &Paint, styled: bool, out: &mut impl Write) -> std::io::Result<()> {
    let text = page.text(paint);
    let text = if styled { text } else { focal_tui::seen(&text) };
    out.write_all(text.as_bytes())?;
    out.flush()
}

/// The head of a page: the mark beside the brand where the terminal shows colour.
fn head(page: &mut Page, paint: &Paint, styled: bool, cols: usize, name: &str, about: &str) {
    let lines = [(tokens::BRIGHT, true, about)];
    // Plain text has no mark: Braille would be noise in a pipe or a log.
    let cols_for_head = if styled { cols } else { cols.min(55) };
    page::head(page, paint, cols_for_head, name, &lines);
}

/// `focal`, `focal help`: what focal is, and every command by section, each action
/// once with the things it takes (`get claim | testament`), alphabetical.
pub(crate) fn top(out: &mut impl Write) -> std::io::Result<()> {
    let (paint, styled) = paint_for(Stream::Stdout);
    let cols = terminal::width(Stream::Stdout);
    let tree = super::command_tree::command();
    let mut page = Page::new();
    head(&mut page, &paint, styled, cols, "", grammar::ABOUT);
    page::heading(&mut page, &paint, "usage");
    page::usage(&mut page, &paint, cols, "focal ACTION THING [ARG...]");
    let sections: Vec<(&str, Vec<grammar::Group>)> = grammar::SECTIONS
        .iter()
        .map(|s| (*s, grammar::groups(s, &tree)))
        .collect();
    // Each action is followed by its things, as shards says them (`create vm |
    // volume`); a long list wraps under its first thing, so that every description
    // starts in one column, past the widest of them.
    let things_w = (cols / 4).clamp(20, 36);
    let wrapped: Vec<Vec<(usize, ThingLines<'_>)>> = sections
        .iter()
        .map(|(_, rows)| {
            rows.iter()
                .map(|g| {
                    (
                        g.action.len().saturating_add(1),
                        thing_lines(&g.things, things_w),
                    )
                })
                .collect()
        })
        .collect();
    let widest = wrapped
        .iter()
        .flatten()
        .flat_map(|(indent, lines)| {
            lines.iter().map(move |parts| {
                let shown: usize = parts
                    .iter()
                    .map(|p| p.len())
                    .sum::<usize>()
                    .saturating_add(parts.len().saturating_sub(1).saturating_mul(3));
                indent.saturating_add(shown)
            })
        })
        .max()
        .unwrap_or(0);
    let about_at = 4usize.saturating_add(widest).saturating_add(3);
    let about_w = cols.saturating_sub(about_at);
    for ((section, rows), wrapped) in sections.iter().zip(&wrapped) {
        page::heading(&mut page, &paint, section);
        for (group, (indent, things)) in rows.iter().zip(wrapped) {
            let abouts = if about_w >= 20 {
                focal_tui::layout::wrap(&group.about, about_w)
            } else {
                Vec::new()
            };
            let height = things.len().max(abouts.len()).max(1);
            for i in 0..height {
                let l = page.line();
                l.pad(4);
                if i == 0 {
                    l.put(&paint, tokens::FOREGROUND, group.action).pad(1);
                } else {
                    l.pad(*indent);
                }
                if let Some(parts) = things.get(i) {
                    let mut first = true;
                    for part in parts {
                        if !first {
                            l.put(&paint, tokens::FAINT, " | ");
                        }
                        first = false;
                        l.put(&paint, tokens::TEAL, part);
                    }
                }
                if let Some(about) = abouts.get(i) {
                    l.to(about_at).put(&paint, tokens::BODY, about);
                }
            }
        }
    }
    let globals: Vec<&Arg> = tree
        .get_arguments()
        .filter(|a| a.is_global_set() && !a.is_hide_set())
        .collect();
    if !globals.is_empty() {
        page::heading(&mut page, &paint, "global options");
        options(&mut page, &paint, cols, &globals);
    }
    page::heading(&mut page, &paint, "more");
    page.line()
        .pad(4)
        .put(&paint, tokens::SUBTLE, "focal ACTION")
        .put(&paint, tokens::BODY, " lists what an action takes; ")
        .put(&paint, tokens::SUBTLE, "focal ACTION THING --help")
        .put(&paint, tokens::BODY, " shows a command.");
    emit(page, &paint, styled, out)
}

/// The things of one help row, a line of them at a time.
type ThingLines<'a> = Vec<Vec<&'a str>>;

/// `things` as lines of at most `width` columns, ` | ` between them.
fn thing_lines<'a>(things: &[&'a str], width: usize) -> ThingLines<'a> {
    let mut lines: Vec<Vec<&'a str>> = Vec::new();
    let mut used = 0usize;
    for thing in things {
        let need = thing.len().saturating_add(if used == 0 { 0 } else { 3 });
        match lines.last_mut() {
            Some(line) if used.saturating_add(need) <= width => {
                line.push(thing);
                used = used.saturating_add(need);
            }
            _ => {
                lines.push(vec![thing]);
                used = thing.len();
            }
        }
    }
    lines
}

/// `focal ACTION`: what it acts on.
pub(crate) fn action(action: &str, out: &mut impl Write) -> std::io::Result<()> {
    let (paint, styled) = paint_for(Stream::Stdout);
    let cols = terminal::width(Stream::Stdout);
    let tree = super::command_tree::command();
    let mut page = Page::new();
    let uses: Vec<&Use> = grammar::USES
        .iter()
        .filter(|u| u.action == action)
        .collect();
    let about = match uses.as_slice() {
        [one] => grammar::about(one, &tree),
        _ => format!("What `{action}` acts on"),
    };
    head(&mut page, &paint, styled, cols, action, &about);
    page::heading(&mut page, &paint, "usage");
    page::usage(
        &mut page,
        &paint,
        cols,
        &format!("focal {action} THING [ARG...]"),
    );
    page::heading(&mut page, &paint, "things");
    let abouts: Vec<String> = uses.iter().map(|u| grammar::about(u, &tree)).collect();
    let rows: Vec<Entry<'_>> = uses
        .iter()
        .zip(&abouts)
        .map(|(u, about)| Entry {
            name: u.thing,
            takes: "",
            about,
        })
        .collect();
    page::entries(&mut page, &paint, cols, &rows);
    emit(page, &paint, styled, out)
}

/// `focal ACTION THING --help`: what the command does, how it is said, what it takes
/// and its options, the global ones apart.
pub(crate) fn command(u: &Use, out: &mut impl Write) -> std::io::Result<()> {
    let (paint, styled) = paint_for(Stream::Stdout);
    let cols = terminal::width(Stream::Stdout);
    let tree = super::command_tree::command();
    let about = grammar::about(u, &tree);
    let name = format!("{} {}", u.action, u.thing);
    let mut page = Page::new();
    head(&mut page, &paint, styled, cols, &name, &about);
    let Some(leaf) = grammar::find(&tree, u.path) else {
        return emit(page, &paint, styled, out);
    };
    // A long description, where the command has more to say than its line.
    if let Some(long) = leaf.get_long_about() {
        let long = long.to_string();
        if long.trim_end_matches('.') != about.trim_end_matches('.') {
            page.blank();
            for part in focal_tui::layout::wrap(&long, cols.saturating_sub(6).max(20)) {
                page.line().pad(4).put(&paint, tokens::BODY, &part);
            }
        }
    }
    page::heading(&mut page, &paint, "usage");
    page::usage(&mut page, &paint, cols, &usage(&name, leaf));
    let positionals: Vec<&Arg> = leaf
        .get_arguments()
        .filter(|a| a.is_positional() && !a.is_hide_set())
        .collect();
    if !positionals.is_empty() {
        page::heading(&mut page, &paint, "arguments");
        let names: Vec<String> = positionals.iter().map(|a| value_name(a)).collect();
        let abouts: Vec<String> = positionals.iter().map(|a| help_of(a)).collect();
        let rows: Vec<Entry<'_>> = names
            .iter()
            .zip(&abouts)
            .map(|(n, a)| Entry {
                name: n,
                takes: "",
                about: a,
            })
            .collect();
        page::entries(&mut page, &paint, cols, &rows);
    }
    let own: Vec<&Arg> = leaf
        .get_arguments()
        .filter(|a| !a.is_positional() && !a.is_hide_set() && !a.is_global_set())
        .collect();
    if !own.is_empty() {
        page::heading(&mut page, &paint, "options");
        options(&mut page, &paint, cols, &own);
    }
    let globals: Vec<&Arg> = tree
        .get_arguments()
        .filter(|a| a.is_global_set() && !a.is_hide_set())
        .collect();
    if !globals.is_empty() {
        page::heading(&mut page, &paint, "global options");
        options(&mut page, &paint, cols, &globals);
    }
    emit(page, &paint, styled, out)
}

fn options(page: &mut Page, paint: &Paint, cols: usize, args: &[&Arg]) {
    let texts: Vec<(String, String, String, Option<String>)> = args
        .iter()
        .map(|a| {
            let long = a
                .get_long()
                .map(str::to_string)
                .unwrap_or_else(|| a.get_id().to_string());
            let value = if takes_value(a) {
                value_name(a)
            } else {
                String::new()
            };
            let defaults: Vec<String> = a
                .get_default_values()
                .iter()
                .map(|v| v.to_string_lossy().into_owned())
                .collect();
            let default = (!defaults.is_empty()).then(|| defaults.join(", "));
            (long, value, help_of(a), default)
        })
        .collect();
    let shown: Vec<Opt<'_>> = args
        .iter()
        .zip(&texts)
        .map(|(a, (long, value, about, default))| Opt {
            short: a.get_short(),
            long,
            value,
            about,
            default: default.as_deref(),
        })
        .collect();
    page::options(page, paint, cols, &shown);
}

fn takes_value(a: &Arg) -> bool {
    matches!(a.get_action(), ArgAction::Set | ArgAction::Append)
}

fn value_name(a: &Arg) -> String {
    match a.get_value_names() {
        Some(names) if !names.is_empty() => names
            .iter()
            .map(|n| format!("<{n}>"))
            .collect::<Vec<_>>()
            .join(" "),
        _ => format!("<{}>", a.get_id().as_str().to_uppercase()),
    }
}

fn help_of(a: &Arg) -> String {
    let text = a.get_help().map(|h| h.to_string()).unwrap_or_default();
    let text = text.trim_end_matches('.').to_string();
    let values: Vec<String> = a
        .get_possible_values()
        .iter()
        .filter(|v| !v.is_hide_set())
        .map(|v| v.get_name().to_string())
        .collect();
    if values.is_empty() || !takes_value(a) {
        text
    } else if text.is_empty() {
        format!("One of {}", values.join(", "))
    } else {
        format!("{text} (one of {})", values.join(", "))
    }
}

/// `focal ACTION THING [OPTIONS] <ARG>...`: the command's words, then what it takes.
fn usage(name: &str, leaf: &Command) -> String {
    let mut line = format!("focal {name}");
    if leaf
        .get_arguments()
        .any(|a| !a.is_positional() && !a.is_hide_set())
    {
        line.push_str(" [OPTIONS]");
    }
    for a in leaf
        .get_arguments()
        .filter(|a| a.is_positional() && !a.is_hide_set())
    {
        let name = value_name(a);
        let many = matches!(a.get_action(), ArgAction::Append)
            || a.get_num_args().is_some_and(|n| n.max_values() > 1);
        let shown = if many { format!("{name}...") } else { name };
        if a.is_required_set() {
            line.push(' ');
            line.push_str(&shown);
        } else {
            line.push_str(&format!(" [{shown}]"));
        }
    }
    line
}

/// An error on stderr: the head naming the command it came from, then a panel with what
/// happened and what to do; plain lines where stderr is no colour terminal.
pub(crate) fn error(title: &str, message: &str, hints: &[&str]) -> std::io::Result<()> {
    let mut err = std::io::stderr().lock();
    match terminal::styled(Stream::Stderr, false) {
        Some(paint) => {
            let cols = terminal::width(Stream::Stderr);
            let mut page = Page::new();
            page::head(&mut page, &paint, cols, title, &[]);
            let mut text = page.text(&paint);
            for (line, _) in focal_tui::panel::error(&paint, cols, "", message, hints) {
                text.push_str(&line);
                text.push_str("\x1b[0m\n");
            }
            text.push('\n');
            err.write_all(text.as_bytes())?;
        }
        None => {
            writeln!(err, "focal: {message}")?;
            for hint in hints {
                writeln!(err, "  {hint}")?;
            }
        }
    }
    err.flush()
}

/// Whether errors on stderr are drawn in a panel.
pub(crate) fn styled_errors() -> bool {
    terminal::styled(Stream::Stderr, false).is_some()
}

/// A parse error of the parser people use, in focal's panel: its message, and its tip
/// and usage as hints. Help and the version are not errors: they print as they are.
pub(crate) fn parse_error(failure: &clap::Error) -> std::io::Result<()> {
    let rendered = failure.render().to_string();
    let mut message = String::new();
    let mut hints: Vec<String> = Vec::new();
    for line in rendered.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with("For more information") {
            continue;
        }
        if let Some(rest) = line.strip_prefix("error: ") {
            message = rest.to_string();
        } else if let Some(rest) = line.strip_prefix("tip: ") {
            hints.push(rest.to_string());
        } else if let Some(rest) = line.strip_prefix("Usage: ") {
            hints.push(format!("usage: {rest}"));
        } else if message.is_empty() {
            message = line.to_string();
        } else {
            hints.push(line.to_string());
        }
    }
    hints.push("focal lists every command; focal ACTION lists what it takes".into());
    let refs: Vec<&str> = hints.iter().map(String::as_str).collect();
    error("", &message, &refs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_command_reads_on_the_top_page() {
        let mut out = Vec::new();
        top(&mut out).unwrap();
        let text = focal_tui::seen(&String::from_utf8(out).unwrap());
        assert!(text.contains("F O C A L"), "{text}");
        assert!(text.contains("focal ACTION THING"), "{text}");
        // One row an action in each section, its first thing beside it; every thing
        // somewhere in its rows.
        let tree = super::super::command_tree::command();
        for section in grammar::SECTIONS {
            for group in grammar::groups(section, &tree) {
                let first = group.things.first().copied().unwrap_or_default();
                assert!(
                    text.lines().any(|l| l
                        .trim_start()
                        .starts_with(&format!("{} {first}", group.action))),
                    "{section}: {} {first}",
                    group.action
                );
            }
        }
        for u in grammar::USES {
            assert!(text.contains(u.thing), "{} {}", u.action, u.thing);
        }
        assert!(text.contains("code | demo"), "{text}");
        for section in grammar::SECTIONS {
            assert!(text.contains(&section.to_uppercase()), "{section}");
        }
    }

    #[test]
    fn a_command_page_shows_its_usage_and_options() {
        let post = grammar::lookup("post", "claim").unwrap();
        let mut out = Vec::new();
        command(post, &mut out).unwrap();
        let text = focal_tui::seen(&String::from_utf8(out).unwrap());
        assert!(text.contains("focal post claim"), "{text}");
        assert!(text.contains("GLOBAL OPTIONS"), "{text}");
        assert!(text.contains("--data-dir"), "{text}");
        let mut out = Vec::new();
        action("invite", &mut out).unwrap();
        let text = focal_tui::seen(&String::from_utf8(out).unwrap());
        assert!(text.contains("node") && text.contains("client"), "{text}");
    }
}
