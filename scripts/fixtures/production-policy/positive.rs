// Production code that handles its errors, with allowances scoped to test
// builds only (CLAUDE.md §1): `scripts/check_production_policy.py` passes it.
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::indexing_slicing))]
fn first(arguments: &[String]) -> Option<&String> {
    arguments.first()
}
fn main() -> std::process::ExitCode {
    let arguments: Vec<String> = std::env::args().collect();
    match first(&arguments) {
        Some(_) => std::process::ExitCode::SUCCESS,
        None => std::process::ExitCode::FAILURE,
    }
}
#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    #[test]
    fn the_first_argument() {
        let arguments = vec!["focal".to_owned()];
        assert_eq!(super::first(&arguments).unwrap(), "focal");
    }
    #[allow(clippy::panic)]
    fn helper() {
        panic!("test-only");
    }
}
#[cfg(test)]
#[path = "positive_tests.rs"]
mod more_tests;
