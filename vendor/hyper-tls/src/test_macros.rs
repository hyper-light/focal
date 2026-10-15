//! Macros used for unit testing.

/// Instantiate the given test functions for the built-in provider, aws-lc-rs.
///
/// The provider module is bound as `provider`.
#[cfg(test)]
macro_rules! test_for_each_provider {
    ($($tt:tt)+) => {

        mod test_with_aws_lc_rs {
            use crate::crypto::aws_lc_rs as provider;
            #[allow(unused_imports)]
            use super::*;
            $($tt)+
        }
    };
}

#[cfg(test)]
#[macro_rules_attribute::apply(test_for_each_provider)]
mod tests {
    #[test]
    fn test_each_provider() {
        std::println!("provider is {:?}", super::provider::default_provider());
    }
}
