// The recursion checker's source fixture (scripts/check_recursion.py --self-test).
use std::future::Future;
use std::pin::Pin;

struct Walker {
    inner: Inner,
}

impl Walker {
    // A function returning a boxed future of itself: refused.
    fn boxed_again(&self, depth: u64) -> Pin<Box<dyn Future<Output = u64> + '_>> {
        Box::pin(async move {
            if depth == 0 {
                return 0;
            }
            // "boxed_again(" in a string or a comment is not a call
            self.boxed_again(depth - 1).await + 1
        })
    }

    // An async function that calls its field's method of the same name: not a self-call.
    async fn delegates(&self) -> u64 {
        self.inner.delegates().await
    }
}

struct Inner;

impl Inner {
    async fn delegates(&self) -> u64 {
        1
    }
}
