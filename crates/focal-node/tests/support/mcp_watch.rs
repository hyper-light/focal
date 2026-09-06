use super::*;
use focal_client::watch::{WatchDelivery, WatchPage};
use focal_model::{ClaimId, ClaimStatus, DeltaFact};
use focal_stream::StreamEvent;

fn retained(result: &Value) -> WatchDelivery {
    assert_eq!(result["result"]["kind"], "watch");
    serde_json::from_value(result["result"]["delivery"].clone()).unwrap()
}
fn acknowledge(mcp: &mut Mcp, name: &str, item: &WatchDelivery) {
    let result = mcp.success(
        "watch.acknowledge",
        json!({"name":name,"delivery_id":item.id.to_string()}),
    );
    assert_eq!(result["condition"], "Consumed");
    assert_eq!(result["result"]["status"]["acknowledged"], item.number);
    assert!(result["result"]["delivery"].is_null());
}

#[test]
fn watch_stdio_retains_seed_and_events_until_explicit_consumption_across_restart() {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir_in("/tmp").unwrap();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let server = start(root.path());
    let mut mcp = Mcp::start(root.path(), true);
    mcp.consumed_mutation("claim.submit", claim());
    let options = json!({"name":"claims","family":"claim","seed":true,"max_items":1});
    let first = retained(&mcp.success("watch.open", options.clone()));
    assert!(matches!(&first.page, WatchPage::Seed { page } if page.objects.len()==1));
    assert_eq!(
        retained(&mcp.success("watch.next", json!({"name":"claims"}))),
        first
    );
    assert_eq!(retained(&mcp.success("watch.open", options.clone())), first);
    assert_eq!(
        mcp.call("watch.open", json!({"name":"claims","seed":false}))["result"]["isError"],
        true
    );
    let wrong = mcp.call(
        "watch.acknowledge",
        json!({"name":"claims","delivery_id":"01".repeat(32)}),
    );
    assert_eq!(wrong["result"]["isError"], true);
    assert_eq!(
        retained(&mcp.success("watch.inspect", json!({"name":"claims"}))),
        first
    );
    drop(mcp);
    let mut mcp = Mcp::start(root.path(), false);
    assert_eq!(
        retained(&mcp.success("watch.next", json!({"name":"claims"}))),
        first
    );
    acknowledge(&mut mcp, "claims", &first);
    acknowledge(&mut mcp, "claims", &first);
    // The requirement is a separate seed row. Family filtering may deliver an
    // empty page; consuming it is still necessary before completing the seed.
    let mut seeded = false;
    for _ in 0..8 {
        let item = retained(&mcp.success("watch.next", json!({"name":"claims"})));
        let tail = matches!(item.page, WatchPage::Events { .. });
        acknowledge(&mut mcp, "claims", &item);
        if tail {
            seeded = true;
            break;
        }
    }
    assert!(seeded, "seed never completed");
    mcp.consumed_mutation("claim.post", json!({"claim":id(100)}));
    let changed = retained(&mcp.success("watch.next", json!({"name":"claims"})));
    let WatchPage::Events { page } = &changed.page else {
        panic!("tail events")
    };
    assert!(page.events.iter().any(|event|matches!(event,StreamEvent::Delta{delta,..} if delta.claim==Some(ClaimId::from_u128(100)) && matches!(delta.fact,DeltaFact::Status{current:ClaimStatus::Posted,..}))));
    // Losing both response transport and service must not consume that change.
    drop(mcp);
    drop(server);
    let _server = start(root.path());
    let mut mcp = Mcp::start(root.path(), true);
    assert_eq!(
        retained(&mcp.success("watch.next", json!({"name":"claims"}))),
        changed
    );
    acknowledge(&mut mcp, "claims", &changed);
    for _ in 0..8 {
        let item = retained(&mcp.success("watch.next", json!({"name":"claims"})));
        acknowledge(&mut mcp, "claims", &item);
    }
    let names = mcp.success("watch.inspect", json!({}));
    assert_eq!(names["result"]["names"], json!(["claims"]));
    mcp.finish();
}

#[test]
fn validation_watch_preserves_the_claim_creation_fact_that_admits_requirements() {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir_in("/tmp").unwrap();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let _server = start(root.path());
    let mut mcp = Mcp::start(root.path(), true);
    let initial = retained(&mcp.success(
        "watch.open",
        json!({"name":"requirements","family":"validation","seed":false}),
    ));
    acknowledge(&mut mcp, "requirements", &initial);
    mcp.consumed_mutation("claim.submit", claim());
    let changed = retained(&mcp.success("watch.next", json!({"name":"requirements"})));
    let WatchPage::Events { page } = &changed.page else {
        panic!("tail events")
    };
    assert!(page.events.iter().any(|event|matches!(event,StreamEvent::Delta{delta,..} if delta.claim==Some(ClaimId::from_u128(100)) && matches!(delta.fact,DeltaFact::Status{previous:None,current:ClaimStatus::Generated,..}))));
    let definitions = mcp.success("validation.list", json!({"claim":id(100)}));
    assert_eq!(
        definitions["result"]["page"]["objects"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    let context = mcp.success("validation.context", json!({"id":id(102)}));
    assert!(
        context["result"]["context"]["records"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    acknowledge(&mut mcp, "requirements", &changed);
    mcp.finish();
}
