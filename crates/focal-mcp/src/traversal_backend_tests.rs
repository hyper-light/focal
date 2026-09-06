use super::*;

#[test]
fn traversal_tool_preserves_empty_page_progress_and_exact_cursor_without_mutation() {
    let root = tempfile::tempdir().unwrap();
    let mut running = Running::start(&root.path().join("operations"), false);
    let input = json!({"roots":["claim:00000000000000000000000000000001"],"edges":["requirement"],"depth":1,"limit":1,"max_visits":1});
    running.tool(1, "ledger.traverse", input.clone());
    let observed = running.observed();
    let Operation::Traverse(query) = &observed.request.operation else {
        panic!("must be one read, never an epoch/mutation")
    };
    assert_eq!(query.max_items, 1);
    assert_eq!(query.max_visits, 1);
    assert_eq!(query.edges, vec![TraversalEdge::Requirement]);
    let token = ReadToken {
        ledger: context().ledger,
        sequence: SessionSeq(9),
        route_epoch: RouteEpoch(1),
    };
    let page = TraversalPage {
        token,
        objects: vec![],
        next: Some(TraversalCursor { bytes: vec![1, 2] }),
        stop: TraversalStop::PageLimit,
        visited: 1,
        total_visits: 1,
    };
    observed
        .response
        .send(Ok(observed
            .request
            .reply(Response::Traversed(page.clone()))))
        .unwrap();
    let result = application(&running.response(1));
    assert_eq!(result.operation_id, None);
    assert_eq!(result.result, OperationOutput::Traversal { page });
    let mut next = input;
    next["cursor"] = json!("0102");
    running.tool(2, "ledger.traverse", next);
    let observed = running.observed();
    let Operation::Traverse(query) = &observed.request.operation else {
        panic!("read")
    };
    assert_eq!(query.cursor.as_ref().unwrap().bytes, vec![1, 2]);
    let page = TraversalPage {
        token,
        objects: vec![],
        next: None,
        stop: TraversalStop::Complete,
        visited: 1,
        total_visits: 2,
    };
    observed
        .response
        .send(Ok(observed
            .request
            .reply(Response::Traversed(page.clone()))))
        .unwrap();
    assert_eq!(
        application(&running.response(2)).result,
        OperationOutput::Traversal { page }
    );
    running.stop();
}
