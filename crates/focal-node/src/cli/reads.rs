use super::{CliError, Context, Result, args::*, authored, output};
use focal_client::{
    input::*,
    operations::{GetDocument, decode_list_cursor},
};
use focal_model::*;
use focal_wire::*;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct ValidationCursor {
    token: ReadToken,
    validation: ValidationId,
    after: ValidationResultPosition,
}
pub(super) fn validation_cursor(
    token: ReadToken,
    validation: ValidationId,
    after: ValidationResultPosition,
) -> Result<String> {
    let bytes = postcard::to_stdvec(&ValidationCursor {
        token,
        validation,
        after,
    })
    .map_err(|_| CliError::InvalidResponse)?;
    Ok(output::hex(&bytes))
}

fn filter(args: Filters, kind: ObjectKind, context: &BuildContext) -> Result<ListFilter> {
    Ok(authored::filters(args).build(kind, context)?.filter)
}
fn fetch(
    runtime: &tokio::runtime::Runtime,
    context: &Context,
    request: ListRequest,
) -> Result<ListPage> {
    let envelope = context.envelope(Operation::List(request))?;
    match runtime.block_on(context.client.request(envelope))?.result {
        Response::Listed(page) => Ok(page),
        _ => Err(CliError::InvalidResponse),
    }
}
pub(super) fn list(
    runtime: &tokio::runtime::Runtime,
    context: &Context,
    command: ListCommand,
) -> Result<()> {
    let (kind, args) = match command {
        ListCommand::Claims(args) => (ObjectKind::Claim, args),
        ListCommand::Testaments(args) => (ObjectKind::Testament, args),
        ListCommand::Artifacts(args) => (ObjectKind::Artifact, args),
        ListCommand::Validations(args) => (ObjectKind::Validation, args),
    };
    let mut document = authored::filters(args.filters);
    document.cursor = args.cursor;
    document.limit = args.limit;
    document.max_visits = WireLimits::default().max_items;
    let request = document.build(kind, &context.build)?;
    output::page(fetch(runtime, context, request)?, args.output.format)
}
fn exact(
    runtime: &tokio::runtime::Runtime,
    context: &Context,
    kind: ObjectKind,
    id: &str,
) -> Result<(ReadToken, ReadObject)> {
    let read = GetDocument {
        id: id.to_owned(),
        prefix: None,
        after: None,
        limit: 1,
    }
    .build(kind, &context.build)?;
    let ReadQuery::Objects(references) = &read.query else {
        return Err(CliError::InvalidResponse);
    };
    let id = references.first().ok_or(CliError::InvalidResponse)?.id;
    let request = context.envelope(Operation::Read(read))?;
    let page = runtime.block_on(context.client.read(request))?;
    let mut objects = page.objects.into_iter();
    let object = objects.next().ok_or(CliError::NotFound)?;
    if objects.next().is_some() || output::key(&object) != (kind, id) || page.next.is_some() {
        return Err(CliError::InvalidResponse);
    }
    Ok((page.token, object))
}
fn validation(
    runtime: &tokio::runtime::Runtime,
    context: &Context,
    args: GetValidationArgs,
) -> Result<()> {
    let id = ValidationId(parse_id(&args.id)?);
    let (consistency, after) = match args.cursor {
        None => (ReadConsistency::Linearizable, None),
        Some(text) => {
            let bytes = decode_list_cursor(&text)?.bytes;
            let (saved, remaining): (ValidationCursor, _) = postcard::take_from_bytes(&bytes)
                .map_err(|_| CliError::Input("invalid validation cursor".into()))?;
            if !remaining.is_empty()
                || saved.validation != id
                || saved.after.run.validation != id
                || saved.token.ledger != context.build.ledger
            {
                return Err(CliError::Input(
                    "validation cursor belongs to another query".into(),
                ));
            }
            (ReadConsistency::Exact(saved.token), Some(saved.after))
        }
    };
    let mut read = GetDocument {
        id: args.id,
        prefix: None,
        after: None,
        limit: args.limit,
    }
    .build(ObjectKind::Validation, &context.build)?;
    // The existing CLI cursor's binary format is preserved. Its ledger and
    // requirement were checked above; pass the exact saved prefix unchanged.
    read.consistency = consistency;
    if let ReadQuery::ValidationResults { after: cursor, .. } = &mut read.query {
        *cursor = after;
    } else {
        return Err(CliError::InvalidResponse);
    }
    let request = context.envelope(Operation::Read(read))?;
    let page = runtime.block_on(context.client.read(request))?;
    let mut objects = page.objects.into_iter();
    let object = objects.next().ok_or(CliError::NotFound)?;
    if objects.next().is_some()
        || !matches!(&object,ReadObject::ValidationResults {id:found,..} if *found==id)
        || page.next.is_some()
    {
        return Err(CliError::InvalidResponse);
    }
    output::object(page.token, &object, args.output.format)
}
fn unique(
    runtime: &tokio::runtime::Runtime,
    context: &Context,
    filter: ListFilter,
) -> Result<(ReadToken, ReadObject)> {
    let mut cursor = None;
    let mut found = None;
    let mut token = None;
    let start = std::time::Instant::now();
    // A singular selection proves uniqueness over a fixed prefix. Exhausting
    // its bounded work budget is an error, never an arbitrary first match.
    for _ in 0..64 {
        if start.elapsed() > std::time::Duration::from_secs(30) {
            break;
        }
        let page = fetch(
            runtime,
            context,
            ListRequest {
                filter: filter.clone(),
                cursor: cursor.clone(),
                max_items: 2,
                max_visits: WireLimits::default().max_items,
            },
        )?;
        if token.is_some_and(|token| token != page.token) {
            return Err(CliError::InvalidResponse);
        }
        token = Some(page.token);
        for object in page.objects {
            if found.replace(object).is_some() {
                return Err(CliError::Ambiguous);
            }
        }
        if page.next.is_none() {
            return Ok((page.token, found.ok_or(CliError::NotFound)?));
        }
        if page.next == cursor {
            return Err(CliError::InvalidResponse);
        }
        cursor = page.next;
    }
    Err(CliError::Input(
        "singular selection exceeded its query budget; narrow the filters or use list claims"
            .into(),
    ))
}
pub(super) fn get(
    runtime: &tokio::runtime::Runtime,
    context: &Context,
    command: GetCommand,
) -> Result<()> {
    let (token, object, format) = match command {
        GetCommand::Claim(args) => {
            let filter = filter(args.filters, ObjectKind::Claim, &context.build)?;
            let pair = match args.id {
                Some(id) => {
                    if filter != ListFilter::new(ObjectKind::Claim) {
                        return Err(CliError::Input("choose a claim ID or filters".into()));
                    }
                    exact(runtime, context, ObjectKind::Claim, &id)?
                }
                None => {
                    if filter == ListFilter::new(ObjectKind::Claim) {
                        return Err(CliError::Input(
                            "provide a claim ID or at least one filter".into(),
                        ));
                    }
                    unique(runtime, context, filter)?
                }
            };
            (pair.0, pair.1, args.output.format)
        }
        GetCommand::Testament(args) => {
            let (token, object) = exact(runtime, context, ObjectKind::Testament, &args.id)?;
            (token, object, args.output.format)
        }
        GetCommand::Validation(args) => return validation(runtime, context, args),
        GetCommand::Artifact(args) => {
            let (token, object) = exact(runtime, context, ObjectKind::Artifact, &args.id)?;
            if let Some(path) = args.output {
                let ReadObject::Artifact { value, .. } = &object else {
                    return Err(CliError::InvalidResponse);
                };
                super::download::artifact(runtime, context, value, &path)?;
            }
            (token, object, args.display.format)
        }
    };
    output::object(token, &object, format)
}
