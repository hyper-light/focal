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

fn fetch(
    runtime: &tokio::runtime::Runtime,
    context: &Context,
    request: Operation,
) -> Result<ListPage> {
    let envelope = context.envelope(request)?;
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
        ListCommand::Evaluations(_)
        | ListCommand::Receipts(_)
        | ListCommand::Monitors(_)
        | ListCommand::Events(_) => {
            return Err(CliError::Input(
                "this list family exists only on the native engine".into(),
            ));
        }
    };
    if let Some(flag) = args.filters.native_only() {
        return Err(CliError::Input(format!(
            "{flag} is served only by the native engine"
        )));
    }
    let mut document = authored::filters(args.filters);
    document.cursor = args.cursor;
    document.limit = args.limit;
    document.max_visits = args.max_visits;
    let request = document.build_operation(kind, &context.build)?;
    if args.all {
        return list_all(runtime, context, request, args.output.format);
    }
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
    if args.context {
        let view = runtime
            .block_on(context.client.validation_context(request))
            .map_err(super::validation_context_error)?;
        return output::validation_context(&view, args.output.format);
    }
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
pub(super) fn get(
    runtime: &tokio::runtime::Runtime,
    context: &Context,
    command: GetCommand,
) -> Result<()> {
    let (token, object, format) = match command {
        GetCommand::Claim(args) => {
            if let Some(flag) = args.filters.native_only() {
                return Err(CliError::Input(format!(
                    "{flag} is served only by the native engine"
                )));
            }
            let mut selector = authored::filters(args.filters);
            selector.limit = 2;
            let selection = selector.build_operation(ObjectKind::Claim, &context.build)?;
            let operation = match args.id {
                Some(id) => {
                    if !matches!(&selection, Operation::List(query) if query.filter == ListFilter::new(ObjectKind::Claim))
                    {
                        return Err(CliError::Input("choose a claim ID or filters".into()));
                    }
                    Operation::Read(
                        GetDocument {
                            id,
                            prefix: None,
                            after: None,
                            limit: 1,
                        }
                        .build(ObjectKind::Claim, &context.build)?,
                    )
                }
                None => selection,
            };
            let page = runtime
                .block_on(context.client.claim_get(context.envelope(operation)?))
                .map_err(|error| match error {
                    focal_client::claim_get::ClaimGetError::Client(error) => {
                        CliError::Client(error)
                    }
                    focal_client::claim_get::ClaimGetError::NotFound => CliError::NotFound,
                    focal_client::claim_get::ClaimGetError::Ambiguous => CliError::Ambiguous,
                    focal_client::claim_get::ClaimGetError::InvalidRequest => {
                        CliError::Input(error.to_string())
                    }
                    focal_client::claim_get::ClaimGetError::Incomplete => CliError::Incomplete,
                })?;
            let pair = (
                page.token,
                page.objects
                    .into_iter()
                    .next()
                    .ok_or(CliError::InvalidResponse)?,
            );
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

/// Stream one authenticated fixed-prefix page at a time. A sink failure never
/// advances past the page that could not be completely flushed.
fn list_all(
    runtime: &tokio::runtime::Runtime,
    context: &Context,
    mut request: Operation,
    format: OutputFormat,
) -> Result<()> {
    use std::io::{self, Write};
    use tokio::io::AsyncWriteExt;
    let mut pages = 0u64;
    let mut prefix = None;
    let mut stdout = tokio::io::stdout();
    let mut interrupted = std::pin::pin!(tokio::signal::ctrl_c());
    let result=runtime.block_on(async {
        loop {
            let envelope=context.envelope(request.clone())?;
            let response=tokio::select! {
                reply=context.client.request(envelope)=>reply?,
                signal=&mut interrupted=>{signal?;return Err(CliError::Io(io::Error::new(io::ErrorKind::Interrupted,"list output interrupted")));}
            };
            let Response::Listed(page)=response.result else {return Err(CliError::InvalidResponse);};
            if prefix.is_some_and(|token|token!=page.token)
                || page.next.as_ref().is_some_and(|next|Some(next)==selection_request(&request).and_then(|query|query.cursor.as_ref()))
                || (page.next.is_some() && page.visited==0) {
                return Err(CliError::InvalidResponse);
            }
            prefix=Some(page.token);
            let mut bytes=ListOutput(Vec::new());
            output::page_stream_to(&page,&mut bytes,format)?;
            tokio::select! {
                result=async {stdout.write_all(&bytes.0).await?;stdout.flush().await}=>result?,
                signal=&mut interrupted=>{signal?;return Err(CliError::Io(io::Error::new(io::ErrorKind::Interrupted,"list output interrupted")));}
            }
            pages=pages.checked_add(1).ok_or(InputError::Capacity)?;
            // Drop both the page and its encoded buffer before admitting another
            // network response. Empty matching pages still carry continuation.
            let query=selection_request_mut(&mut request).ok_or(CliError::InvalidResponse)?;
            query.cursor=page.next;
            if query.cursor.is_none() {return Ok(());}
        }
    });
    if result.is_err() {
        let mut error = io::stderr().lock();
        let _ = writeln!(
            error,
            "List incomplete after {pages} fully flushed pages; retain those pages as a partial result."
        );
        if let Some(cursor) = selection_request(&request).and_then(|query| query.cursor.as_ref()) {
            let _ = writeln!(
                error,
                "Resume the same list filters and limits with --cursor {} (the original read lease must remain valid). The interrupted page may have emitted partial bytes.",
                output::hex(&cursor.bytes)
            );
        } else {
            let _ = writeln!(
                error,
                "The first page was not fully flushed; restart the list and discard its partial output."
            );
        }
    }
    result
}
/// One output page may expand its bounded wire strings/arrays. The cap applies
/// before any bytes reach stdout, including YAML expansion; never a whole list.
struct ListOutput(Vec<u8>);
impl std::io::Write for ListOutput {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let size = self
            .0
            .len()
            .checked_add(bytes.len())
            .filter(|size| *size <= 16 * 1024 * 1024)
            .ok_or_else(|| std::io::Error::other("list page output exceeds 16 MiB"))?;
        if size > self.0.capacity() {
            let target = self
                .0
                .capacity()
                .checked_mul(2)
                .unwrap_or(16 * 1024 * 1024)
                .max(size)
                .min(16 * 1024 * 1024);
            self.0
                .try_reserve_exact(target.saturating_sub(self.0.len()))
                .map_err(|_| std::io::Error::other("list page output capacity"))?;
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
