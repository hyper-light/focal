use crate::{ProtocolError, Tool};
use focal_client::operations::{self, OperationDescriptor};

/// Tools derived from the shared administration descriptors.
pub(crate) const TOOL_COUNT: usize = operations::ADMIN_TOOL_COUNT;
pub(crate) fn append(tools: &mut Vec<Tool>) -> Result<(), ProtocolError> {
    tools
        .try_reserve_exact(TOOL_COUNT)
        .map_err(|_| ProtocolError::Capacity)?;
    for descriptor in operations::admin_descriptors() {
        tools.push(tool(descriptor)?);
    }
    Ok(())
}
fn tool(descriptor: &OperationDescriptor) -> Result<Tool, ProtocolError> {
    Ok(Tool {
        name: descriptor.name.into(),
        description: descriptor.description.into(),
        input_schema: descriptor
            .input_schema()
            .map_err(|_| ProtocolError::Limits)?,
        output_schema: crate::catalog::output_schema(descriptor.name)?,
        read_only: descriptor.read_only(),
        destructive: descriptor.destructive,
        idempotent: descriptor.repeatable(),
    })
}
