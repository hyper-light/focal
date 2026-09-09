#!/usr/bin/env python3
"""The official Python MCP SDK against a real focal adapter."""
import asyncio
import json
import os
import sys

try:
    import mcp
    from mcp import ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client
except ImportError:
    sys.exit(3)


async def main() -> int:
    binary = os.environ["FOCAL_BIN"]
    data_dir = os.environ["FOCAL_DATA_DIR"]
    print("python mcp", getattr(mcp, "__version__", "unknown"))
    params = StdioServerParameters(command=binary, args=["--data-dir", data_dir, "mcp", "serve"])
    async with stdio_client(params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            names = []
            cursor = None
            while True:
                page = await session.list_tools(cursor=cursor)
                names.extend(tool.name for tool in page.tools)
                cursor = page.nextCursor
                if not cursor:
                    break
            if not names:
                print("no tools listed", file=sys.stderr)
                return 1
            probe = "ledger.standing" if "ledger.standing" in names else "ledger.summary"
            result = await session.call_tool(probe, {})
            content = result.structuredContent or {}
            if "condition" not in content:
                text = "".join(getattr(item, "text", "") for item in result.content)
                content = json.loads(text) if text else {}
            if "condition" not in content:
                print("no structured application result", file=sys.stderr)
                return 1
            print("python-sdk: ok", len(names), "tools")
            return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
