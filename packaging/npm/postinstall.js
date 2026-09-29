// Resolves the platform package that carries the `focal` executable for this
// machine and, on Unix, puts that executable in place of the `focal` shim so
// the command on PATH is the native binary with no Node process in front of
// it. On Windows the shim stays: npm's generated `focal.cmd` wrapper runs it
// through node, and it spawns `focal.exe`.
//
// Which package is chosen is decided by the same facts npm itself uses to
// pick among the optional dependencies (`os`, `cpu`, `libc`): platform,
// architecture, and on Linux whether this Node runs on glibc. Node built
// against glibc reports its runtime version in the process report; Node built
// on musl (Alpine) does not. The musl executable is fully static and runs on
// either, so when the C library cannot be told, musl is the safe choice.
"use strict";

const fs = require("node:fs");
const path = require("node:path");

const binaryName = process.platform === "win32" ? "focal.exe" : "focal";

function linuxLibc() {
  try {
    const header = process.report && process.report.getReport().header;
    if (header && typeof header.glibcVersionRuntime === "string") return "gnu";
  } catch (_) {
    // No process report: decided below.
  }
  return "musl";
}

function platformPackage() {
  const { platform, arch } = process;
  if (platform === "darwin" && (arch === "arm64" || arch === "x64")) {
    return `@hyper-light/focal-darwin-${arch}`;
  }
  if (platform === "linux" && (arch === "arm64" || arch === "x64")) {
    return `@hyper-light/focal-linux-${arch}-${linuxLibc()}`;
  }
  if (platform === "win32" && (arch === "arm64" || arch === "x64")) {
    return `@hyper-light/focal-win32-${arch}-msvc`;
  }
  return null;
}

function binaryDirectory() {
  const name = platformPackage();
  if (name === null) return null;
  try {
    const directory = path.dirname(
      require.resolve(`${name}/package.json`, { paths: [__dirname] }),
    );
    if (fs.existsSync(path.join(directory, binaryName))) return directory;
  } catch (_) {
    // The optional dependency was not installed (`--no-optional`, or an
    // unsupported platform); reported by the caller.
  }
  return null;
}

function binaryPath() {
  const directory = binaryDirectory();
  return directory === null ? null : path.join(directory, binaryName);
}

function install() {
  const source = binaryPath();
  if (source === null) {
    const name = platformPackage();
    console.error(
      name === null
        ? `[focal] no prebuilt executable for ${process.platform}-${process.arch}; ` +
            "build from source: https://github.com/hyper-light/focal#install"
        : `[focal] the platform package ${name} is not installed; ` +
            "install without --no-optional, or install that package explicitly",
    );
    process.exit(1);
  }
  if (process.platform === "win32") return;
  const destination = path.join(__dirname, binaryName);
  try {
    fs.rmSync(destination, { force: true });
    try {
      fs.linkSync(source, destination);
    } catch (_) {
      fs.copyFileSync(source, destination);
    }
    fs.chmodSync(destination, 0o755);
  } catch (error) {
    console.error(`[focal] could not place the executable at ${destination}: ${error.message}`);
    process.exit(1);
  }
}

module.exports = { binaryName, platformPackage, binaryPath };

if (require.main === module) install();
