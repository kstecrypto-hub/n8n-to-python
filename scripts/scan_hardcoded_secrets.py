from __future__ import annotations

import argparse
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SKIP_DIRS = {".git", ".venv", "venv", "node_modules", "dist", "build", "coverage", "__pycache__"}
SKIP_NAMES = {"package-lock.json", "pnpm-lock.yaml", "yarn.lock", "poetry.lock"}
SKIP_SUFFIXES = {".pyc", ".png", ".jpg", ".jpeg", ".gif", ".ico", ".pdf", ".sqlite", ".db"}
SCAN_SUFFIXES = {
    ".py",
    ".ts",
    ".tsx",
    ".js",
    ".jsx",
    ".json",
    ".yaml",
    ".yml",
    ".toml",
    ".ini",
    ".cfg",
    ".conf",
    ".md",
    ".txt",
    ".ps1",
    ".sh",
    ".bat",
    ".dockerfile",
}

PLACEHOLDER_FRAGMENTS = (
    "example",
    "placeholder",
    "dummy",
    "fake",
    "test",
    "local",
    "dev",
    "changeme",
    "change-me",
    "redacted",
    "replace",
    "not-set",
    "...",
    "your_",
    "your-",
    "<",
    "${",
    "$env:",
    "$",
    "env.",
    "var.",
    "ctx.",
    "req.",
    "request.",
    "args.",
    "runtime.",
    "os.getenv",
    "process.env",
    "settings.",
    "config.",
    "conf.",
    "body.",
    "$json.",
    "form",
    "window.",
    "abc123",
    "1x0",
)
PLACEHOLDER_VALUES = {
    "",
    "bee",
    "bee_auth",
    "postgres",
    "password",
    "user",
    "pass",
    "admin",
    "secret",
    "token",
    "string",
    "number",
    "boolean",
    "unknown",
    "void",
    "none",
    "null",
    "true",
    "false",
    "0",
    "1",
}

KNOWN_TOKEN_PATTERNS = [
    re.compile(r"sk-[A-Za-z0-9]{20,}"),
    re.compile(r"ghp_[A-Za-z0-9_]{20,}"),
    re.compile(r"github_pat_[A-Za-z0-9_]{20,}"),
    re.compile(r"AKIA[0-9A-Z]{16}"),
    re.compile(r"ASIA[0-9A-Z]{16}"),
    re.compile(r"AIza[0-9A-Za-z_-]{35}"),
    re.compile(r"sk_live_[0-9A-Za-z]{20,}"),
]
ASSIGNMENT_RE = re.compile(
    r"(?i)\b(api[_-]?key|apikey|secret|token|password|passwd|pwd|private[_-]?key|client[_-]?secret|jwt[_-]?secret|fernet|encryption[_-]?key)\b"
    r"\s*[:=]\s*['\"]?([^'\"\s,#}]+)"
)
URL_CREDENTIAL_RE = re.compile(
    r"(?i)\b(postgresql|postgres|mysql|mongodb(?:\+srv)?|redis)://([^:@/\s\"']+):([^@/\s\"']+)@([^/\s\"']+)"
)


def is_env_file(path: Path) -> bool:
    name = path.name.lower()
    return name == ".env" or name.startswith(".env.") or name.endswith(".env")


def should_scan(path: Path) -> bool:
    if any(part in SKIP_DIRS for part in path.parts):
        return False
    if is_env_file(path):
        return False
    if path.name == "scan_hardcoded_secrets.py":
        return False
    if path.name in SKIP_NAMES:
        return False
    if path.suffix.lower() in SKIP_SUFFIXES:
        return False
    if path.suffix.lower() in SCAN_SUFFIXES:
        return True
    return path.name.lower() in {"dockerfile", "compose.yaml", "compose.yml"}


def is_placeholder(value: str) -> bool:
    normalized = value.strip().strip("'\"").lower()
    if normalized in PLACEHOLDER_VALUES:
        return True
    return any(fragment in normalized for fragment in PLACEHOLDER_FRAGMENTS)


def redact(value: str) -> str:
    text = value.strip().strip("'\"")
    if len(text) <= 8:
        return "...REDACTED..."
    return f"{text[:4]}...REDACTED...{text[-4:]}"


def scan_file(path: Path) -> list[str]:
    findings: list[str] = []
    try:
      text = path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
      text = path.read_text(encoding="utf-8", errors="ignore")
    for line_no, line in enumerate(text.splitlines(), start=1):
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        lowered = stripped.lower()
        if any(context in lowered for context in ["for example", "create token", "secret put", "function ", "interface "]):
            continue
        for pattern in KNOWN_TOKEN_PATTERNS:
            for match in pattern.finditer(stripped):
                if any(fragment in lowered for fragment in PLACEHOLDER_FRAGMENTS):
                    continue
                findings.append(f"{path.relative_to(ROOT)}:{line_no}: known token pattern {redact(match.group(0))}")
        if "-----BEGIN" in stripped and "PRIVATE KEY" in stripped:
            findings.append(f"{path.relative_to(ROOT)}:{line_no}: private key material marker")
        for match in URL_CREDENTIAL_RE.finditer(stripped):
            user = match.group(2)
            password = match.group(3)
            host = match.group(4).lower()
            if host.startswith("localhost") or host.startswith("127.0.0.1") or host.startswith("postgres"):
                continue
            if not is_placeholder(user) or not is_placeholder(password):
                findings.append(f"{path.relative_to(ROOT)}:{line_no}: credentialed URL {match.group(1)}://{redact(user)}:{redact(password)}@")
        for match in ASSIGNMENT_RE.finditer(stripped):
            value = match.group(2).rstrip("',\")")
            if any(marker in value for marker in ["(", ")", "[", ".", "`"]):
                continue
            if len(value) < 8 or is_placeholder(value):
                continue
            findings.append(f"{path.relative_to(ROOT)}:{line_no}: {match.group(1)}={redact(value)}")
    return findings


def main() -> int:
    parser = argparse.ArgumentParser(description="Scan repo-authored files for likely hardcoded secrets outside env files.")
    parser.add_argument("--list-tracked-env", action="store_true", help="Also print tracked env files discovered by git ls-files fallback checks.")
    args = parser.parse_args()

    findings: list[str] = []
    for path in ROOT.rglob("*"):
        if path.is_file() and should_scan(path):
            findings.extend(scan_file(path))

    if findings:
        print("Potential hardcoded secrets found:")
        for finding in findings:
            print(f"- {finding}")
        return 1
    print("No likely hardcoded secrets found outside env files.")
    if args.list_tracked_env:
        print("Tracked env files must be checked separately with: git ls-files | rg -i '(^|/)\\.env($|\\.)|\\.env$'")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
