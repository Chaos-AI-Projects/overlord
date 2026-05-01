#!/usr/bin/env python3
"""pdf2md.py — Extract text from PDFs as markdown.

Strategy:
  1. Fast path: pdftotext — if it produces coherent text, output as markdown.
  2. Quality gate: assess extracted text quality via heuristics.
  3. Fallback: convert pages to images with pdftoppm, send to a vision model
     via LiteLLM for markdown extraction.

Usage:
    python pdf2md.py <input.pdf>

Environment variables:
    LITELLM_BASE_URL  — LiteLLM proxy base URL (default: http://localhost:4000)
    PDF_VISION_MODEL  — Vision model for fallback (default: sonnet)
"""

import base64
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import urllib.request


# --------------- configuration ---------------

LITELLM_BASE_URL = os.environ.get("LITELLM_BASE_URL", "http://localhost:4000")
PDF_VISION_MODEL = os.environ.get("PDF_VISION_MODEL", "sonnet")
GRACE_WORD_COUNT = 10
PRINTABLE_RATIO_THRESHOLD = 0.80
WORD_LIKE_RATIO_THRESHOLD = 0.60

VISION_PROMPT = (
    "Extract all text content from this PDF page image. "
    "Output as clean markdown, preserving headings, lists, tables, "
    "and formatting structure. Output only the markdown, no commentary."
)


# --------------- text quality assessment ---------------

def is_printable(ch: str) -> bool:
    """Check if a character is 'printable' (not a control char, excluding whitespace)."""
    return ch.isprintable() or ch in ("\n", "\r", "\t")


def assess_text_quality(text: str) -> bool:
    """Return True if the text looks like coherent extracted content."""
    stripped = text.strip()
    if not stripped:
        return False

    # Word count check
    words = stripped.split()
    if len(words) < GRACE_WORD_COUNT:
        return False

    # Printable character ratio
    if len(stripped) > 0:
        printable_count = sum(1 for ch in stripped if is_printable(ch))
        if printable_count / len(stripped) < PRINTABLE_RATIO_THRESHOLD:
            log(f"Printable ratio too low: {printable_count}/{len(stripped)}")
            return False

    # Word-like token ratio (3+ alphabetic characters)
    word_like = sum(1 for w in words if len(re.sub(r"[^a-zA-Z]", "", w)) >= 3)
    if len(words) > 0 and word_like / len(words) < WORD_LIKE_RATIO_THRESHOLD:
        log(f"Word-like ratio too low: {word_like}/{len(words)}")
        return False

    return True


# --------------- pdftotext extraction ---------------

def extract_with_pdftotext(pdf_path: str) -> str | None:
    """Try pdftotext and return text if quality is acceptable, else None."""
    try:
        result = subprocess.run(
            ["pdftotext", "-layout", pdf_path, "-"],
            capture_output=True,
            text=True,
            timeout=60,
        )
    except FileNotFoundError:
        log("pdftotext not found; skipping fast path")
        return None
    except subprocess.TimeoutExpired:
        log("pdftotext timed out")
        return None

    if result.returncode != 0:
        log(f"pdftotext exited with code {result.returncode}")
        return None

    text = result.stdout
    if assess_text_quality(text):
        return text

    log("pdftotext output failed quality check; falling back to vision")
    return None


# --------------- pdftoppm + vision fallback ---------------

def convert_to_images(pdf_path: str, output_dir: str) -> list[str]:
    """Convert PDF pages to PNG images using pdftoppm. Returns sorted list of paths."""
    prefix = os.path.join(output_dir, "page")
    try:
        subprocess.run(
            ["pdftoppm", "-png", "-r", "300", pdf_path, prefix],
            check=True,
            capture_output=True,
            timeout=300,
        )
    except FileNotFoundError:
        log("pdftoppm not found; cannot fall back to vision")
        return []
    except subprocess.TimeoutExpired:
        log("pdftoppm timed out")
        return []
    except subprocess.CalledProcessError as e:
        log(f"pdftoppm failed: {e.stderr.decode(errors='replace')}")
        return []

    images = sorted(
        os.path.join(output_dir, f)
        for f in os.listdir(output_dir)
        if f.startswith("page") and f.endswith(".png")
    )
    return images


def call_vision_model(image_path: str) -> str:
    """Send an image to the vision model via LiteLLM and return markdown text."""
    with open(image_path, "rb") as f:
        image_data = base64.b64encode(f.read()).decode("ascii")

    payload = {
        "model": PDF_VISION_MODEL,
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "image_url",
                        "image_url": {
                            "url": f"data:image/png;base64,{image_data}",
                        },
                    },
                    {
                        "type": "text",
                        "text": VISION_PROMPT,
                    },
                ],
            }
        ],
        "max_tokens": 4096,
    }

    url = f"{LITELLM_BASE_URL}/v1/chat/completions"
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            body = json.loads(resp.read().decode("utf-8"))
            return body["choices"][0]["message"]["content"]
    except Exception as e:
        log(f"Vision API call failed for {os.path.basename(image_path)}: {e}")
        return ""


def extract_with_vision(pdf_path: str) -> str | None:
    """Convert PDF to images and process each page with a vision model."""
    tmpdir = tempfile.mkdtemp(prefix="pdf2md_")
    try:
        images = convert_to_images(pdf_path, tmpdir)
        if not images:
            return None

        log(f"Processing {len(images)} page(s) via vision model")
        pages = []
        for i, img_path in enumerate(images, 1):
            log(f"  Page {i}/{len(images)}...")
            md = call_vision_model(img_path)
            if md:
                pages.append(md.strip())

        if not pages:
            return None

        return "\n\n---\n\n".join(pages)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


# --------------- main ---------------

def log(msg: str) -> None:
    print(f"pdf2md: {msg}", file=sys.stderr)


def main() -> int:
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <input.pdf>", file=sys.stderr)
        return 1

    pdf_path = sys.argv[1]
    if not os.path.isfile(pdf_path):
        log(f"File not found: {pdf_path}")
        return 1

    # Fast path
    text = extract_with_pdftotext(pdf_path)
    if text is not None:
        log("Using pdftotext output")
        print(text)
        return 0

    # Fallback path
    text = extract_with_vision(pdf_path)
    if text is not None:
        log("Using vision model output")
        print(text)
        return 0

    log("Failed to extract text from PDF")
    return 1


if __name__ == "__main__":
    sys.exit(main())
