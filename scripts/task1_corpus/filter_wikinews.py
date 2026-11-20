from pathlib import Path
from datetime import datetime, timezone
import bz2
import hashlib
import json
import xml.etree.ElementTree as ET


BASE = Path(__file__).resolve().parent
OUTPUT = BASE / "filtered"

FILENAMES = [
    "enwikinews-20261001-pages-meta-history1.xml-p1p481753.bz2",
    "enwikinews-20261001-pages-meta-history1.xml-p481754p3096188.bz2",
]

# Include all revisions from 2013 through 2017.
START = "2013-01-01T00:00:00Z"
END = "2018-01-01T00:00:00Z"


def sha256(path):
    digest = hashlib.sha256()

    with path.open("rb") as file:
        for block in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(block)

    return digest.hexdigest()


def process_file(source, destination):
    counts = {
        "pages_seen": 0,
        "article_pages": 0,
        "article_revisions_seen": 0,
        "revisions_kept": 0,
        "kept_revisions_with_missing_or_empty_text": 0,
    }

    earliest = None
    latest = None
    page_info = None
    page_element = None

    with bz2.open(source, "rb") as compressed:
        context = ET.iterparse(compressed, events=("start", "end"))
        _, root = next(context)

        # Detect the XML namespace used by this file.
        namespace = root.tag.split("}")[0] + "}" if "}" in root.tag else ""

        def field(element, name):
            return element.findtext(namespace + name)

        with destination.open(
            "w", encoding="utf-8", newline="\n"
        ) as output:
            for event, element in context:
                tag = element.tag.removeprefix(namespace)

                if event == "start" and tag == "page":
                    page_element = element
                    page_info = None

                elif event == "end" and tag == "revision":
                    # Page metadata appears before its revisions.
                    if page_info is None:
                        page_info = {
                            "page_id": field(page_element, "id"),
                            "title": field(page_element, "title"),
                            "namespace": field(page_element, "ns"),
                        }

                    # Keep only pages in the main article namespace.
                    if page_info["namespace"] == "0":
                        counts["article_revisions_seen"] += 1
                        timestamp = field(element, "timestamp")

                        # Include 2013–2017; exclude 2018 onwards.
                        if timestamp and START <= timestamp < END:
                            text_element = element.find(namespace + "text")

                            text = (
                                text_element.text
                                if text_element is not None
                                else None
                            )

                            record = {
                                **page_info,
                                "revision_id": field(element, "id"),
                                "parent_id": field(element, "parentid"),
                                "revision_timestamp": timestamp,
                                "text": text,
                                "text_attributes": (
                                    dict(text_element.attrib)
                                    if text_element is not None
                                    else {}
                                ),
                                "content_model": field(element, "model"),
                                "content_format": field(element, "format"),
                                "revision_sha1": field(element, "sha1"),
                                "source_file": source.name,
                                "source_url": (
                                    "https://dumps.wikimedia.org/"
                                    f"enwikinews/20261001/{source.name}"
                                ),
                                "snapshot_date": "2026-10-01",
                            }

                            output.write(
                                json.dumps(record, ensure_ascii=False) + "\n"
                            )

                            counts["revisions_kept"] += 1

                            if not text:
                                counts[
                                    "kept_revisions_with_missing_or_empty_text"
                                ] += 1

                            earliest = (
                                min(earliest, timestamp)
                                if earliest
                                else timestamp
                            )
                            latest = (
                                max(latest, timestamp)
                                if latest
                                else timestamp
                            )

                    # Release each revision to limit memory usage.
                    page_element.remove(element)
                    element.clear()

                elif event == "end" and tag == "page":
                    counts["pages_seen"] += 1

                    if field(element, "ns") == "0":
                        counts["article_pages"] += 1

                    root.remove(element)
                    element.clear()

                    if counts["pages_seen"] % 10000 == 0:
                        print(
                            f"  Pages: {counts['pages_seen']:,}; "
                            f"revisions kept: {counts['revisions_kept']:,}",
                            flush=True,
                        )

    return {
        **counts,
        "earliest_kept_revision": earliest,
        "latest_kept_revision": latest,
    }


def main():
    # Read input files directly beside this Python script.
    sources = [BASE / name for name in FILENAMES]

    for source in sources:
        if not source.is_file():
            raise FileNotFoundError(
                f"Missing input file: {source}\n"
                "Place both .bz2 files in the same folder as this script."
            )

    OUTPUT.mkdir(exist_ok=True)
    manifest_path = OUTPUT / "filter_manifest.json"

    # Prevent accidental replacement of an earlier run.
    if manifest_path.exists() or list(OUTPUT.glob("*.jsonl*")):
        raise FileExistsError(
            "The filtered folder already contains output. "
            "Move that output elsewhere before running again."
        )

    manifest = {
        "processed_at_utc": datetime.now(timezone.utc).isoformat(),
        "filter": {
            "namespace": 0,
            "revision_timestamp_start_inclusive": START,
            "revision_timestamp_end_exclusive": END,
        },
        "files": [],
    }

    for number, source in enumerate(sources, start=1):
        print(f"\nProcessing {source.name}", flush=True)

        destination = OUTPUT / f"wikinews_2013_2017_part{number}.jsonl"
        temporary = destination.with_suffix(".jsonl.partial")

        statistics = process_file(source, temporary)
        temporary.rename(destination)

        print("Calculating checksums...", flush=True)

        manifest["files"].append({
            "source_file": source.name,
            "source_bytes": source.stat().st_size,
            "source_sha256": sha256(source),
            "output_file": destination.name,
            "output_bytes": destination.stat().st_size,
            "output_sha256": sha256(destination),
            **statistics,
        })

        print(
            f"Saved {statistics['revisions_kept']:,} revisions.",
            flush=True,
        )

    manifest_path.write_text(
        json.dumps(manifest, indent=2),
        encoding="utf-8",
    )

    total_kept = sum(
        item["revisions_kept"] for item in manifest["files"]
    )

    print(f"\nCompleted. Total revisions kept: {total_kept:,}")
    print(f"Output folder: {OUTPUT}")


if __name__ == "__main__":
    main()
    