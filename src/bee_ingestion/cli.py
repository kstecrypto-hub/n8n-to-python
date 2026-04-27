from __future__ import annotations

import argparse
from pathlib import Path

from src.bee_ingestion.models import SourceDocument
from src.bee_ingestion.pdf_utils import build_pdf_content_hash
from src.bee_ingestion.service import IngestionService


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the bee ingestion worker on a text file or PDF.")
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument("--reprocess-kg", action="store_true", help="Replay KG extraction for accepted chunks only.")
    mode_group.add_argument("--review-chunks", action="store_true", help="Run the LLM reviewer on chunks currently marked review.")
    mode_group.add_argument("--repair-document", action="store_true", help="Re-run chunk validation and index/KG sync for one document.")
    mode_group.add_argument("--rebuild-document", action="store_true", help="Rebuild one document from its stored source text.")
    mode_group.add_argument("--reset-data", action="store_true", help="Clear ingestion data for one document or the whole project.")
    parser.add_argument("--document-id", help="Optional document id filter for KG replay.")
    parser.add_argument("--all", action="store_true", help="Required with --reset-data to reset the entire ingestion dataset.")
    parser.add_argument("--confirm-reset-all", action="store_true", help="Second explicit confirmation required for --reset-data --all.")
    parser.add_argument("--batch-size", type=int, default=200, help="Batch size for KG replay.")
    parser.add_argument("--skip-kg", action="store_true", help="Skip KG replay when repairing a document.")
    parser.add_argument("--tenant-id", default="shared")
    parser.add_argument("--source-type", default="text")
    parser.add_argument("--document-class")
    parser.add_argument("--filename")
    input_group = parser.add_mutually_exclusive_group()
    input_group.add_argument("--file", help="Path to input text or PDF file.")
    input_group.add_argument("--text", help="Inline text to ingest.")
    parser.add_argument("--page-start", type=int, help="Optional 1-based PDF page start for slice ingest.")
    parser.add_argument("--page-end", type=int, help="Optional 1-based PDF page end for slice ingest.")
    args = parser.parse_args()

    service = IngestionService()

    if args.reset_data:
        if args.document_id and args.all:
            parser.error("--reset-data accepts either --document-id or --all, not both")
        if args.document_id:
            result = service.reset_ingestion_data(document_id=args.document_id)
            print(result)
            return
        if not args.all:
            parser.error("--reset-data requires either --document-id for a scoped reset or --all for a global reset")
        if not args.confirm_reset_all:
            parser.error("--reset-data --all also requires --confirm-reset-all")
        result = service.reset_ingestion_data(document_id=None)
        print(result)
        return

    if args.rebuild_document:
        if not args.document_id:
            parser.error("--rebuild-document requires --document-id")
        result = service.rebuild_document(document_id=args.document_id)
        print(result)
        return

    if args.reprocess_kg:
        result = service.reprocess_kg(document_id=args.document_id, batch_size=max(1, args.batch_size))
        print(result)
        return

    if args.repair_document:
        if not args.document_id:
            parser.error("--repair-document requires --document-id")
        result = service.repair_document(document_id=args.document_id, rerun_kg=not args.skip_kg)
        print(result)
        return

    if args.review_chunks:
        result = service.auto_review_chunks(document_id=args.document_id, batch_size=max(1, args.batch_size))
        print(result)
        return

    if not args.file and not args.text:
        parser.error("Provide --file or --text")

    if args.text is not None:
        raw_text = args.text
        filename = args.filename or "inline.txt"
        source_type = args.source_type
    else:
        path = Path(args.file)
        filename = args.filename or path.name
        source_type = "pdf" if path.suffix.lower() == ".pdf" else args.source_type
        raw_text = (
            ""
            if path.suffix.lower() == ".pdf"
            else path.read_text(encoding="utf-8")
        )
    document_class = args.document_class or _infer_document_class(filename, source_type)
    source_metadata = {}
    if args.file:
        source_metadata["source_path"] = str(Path(args.file))
    if args.page_start is not None or args.page_end is not None:
        source_metadata["page_range"] = {"start": args.page_start, "end": args.page_end}

    result = service.ingest_text(
        SourceDocument(
            tenant_id=args.tenant_id,
            source_type=source_type,
            filename=filename,
            raw_text=raw_text,
            metadata=source_metadata,
            document_class=document_class,
            content_hash_value=(
                _build_pdf_content_hash(Path(args.file), args.page_start, args.page_end)
                if args.file and source_type == "pdf"
                else None
            ),
        )
    )
    print(result)


def _infer_document_class(filename: str, source_type: str) -> str:
    lowered = filename.lower()
    if source_type == "pdf":
        return "book"
    if "manual" in lowered:
        return "manual"
    if "article" in lowered:
        return "article"
    if "research" in lowered or "paper" in lowered:
        return "research_paper"
    if "experience" in lowered:
        return "practical_experience"
    return "note"

if __name__ == "__main__":
    main()
