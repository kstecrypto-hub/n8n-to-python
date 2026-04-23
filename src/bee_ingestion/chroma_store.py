from __future__ import annotations

import chromadb

from src.bee_ingestion.models import Chunk, PageAsset
from src.bee_ingestion.settings import settings


class ChromaStore:
    def __init__(self, collection_name: str | None = None, persist_path: str | None = None) -> None:
        # The store can target local persistence or a Chroma HTTP server with the same API surface.
        if settings.chroma_host:
            self.client = chromadb.HttpClient(
                host=settings.chroma_host,
                port=settings.chroma_port,
                ssl=settings.chroma_ssl,
            )
        else:
            self.client = chromadb.PersistentClient(path=persist_path or settings.chroma_path)
        self.collection_name = collection_name or settings.chroma_collection

    @staticmethod
    def _publish_state_value(publish_state: str | None) -> str:
        value = str(publish_state or "ready").strip().lower()
        if value not in {"ready", "staged"}:
            raise ValueError("publish_state must be 'ready' or 'staged'")
        return value

    @staticmethod
    def _chunk_metadata(chunk: Chunk, publish_state: str) -> dict:
        return {
            "document_id": chunk.document_id,
            "tenant_id": chunk.tenant_id,
            "chunk_index": chunk.chunk_index,
            "page_start": chunk.page_start or 0,
            "page_end": chunk.page_end or 0,
            "section": " / ".join(chunk.section_path),
            "content_type": chunk.content_type,
            "parser_version": chunk.parser_version,
            "chunker_version": chunk.chunker_version,
            "prev_chunk_id": chunk.prev_chunk_id or "",
            "next_chunk_id": chunk.next_chunk_id or "",
            "chunk_role": str(chunk.metadata.get("chunk_role", "")),
            "document_class": str(chunk.metadata.get("document_class", "")),
            "section_title": str(chunk.metadata.get("section_title", "")),
            "title": str(chunk.metadata.get("title", "")),
            "section_heading": str(chunk.metadata.get("section_heading", "")),
            "hierarchy_path": " / ".join(chunk.metadata.get("hierarchy_path", [])),
            "language": str(chunk.metadata.get("language", "")),
            "token_count": int(chunk.metadata.get("token_count", 0)),
            "provenance_ref": str(chunk.metadata.get("provenance_ref", "")),
            "quality_flags": ",".join(chunk.metadata.get("quality_flags", [])),
            "surface_terms": ",".join(chunk.metadata.get("surface_terms", [])),
            "canonical_terms": ",".join(chunk.metadata.get("canonical_terms", [])),
            "validation_status": str(chunk.metadata.get("validation_status", "")),
            "quality_score": float(chunk.metadata.get("quality_score", 0.0)),
            "ontology_classes": ",".join(chunk.metadata.get("ontology_classes", [])),
            "publish_state": publish_state,
        }

    @staticmethod
    def _asset_metadata(asset: PageAsset, publish_state: str) -> dict:
        return {
            "asset_id": asset.asset_id,
            "document_id": asset.document_id,
            "tenant_id": asset.tenant_id,
            "page_number": asset.page_number,
            "asset_index": asset.asset_index,
            "asset_type": asset.asset_type,
            "asset_path": asset.asset_path,
            "important_terms": ",".join(asset.metadata.get("important_terms", [])),
            "publish_state": publish_state,
        }

    @staticmethod
    def _merge_where_clause(
        tenant_id: str | None = None,
        document_ids: list[str] | None = None,
        publish_state: str | None = "ready",
    ) -> dict | None:
        clauses: list[dict] = []
        normalized_state = ChromaStore._publish_state_value(publish_state) if publish_state else None
        if normalized_state:
            clauses.append({"publish_state": normalized_state})
        if tenant_id:
            clauses.append({"tenant_id": tenant_id})
        if document_ids:
            if len(document_ids) == 1:
                clauses.append({"document_id": document_ids[0]})
            else:
                clauses.append({"$or": [{"document_id": item} for item in document_ids]})
        if not clauses:
            return None
        if len(clauses) == 1:
            return clauses[0]
        return {"$and": clauses}

    @staticmethod
    def _collection_metadata(kind: str, name: str) -> dict:
        return {
            "kind": kind,
            "name": name,
            "tenant_scope": "multi-tenant",
            "record_id_field": "chunk_id" if kind == "chunks" else "asset_id",
            "document_field": "document_id",
        }

    @staticmethod
    def _ensure_collection_metadata(collection, metadata: dict) -> None:
        current = dict(getattr(collection, "metadata", None) or {})
        if current == metadata:
            return
        modify = getattr(collection, "modify", None)
        if callable(modify):
            modify(metadata=metadata)

    @property
    def collection(self):
        metadata = self._collection_metadata("chunks", self.collection_name)
        collection = self.client.get_or_create_collection(name=self.collection_name, metadata=metadata)
        self._ensure_collection_metadata(collection, metadata)
        return collection

    @property
    def asset_collection(self):
        metadata = self._collection_metadata("assets", settings.chroma_asset_collection)
        collection = self.client.get_or_create_collection(name=settings.chroma_asset_collection, metadata=metadata)
        self._ensure_collection_metadata(collection, metadata)
        return collection

    def upsert_chunks(self, chunks: list[Chunk], embeddings: list[list[float]], publish_state: str = "ready") -> None:
        # Metadata mirrors the Postgres truth closely enough that agent retrieval can stay mostly index-local.
        normalized_state = self._publish_state_value(publish_state)
        batch_size = max(1, settings.chroma_upsert_batch_size)
        for start in range(0, len(chunks), batch_size):
            chunk_batch = chunks[start : start + batch_size]
            embedding_batch = embeddings[start : start + batch_size]
            self.collection.upsert(
                ids=[chunk.chunk_id for chunk in chunk_batch],
                documents=[chunk.text for chunk in chunk_batch],
                embeddings=embedding_batch,
                metadatas=[self._chunk_metadata(chunk, normalized_state) for chunk in chunk_batch],
            )

    def upsert_assets(self, assets: list[PageAsset], embeddings: list[list[float]], publish_state: str = "ready") -> None:
        normalized_state = self._publish_state_value(publish_state)
        batch_size = max(1, settings.chroma_upsert_batch_size)
        for start in range(0, len(assets), batch_size):
            asset_batch = assets[start : start + batch_size]
            embedding_batch = embeddings[start : start + batch_size]
            self.asset_collection.upsert(
                ids=[asset.asset_id for asset in asset_batch],
                documents=[asset.search_text for asset in asset_batch],
                embeddings=embedding_batch,
                metadatas=[self._asset_metadata(asset, normalized_state) for asset in asset_batch],
            )

    def set_chunk_publish_state(self, chunk_ids: list[str], publish_state: str) -> None:
        ids = [item for item in chunk_ids if str(item).strip()]
        if not ids:
            return
        normalized_state = self._publish_state_value(publish_state)
        self.collection.update(ids=ids, metadatas=[{"publish_state": normalized_state} for _ in ids])

    def set_asset_publish_state(self, asset_ids: list[str], publish_state: str) -> None:
        ids = [item for item in asset_ids if str(item).strip()]
        if not ids:
            return
        normalized_state = self._publish_state_value(publish_state)
        self.asset_collection.update(ids=ids, metadatas=[{"publish_state": normalized_state} for _ in ids])

    def delete_chunks(self, chunk_ids: list[str]) -> None:
        ids = [item for item in chunk_ids if str(item).strip()]
        if ids:
            self.collection.delete(ids=ids)

    def delete_assets(self, asset_ids: list[str]) -> None:
        ids = [item for item in asset_ids if str(item).strip()]
        if ids:
            self.asset_collection.delete(ids=ids)

    def list_records(self, document_id: str | None = None, limit: int = 50, offset: int = 0, collection_name: str | None = None) -> list[dict]:
        collection = self.client.get_or_create_collection(name=collection_name or self.collection_name)
        where = {"document_id": document_id} if document_id else None
        result = collection.get(
            where=where,
            limit=limit,
            offset=offset,
            include=["documents", "metadatas"],
        )
        ids = result.get("ids", [])
        documents = result.get("documents", [])
        metadatas = result.get("metadatas", [])
        rows: list[dict] = []
        for index, record_id in enumerate(ids):
            rows.append(
                {
                    "id": record_id,
                    "document": documents[index] if index < len(documents) else None,
                    "metadata": metadatas[index] if index < len(metadatas) else {},
                }
        )
        return rows

    def search(
        self,
        query_embedding: list[float],
        top_k: int = 8,
        tenant_id: str | None = None,
        document_ids: list[str] | None = None,
    ) -> list[dict]:
        # Tenant/document filters are applied inside Chroma so cross-tenant retrieval never leaves the index.
        where = self._merge_where_clause(tenant_id=tenant_id, document_ids=document_ids, publish_state="ready")

        result = self.collection.query(
            query_embeddings=[query_embedding],
            n_results=top_k,
            where=where,
            include=["documents", "metadatas", "distances"],
        )

        ids = result.get("ids", [[]])[0]
        documents = result.get("documents", [[]])[0]
        metadatas = result.get("metadatas", [[]])[0]
        distances = result.get("distances", [[]])[0]

        rows: list[dict] = []
        for index, chunk_id in enumerate(ids):
            rows.append(
                {
                    "chunk_id": chunk_id,
                    "document": documents[index] if index < len(documents) else None,
                    "metadata": metadatas[index] if index < len(metadatas) else {},
                    "distance": distances[index] if index < len(distances) else None,
                    "rank": index + 1,
                }
            )
        return rows

    def search_assets(
        self,
        query_embedding: list[float],
        top_k: int = 6,
        tenant_id: str | None = None,
        document_ids: list[str] | None = None,
    ) -> list[dict]:
        where = self._merge_where_clause(tenant_id=tenant_id, document_ids=document_ids, publish_state="ready")

        result = self.asset_collection.query(
            query_embeddings=[query_embedding],
            n_results=top_k,
            where=where,
            include=["documents", "metadatas", "distances"],
        )

        ids = result.get("ids", [[]])[0]
        documents = result.get("documents", [[]])[0]
        metadatas = result.get("metadatas", [[]])[0]
        distances = result.get("distances", [[]])[0]
        rows: list[dict] = []
        for index, asset_id in enumerate(ids):
            rows.append(
                {
                    "asset_id": asset_id,
                    "document": documents[index] if index < len(documents) else None,
                    "metadata": metadatas[index] if index < len(metadatas) else {},
                    "distance": distances[index] if index < len(distances) else None,
                    "rank": index + 1,
                }
            )
        return rows

    def get_record(self, chunk_id: str) -> dict | None:
        result = self.collection.get(
            ids=[chunk_id],
            include=["documents", "metadatas"],
        )
        ids = result.get("ids", [])
        if not ids:
            return None
        documents = result.get("documents", [])
        metadatas = result.get("metadatas", [])
        return {
            "id": ids[0],
            "document": documents[0] if documents else None,
            "metadata": metadatas[0] if metadatas else {},
        }

    def get_asset_record(self, asset_id: str) -> dict | None:
        result = self.asset_collection.get(ids=[asset_id], include=["documents", "metadatas"])
        ids = result.get("ids", [])
        if not ids:
            return None
        documents = result.get("documents", [])
        metadatas = result.get("metadatas", [])
        return {
            "id": ids[0],
            "document": documents[0] if documents else None,
            "metadata": metadatas[0] if metadatas else {},
        }

    def count_records(self, document_id: str | None = None, collection_name: str | None = None) -> int:
        collection = self.client.get_or_create_collection(name=collection_name or self.collection_name)
        # Use the collection count for the global case so live polling does not full-scan the corpus.
        if document_id is None:
            return int(collection.count())
        # Chroma does not expose a filtered count primitive, so document-scoped counts still derive from ids.
        where = {"document_id": document_id} if document_id else None
        result = collection.get(
            where=where,
            include=[],
        )
        return len(result.get("ids", []))

    def list_collections(self) -> list:
        return self.client.list_collections()

    def delete_document(self, document_id: str) -> None:
        self.collection.delete(where={"document_id": document_id})
        self.asset_collection.delete(where={"document_id": document_id})

    def delete_chunk(self, chunk_id: str) -> None:
        self.collection.delete(ids=[chunk_id])

    def delete_asset(self, asset_id: str) -> None:
        self.asset_collection.delete(ids=[asset_id])

    def reset_collection(self) -> None:
        # Reset is destructive by design: delete the collection, then recreate an empty shell with the same name.
        self._reset_named_collection(self.collection_name)
        self._reset_named_collection(settings.chroma_asset_collection)

    def _reset_named_collection(self, name: str) -> None:
        kind = "assets" if name == settings.chroma_asset_collection else "chunks"
        metadata = self._collection_metadata(kind, name)
        try:
            self.client.delete_collection(name)
        except Exception:
            pass
        collection = self.client.get_or_create_collection(name=name, metadata=metadata)
        self._ensure_collection_metadata(collection, metadata)
        if int(collection.count()) > 0:
            existing = collection.get(include=[])
            ids = list(existing.get("ids", []) or [])
            if ids:
                collection.delete(ids=ids)
        if int(collection.count()) != 0:
            raise RuntimeError(f"Unable to reset Chroma collection '{name}' cleanly")
