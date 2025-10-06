"""
Storage abstraction layer with LRU caching support.

Supports both Google Cloud Storage (GCS) and local filesystem as backends.
"""

import os
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import List, Optional

from google.auth.credentials import AnonymousCredentials
from google.cloud import storage


class BlobInfo:
    """Unified blob/file information wrapper."""

    def __init__(self, name: str, size: int, updated: str, etag: str):
        self.name = name
        self.size = size
        self.updated = updated
        self.etag = etag


class StorageBackend:
    """Base storage backend interface."""

    def get_blob_content(self, bucket: str, blob_name: str) -> Optional[bytes]:
        """Get blob content as bytes."""
        raise NotImplementedError

    def list_blobs(self, bucket: str, prefix: str = "") -> List[BlobInfo]:
        """List blobs with given prefix."""
        raise NotImplementedError


class GCSBackend(StorageBackend):
    """Google Cloud Storage backend."""

    def __init__(self, project: str = "PROJECT"):
        self.project = project
        self._client = None

    @property
    def client(self):
        """Lazy client initialization."""
        if self._client is None:
            if os.environ.get("STORAGE_EMULATOR_HOST") is not None:
                self._client = storage.Client(
                    credentials=AnonymousCredentials(),
                    project=self.project,
                )
            else:  # pragma: no cover
                self._client = storage.Client(project=self.project)
        return self._client

    def get_blob_content(self, bucket: str, blob_name: str) -> Optional[bytes]:
        """Get blob content from GCS."""
        try:
            gcs_bucket = self.client.get_bucket(bucket)
            blob = gcs_bucket.get_blob(blob_name)
            if blob is None:
                return None
            return blob.download_as_bytes()
        except Exception:
            return None

    def list_blobs(self, bucket: str, prefix: str = "") -> List[BlobInfo]:
        """List blobs from GCS."""
        gcs_bucket = self.client.get_bucket(bucket)
        blobs = list(self.client.list_blobs(bucket_or_name=gcs_bucket, prefix=prefix))
        return [
            BlobInfo(
                name=blob.name,
                size=blob.size,
                updated=blob.updated.isoformat(),
                etag=blob.etag,
            )
            for blob in blobs
        ]


class LocalFilesystemBackend(StorageBackend):
    """Local filesystem backend."""

    def __init__(self, base_path: str = "/data"):
        self.base_path = Path(base_path)

    def get_blob_content(self, bucket: str, blob_name: str) -> Optional[bytes]:
        """Get file content from local filesystem."""
        file_path = self.base_path / bucket / blob_name
        try:
            if file_path.exists() and file_path.is_file():
                return file_path.read_bytes()
        except Exception:
            pass
        return None

    def list_blobs(self, bucket: str, prefix: str = "") -> List[BlobInfo]:
        """List files from local filesystem."""
        bucket_path = self.base_path / bucket
        if not bucket_path.exists():
            return []

        prefix_path = bucket_path / prefix if prefix else bucket_path
        blobs = []

        # If prefix points to a directory, list files in it
        if prefix_path.is_dir():
            for file_path in prefix_path.rglob("*"):
                if file_path.is_file():
                    relative_path = file_path.relative_to(bucket_path)
                    stat = file_path.stat()
                    blobs.append(
                        BlobInfo(
                            name=str(relative_path),
                            size=stat.st_size,
                            updated=datetime.fromtimestamp(stat.st_mtime).isoformat()
                            + "Z",
                            etag=f'"{hash(str(relative_path))}"',
                        )
                    )
        # If prefix is a partial path, find matching files
        else:
            for file_path in bucket_path.rglob("*"):
                if file_path.is_file():
                    relative_path = str(file_path.relative_to(bucket_path))
                    if relative_path.startswith(prefix):
                        stat = file_path.stat()
                        blobs.append(
                            BlobInfo(
                                name=relative_path,
                                size=stat.st_size,
                                updated=datetime.fromtimestamp(
                                    stat.st_mtime
                                ).isoformat()
                                + "Z",
                                etag=f'"{hash(relative_path)}"',
                            )
                        )

        blobs.sort(key=lambda blob: blob.name)
        return blobs


class CachedStorageBackend:
    """Storage backend with LRU caching."""

    def __init__(self, backend: StorageBackend, cache_size: int = 128):
        self.backend = backend
        self.cache_size = cache_size
        # Create cached version of get_blob_content
        self._cached_get_blob_content = lru_cache(maxsize=cache_size)(
            self._get_blob_content_impl
        )

    def _get_blob_content_impl(self, bucket: str, blob_name: str) -> Optional[bytes]:
        """Internal implementation for caching."""
        return self.backend.get_blob_content(bucket, blob_name)

    def get_blob_content(self, bucket: str, blob_name: str) -> Optional[bytes]:
        """Get blob content with caching."""
        return self._cached_get_blob_content(bucket, blob_name)

    def list_blobs(self, bucket: str, prefix: str = "") -> List[BlobInfo]:
        """List blobs (not cached as it's less frequently used)."""
        return self.backend.list_blobs(bucket, prefix)

    def clear_cache(self):
        """Clear the LRU cache."""
        self._cached_get_blob_content.cache_clear()

    def cache_info(self):
        """Get cache statistics."""
        return self._cached_get_blob_content.cache_info()


# Global storage backend instance
_storage_backend: Optional[CachedStorageBackend] = None


def get_storage_backend() -> CachedStorageBackend:
    """Get or create the global storage backend instance."""
    global _storage_backend

    if _storage_backend is None:
        # Determine which backend to use based on environment variable
        backend_type = os.environ.get("STORAGE_BACKEND", "gcs").lower()
        cache_size = int(os.environ.get("STORAGE_CACHE_SIZE", "128"))

        if backend_type == "local":
            base_path = os.environ.get("LOCAL_STORAGE_PATH", "/data")
            backend = LocalFilesystemBackend(base_path)
        else:  # default to GCS
            project = os.environ.get("GCS_PROJECT", "PROJECT")
            backend = GCSBackend(project)

        _storage_backend = CachedStorageBackend(backend, cache_size)

    return _storage_backend


def get_blob_content(bucket: str, blob_name: str) -> Optional[bytes]:
    """Get blob content from storage backend with caching."""
    backend = get_storage_backend()
    return backend.get_blob_content(bucket, blob_name)


def list_blobs(bucket: str, prefix: str = "") -> List[BlobInfo]:
    """List blobs from storage backend."""
    backend = get_storage_backend()
    return backend.list_blobs(bucket, prefix)
