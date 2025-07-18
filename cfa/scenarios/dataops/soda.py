import warnings
from collections.abc import Iterator, Sequence
from typing import Any, Optional
from urllib.parse import urlunparse

import httpx


def _int_divide_ceiling(a: int, b: int) -> int:
    return -(a // -b)


class Query:
    def __init__(
        self,
        domain: str,
        id: str,
        clauses: Optional[dict[str, Any]] = None,
        select: Optional[str | Sequence[str]] = None,
        where: Optional[str] = None,
        limit: Optional[int] = None,
        offset: int = 0,
        app_token: Optional[str] = None,
        verbose=True,
    ):
        self.domain = domain
        self.id = id
        self.select = select
        self.where = where
        self.limit = limit
        self.offset = offset
        self.app_token = app_token
        self.verbose = verbose
        self.clauses = clauses

    def get_all(self) -> list[dict]:
        if self.verbose:
            print(
                f"Downloading dataset {self.domain} {self.id}: {self.n_rows} rows"
            )

        if self.clauses is None:
            self.clauses = self._build_payload(
                select=self.select,
                where=self.where,
                limit=self.limit,
                offset=self.offset,
            )

        result = self._get_request(
            self.url,
            params=self.clauses,
            app_token=self.app_token,
        )

        if self.verbose:
            print(f"  Downloaded {len(result)} rows")

        return result

    def get_pages(self, page_size: int = 10_000) -> Iterator[list[dict]]:
        row_count = self.n_rows
        n_pages = _int_divide_ceiling(row_count, page_size)

        if self.verbose:
            print(
                f"Downloading dataset {self.domain} {self.id}: "
                f"{row_count} rows in {n_pages} page(s) of at most "
                f"{page_size} rows each..."
            )

        for i in range(n_pages):
            if self.verbose:
                print(f"  Downloading page {i + 1}/{n_pages}")

            start = i * page_size
            end = (i + 1) * page_size - 1
            page = self._get_records(start=start, end=end)

            assert len(page) > 0
            assert len(page) <= page_size

            yield page

    @property
    def n_rows(self) -> int:
        result = self._get_request(
            self.url,
            params=self._build_payload(
                select="count(:id)", where=self.where, limit=1
            ),
            app_token=self.app_token,
        )

        assert len(result) == 1, f"Expected length 1, got {len(result)}"
        assert "count_id" in result[0]
        n_dataset_rows = int(result[0]["count_id"])

        if n_dataset_rows == 0:
            if self.verbose:
                warnings.warn(
                    f"Dataset {self.id} at {self.domain} has no rows. "
                    "This may be due to an bad query."
                )
            return 0

        n_rows_after_offset = n_dataset_rows - self.offset

        if n_rows_after_offset < 0:
            if self.verbose:
                warnings.warn(
                    f"Offset {self.offset} is larger than the number of rows"
                    f" in the dataset ({n_dataset_rows})."
                )
            return 0

        if self.limit is None or self.limit > n_rows_after_offset:
            return n_rows_after_offset
        else:
            return n_rows_after_offset - self.limit

    def _get_records(self, start: int, end: int) -> list[dict]:
        assert end >= start
        assert (
            self.limit is None or end < self.limit
        ), f"End index {end} is larger than limit {self.limit}."
        n_rows = end - start + 1

        return self._get_request(
            self.url,
            params=self._build_payload(
                select=self.select,
                where=self.where,
                offset=self.offset + start,
                limit=n_rows,
            ),
            app_token=self.app_token,
        )

    @classmethod
    def _build_payload(
        cls,
        select: Optional[str | Sequence[str]] = None,
        where: Optional[str] = None,
        limit: Optional[int] = None,
        offset: int = 0,
    ) -> dict:
        clauses = {}

        if select is None:
            pass
        elif isinstance(select, str):
            clauses["$select"] = select
        else:
            clauses["$select"] = ",".join(select)

        if where is not None:
            clauses["$where"] = where

        if limit is not None:
            assert isinstance(limit, int)
            assert limit > 0
            clauses["$limit"] = limit

        assert isinstance(offset, int)
        assert offset >= 0
        clauses["$offset"] = offset

        return clauses

    @property
    def url(self) -> str:
        return urlunparse(
            ("https", self.domain, f"resource/{self.id}.json", "", "", "")
        )

    @classmethod
    def _get_request(
        cls,
        url: str,
        params: Optional[dict] = None,
        app_token: Optional[str] = None,
    ) -> list[dict]:
        headers = {}
        if app_token is not None:
            headers["X-App-Token"] = app_token

        with httpx.Client() as client:
            r = client.get(url, headers=headers, params=params)
            r.raise_for_status()
            return r.json()
