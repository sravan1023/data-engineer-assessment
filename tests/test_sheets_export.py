"""
Unit tests — Google Sheets export (pipeline.sheets_export).
All Google API calls are mocked.
"""

import os
from unittest.mock import MagicMock, patch, call

import pandas as pd
import pytest

from pipeline.sheets_export import (
    QUERY_TITLES,
    SCOPES,
    _df_to_sheet_values,
    _build_bold_header_request,
    _build_autosize_request,
    _get_credentials,
    export_to_google_sheets,
)


class TestDfToSheetValues:
    """_df_to_sheet_values — DataFrame → list-of-lists conversion."""

    def test_header_plus_rows(self):
        df = pd.DataFrame({"A": [1, 2], "B": ["x", "y"]})
        values = _df_to_sheet_values(df)
        assert values[0] == ["A", "B"]
        assert len(values) == 3  # header + 2 data rows

    def test_empty_dataframe(self):
        df = pd.DataFrame(columns=["X", "Y"])
        values = _df_to_sheet_values(df)
        assert values == [["X", "Y"]]

    def test_nan_replaced_with_empty_string(self):
        df = pd.DataFrame({"A": [None, 1]})
        values = _df_to_sheet_values(df)
        assert values[1][0] == ""  # None → ""

    def test_all_values_are_strings(self):
        df = pd.DataFrame({"N": [42, 3.14], "B": [True, False]})
        values = _df_to_sheet_values(df)
        for row in values[1:]:
            for cell in row:
                assert isinstance(cell, str)


class TestBoldHeaderRequest:
    """_build_bold_header_request — formatting payload."""

    def test_structure(self):
        req = _build_bold_header_request(sheet_id=0, num_columns=3)
        assert "repeatCell" in req
        rng = req["repeatCell"]["range"]
        assert rng["sheetId"] == 0
        assert rng["endColumnIndex"] == 3
        assert rng["endRowIndex"] == 1

    def test_bold_flag_set(self):
        req = _build_bold_header_request(sheet_id=1, num_columns=5)
        fmt = req["repeatCell"]["cell"]["userEnteredFormat"]
        assert fmt["textFormat"]["bold"] is True


class TestAutosizeRequest:
    """_build_autosize_request — column autosize payload."""

    def test_dimension_is_columns(self):
        req = _build_autosize_request(sheet_id=2)
        dim = req["autoResizeDimensions"]["dimensions"]
        assert dim["sheetId"] == 2
        assert dim["dimension"] == "COLUMNS"


class TestGetCredentials:
    """_get_credentials — file lookup order."""

    def test_missing_file_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="Service-account key not found"):
            _get_credentials(str(tmp_path / "nonexistent.json"))

    @patch.dict(os.environ, {"GOOGLE_APPLICATION_CREDENTIALS": "/tmp/fake.json"})
    def test_env_var_used_when_no_arg(self):
        """Falls through to env var when credentials_path is None."""
        with pytest.raises(FileNotFoundError, match="fake.json"):
            _get_credentials(None)


class TestQueryTitles:
    def test_all_five_present(self):
        assert set(QUERY_TITLES.keys()) == {"4a", "4b", "4c", "4d", "4e"}

    def test_titles_are_strings(self):
        for v in QUERY_TITLES.values():
            assert isinstance(v, str) and len(v) > 0


class TestExportToGoogleSheets:
    """export_to_google_sheets — end-to-end with mocked Sheets/Drive APIs."""

    def _sample_dfs(self) -> dict[str, pd.DataFrame]:
        return {
            "4a": pd.DataFrame({"SOURCE": ["s1", "s2"], "COUNT": [10, 20]}),
            "4b": pd.DataFrame({"MONTH": ["2025-01"], "DOCS": [5]}),
            "4c": pd.DataFrame({"SRC": ["s1"], "RATE": [99.5]}),
            "4d": pd.DataFrame({"SEGMENT": ["en"], "FREQ": [100]}),
            "4e": pd.DataFrame({"TOTAL": [200], "STALE": [15]}),
        }

    def _setup_mocks(self, mock_build):
        """Create and wire up Sheets + Drive mock services."""
        mock_sheets = MagicMock()
        mock_drive = MagicMock()

        def _pick_service(api, version, credentials):
            return mock_sheets if api == "sheets" else mock_drive
        mock_build.side_effect = _pick_service

        sheets_api = mock_sheets.spreadsheets.return_value

        # Simulate existing spreadsheet with one default "Sheet1"
        sheets_api.get.return_value.execute.return_value = {
            "sheets": [{"properties": {"title": "Sheet1", "sheetId": 0}}]
        }
        # batchUpdate for addSheet returns sheetId per new sheet
        add_replies = [
            {"addSheet": {"properties": {"title": QUERY_TITLES[qid], "sheetId": idx + 1}}}
            for idx, qid in enumerate(sorted(QUERY_TITLES))
        ]
        sheets_api.batchUpdate.return_value.execute.return_value = {
            "replies": add_replies,
        }
        sheets_api.values.return_value.update.return_value.execute.return_value = {}
        sheets_api.values.return_value.clear.return_value.execute.return_value = {}

        return mock_sheets, mock_drive, sheets_api

    @patch("pipeline.sheets_export._get_credentials")
    @patch("pipeline.sheets_export.build")
    def test_writes_to_existing_spreadsheet(self, mock_build, mock_creds):
        mock_sheets, mock_drive, sheets_api = self._setup_mocks(mock_build)

        dfs = self._sample_dfs()
        url = export_to_google_sheets(dfs, spreadsheet_id="abc123")

        assert "abc123" in url
        # Should NOT call create — we write to an existing spreadsheet
        sheets_api.create.assert_not_called()
        # Should fetch existing metadata
        sheets_api.get.assert_called_once()
        # One values().update() per query
        assert sheets_api.values.return_value.update.call_count == 5

    @patch("pipeline.sheets_export._get_credentials")
    @patch("pipeline.sheets_export.build")
    def test_adds_worksheets_and_deletes_default(self, mock_build, mock_creds):
        mock_sheets, mock_drive, sheets_api = self._setup_mocks(mock_build)

        export_to_google_sheets(self._sample_dfs(), spreadsheet_id="abc123")

        # First batchUpdate should include addSheet requests for 5 queries
        first_batch_call = sheets_api.batchUpdate.call_args_list[0]
        requests = first_batch_call[1]["body"]["requests"]
        add_sheet_requests = [r for r in requests if "addSheet" in r]
        assert len(add_sheet_requests) == 5

    @patch("pipeline.sheets_export._get_credentials")
    @patch("pipeline.sheets_export.build")
    def test_share_with_creates_permission(self, mock_build, mock_creds):
        mock_sheets, mock_drive, sheets_api = self._setup_mocks(mock_build)

        export_to_google_sheets(
            self._sample_dfs(),
            spreadsheet_id="xyz",
            share_with="alice@example.com",
        )

        mock_drive.permissions.return_value.create.assert_called_once()
        perm_body = mock_drive.permissions.return_value.create.call_args[1]["body"]
        assert perm_body["emailAddress"] == "alice@example.com"
        assert perm_body["role"] == "writer"

    @patch("pipeline.sheets_export._get_credentials")
    @patch("pipeline.sheets_export.build")
    def test_no_share_skips_drive_permission(self, mock_build, mock_creds):
        mock_sheets, mock_drive, sheets_api = self._setup_mocks(mock_build)

        export_to_google_sheets(self._sample_dfs(), spreadsheet_id="id1")

        mock_drive.permissions.return_value.create.assert_not_called()

    @patch("pipeline.sheets_export._get_credentials")
    @patch("pipeline.sheets_export.build")
    def test_clears_cells_before_writing(self, mock_build, mock_creds):
        mock_sheets, mock_drive, sheets_api = self._setup_mocks(mock_build)

        export_to_google_sheets(self._sample_dfs(), spreadsheet_id="id2")

        # One clear() per worksheet
        assert sheets_api.values.return_value.clear.call_count == 5

    @patch("pipeline.sheets_export._get_credentials")
    @patch("pipeline.sheets_export.build")
    def test_formatting_batch_has_bold_and_autosize(self, mock_build, mock_creds):
        mock_sheets, mock_drive, sheets_api = self._setup_mocks(mock_build)

        export_to_google_sheets(self._sample_dfs(), spreadsheet_id="id3")

        # Find the formatting batchUpdate call (has repeatCell requests)
        for c in sheets_api.batchUpdate.call_args_list:
            reqs = c[1]["body"]["requests"]
            repeat_cells = [r for r in reqs if "repeatCell" in r]
            if repeat_cells:
                # 5 bold headers + 5 autosize = 10 formatting requests
                assert len(reqs) == 10
                return
        pytest.fail("No formatting batchUpdate call found")
