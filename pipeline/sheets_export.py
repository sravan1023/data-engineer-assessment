"""
pipeline.sheets_export — Export Task 4 query results to Google Sheets via the Sheets API.
"""

from __future__ import annotations

import os
from typing import Optional

import pandas as pd

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# Required scope to create/edit Google Sheets
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Display titles for each query worksheet
QUERY_TITLES: dict[str, str] = {
    "4a": "4a – Doc Count by Source",
    "4b": "4b – Monthly Distribution",
    "4c": "4c – Fetch Success Rate",
    "4d": "4d – Top 10 Path Segments",
    "4e": "4e – Stale Document Analysis",
}



def _get_credentials(credentials_path: Optional[str] = None) -> Credentials:
    """Resolve a ``Credentials`` object from a service-account JSON key file."""
    path = (
        credentials_path
        or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        or "service_account.json"
    )
    if not os.path.isfile(path):
        raise FileNotFoundError(
            f"Service-account key not found at '{path}'.  "
            "Set credentials_path, GOOGLE_APPLICATION_CREDENTIALS, "
            "or place service_account.json in the working directory."
        )
    return Credentials.from_service_account_file(path, scopes=SCOPES)


def _df_to_sheet_values(df: pd.DataFrame) -> list[list]:
    """
    Convert a DataFrame to a list-of-lists suitable for the Sheets API
    ``values.update`` call.  The first element is the header row.
    """
    header = list(df.columns)
    rows = df.fillna("").astype(str).values.tolist()
    return [header] + rows


def _build_bold_header_request(sheet_id: int, num_columns: int) -> dict:
    """Return a ``repeatCell`` request that bolds the first row of a sheet."""
    return {
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 0,
                "endRowIndex": 1,
                "startColumnIndex": 0,
                "endColumnIndex": num_columns,
            },
            "cell": {
                "userEnteredFormat": {
                    "textFormat": {"bold": True},
                    "backgroundColor": {
                        "red": 0.9,
                        "green": 0.9,
                        "blue": 0.9,
                    },
                },
            },
            "fields": "userEnteredFormat(textFormat,backgroundColor)",
        }
    }


def _build_autosize_request(sheet_id: int) -> dict:
    """Return an ``autoResizeDimensions`` request for all columns."""
    return {
        "autoResizeDimensions": {
            "dimensions": {
                "sheetId": sheet_id,
                "dimension": "COLUMNS",
            }
        }
    }



def export_to_google_sheets(
    dataframes: dict[str, pd.DataFrame],
    spreadsheet_id: str,
    credentials_path: Optional[str] = None,
    share_with: Optional[str] = None,
) -> str:
    """
    Write each query result to its own worksheet inside an **existing**
    Google Spreadsheet.

    The service account does not have Drive storage quota, so we cannot
    create new spreadsheets.  Instead the caller must provide a
    ``spreadsheet_id`` for a spreadsheet that is already shared with the
    service account as **Editor**.

    Parameters
    ----------
    dataframes : dict[str, DataFrame]
        Mapping of query id (``"4a"`` … ``"4e"``) to its result DataFrame.
    spreadsheet_id : str
        The ID of an existing Google Spreadsheet (from the URL).
    credentials_path : str | None
        Path to Google service-account JSON key file.
    share_with : str | None
        Optional e-mail address to grant editor access to.

    Returns
    -------
    str
        URL of the spreadsheet.
    """
    creds = _get_credentials(credentials_path)
    service = build("sheets", "v4", credentials=creds)
    drive_service = build("drive", "v3", credentials=creds)
    sheets_api = service.spreadsheets()

    sorted_items = sorted(dataframes.items())

    url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit"
    print(f"Using existing spreadsheet: {url}")

    # 1. Fetch existing sheet metadata
    spreadsheet = sheets_api.get(spreadsheetId=spreadsheet_id).execute()
    existing_sheets = {
        s["properties"]["title"]: s["properties"]["sheetId"]
        for s in spreadsheet.get("sheets", [])
    }
    print(f"  Existing worksheets: {list(existing_sheets.keys())}")

    # 2. Add required worksheets (skip if already present)
    add_requests: list[dict] = []
    needed_titles: list[str] = []
    for idx, (qid, _) in enumerate(sorted_items):
        title = QUERY_TITLES.get(qid, qid)
        needed_titles.append(title)
        if title not in existing_sheets:
            add_requests.append({
                "addSheet": {
                    "properties": {"title": title, "index": idx}
                }
            })

    title_to_id = dict(existing_sheets)  # start with existing

    if add_requests:
        resp = sheets_api.batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": add_requests},
        ).execute()
        for reply in resp.get("replies", []):
            props = reply.get("addSheet", {}).get("properties")
            if props:
                title_to_id[props["title"]] = props["sheetId"]

    # 3. Delete worksheets that aren't needed (e.g., default "Sheet1")
    delete_requests: list[dict] = []
    for title, sid in existing_sheets.items():
        if title not in needed_titles:
            delete_requests.append({"deleteSheet": {"sheetId": sid}})
    if delete_requests:
        try:
            sheets_api.batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={"requests": delete_requests},
            ).execute()
        except Exception:
            pass  # may fail if it would leave zero sheets; non-critical

    # 4. Rename the spreadsheet
    sheets_api.batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"requests": [{
            "updateSpreadsheetProperties": {
                "properties": {"title": "Data Eng Assessment"},
                "fields": "title",
            }
        }]},
    ).execute()

    # 5. Write data into each worksheet
    format_requests: list[dict] = []

    for idx, (qid, df) in enumerate(sorted_items):
        title = QUERY_TITLES.get(qid, qid)
        values = _df_to_sheet_values(df)
        range_notation = f"'{title}'!A1"

        # Clear existing content first
        sheets_api.values().clear(
            spreadsheetId=spreadsheet_id,
            range=f"'{title}'",
            body={},
        ).execute()

        sheets_api.values().update(
            spreadsheetId=spreadsheet_id,
            range=range_notation,
            valueInputOption="RAW",
            body={"values": values},
        ).execute()

        print(f"  [{qid}] Wrote {len(df)} data rows to '{title}'")

        # Queue formatting requests
        format_requests.append(_build_bold_header_request(title_to_id[title], len(df.columns)))
        format_requests.append(_build_autosize_request(title_to_id[title]))

    # 6. Apply header formatting in a single batch
    if format_requests:
        sheets_api.batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": format_requests},
        ).execute()

    # 7. Optionally share with a user
    if share_with:
        try:
            drive_service.permissions().create(
                fileId=spreadsheet_id,
                body={
                    "type": "user",
                    "role": "writer",
                    "emailAddress": share_with,
                },
                sendNotificationEmail=False,
            ).execute()
            print(f"  Shared with {share_with}")
        except Exception:
            print(f"  (Sharing skipped – spreadsheet may already be accessible to {share_with})")

    return url
