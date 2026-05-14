"""
MSPCA dog scraper + hypoallergenic-focused email report.
...
# (Rest of the code above remains unchanged)
...

def _row_cells(row: Dict[str, str], include_change_flag: bool = False) -> Dict[str, str]:
    updated = ""
    if include_change_flag and row.get("changed_fields"):
        updated = "yes"
    return {
        "name": _safe_text(row.get("name", "")) or "Unknown name",
        "breed": _safe_text(row.get("breed", "")) or "Unknown breed",
        "location": _safe_text(row.get("location", "")) or "Unknown location",
        "gender": _safe_text(row.get("gender", "")) or "Unknown gender",
        "age": _safe_text(row.get("age", "")) or "Unknown age",
        "score": _safe_text(row.get("hypo_score", "")),
        "confidence": _safe_text(row.get("hypo_confidence", "")),
        "updated": updated,
        "url": _safe_text(row.get("detail_url", "")),
    }

def _render_text_table(rows: List[Dict[str, str]], include_change_flag: bool = False) -> List[str]:
    if not rows:
        return ["(none)"]

    columns = [
        ("Name", "name", 16),
        ("Breed", "breed", 24),
        ("Location", "location", 12),
        ("Gender", "gender", 8),
        ("Age", "age", 10),
        ("Score", "score", 5),
        ("Conf", "confidence", 6),
        ("Upd", "updated", 3),
    ]
    header = " | ".join(title.ljust(width) for title, _, width in columns)
    divider = "-+-".join("-" * width for _, _, width in columns)
    lines = [header, divider]

    for row in rows:
        cells = _row_cells(row, include_change_flag=include_change_flag)
        line = " | ".join(
            _truncate(cells[key], width).ljust(width) for _, key, width in columns
        )
        lines.append(line)
        if cells["url"]:
            lines.append(f"  {cells['url']}")
    return lines

def _render_html_table(rows: List[Dict[str, str]], include_change_flag: bool = False) -> str:
    if not rows:
        return "<p><em>None</em></p>"

    header_cells = "".join(
        f"<th style='text-align:left;padding:8px;border-bottom:1px solid #ddd'>{label}</th>"
        for label in ("Name", "Breed", "Location", "Gender", "Age", "Score", "Confidence", "Updated", "Link")
    )

    body_rows = []
    for row in rows:
        cells = _row_cells(row, include_change_flag=include_change_flag)
        link = ""
        if cells["url"]:
            safe_url = html.escape(cells["url"])
            link = f"<a href='{safe_url}'>View</a>"
        body_rows.append(
            "<tr>"
            f"<td style='padding:8px;border-bottom:1px solid #eee'>{html.escape(cells['name'])}</td>"
            f"<td style='padding:8px;border-bottom:1px solid #eee'>{html.escape(cells['breed'])}</td>"
            f"<td style='padding:8px;border-bottom:1px solid #eee'>{html.escape(cells['location'])}</td>"
            f"<td style='padding:8px;border-bottom:1px solid #eee'>{html.escape(cells['gender'])}</td>"
            f"<td style='padding:8px;border-bottom:1px solid #eee'>{html.escape(cells['age'])}</td>"
            f"<td style='padding:8px;border-bottom:1px solid #eee'>{html.escape(cells['score'])}</td>"
            f"<td style='padding:8px;border-bottom:1px solid #eee'>{html.escape(cells['confidence'])}</td>"
            f"<td style='padding:8px;border-bottom:1px solid #eee'>{html.escape(cells['updated'])}</td>"
            f"<td style='padding:8px;border-bottom:1px solid #eee'>{link}</td>"
            "</tr>"
        )

    return (
        "<table style='border-collapse:collapse;width:100%;max-width:1100px;font-family:Arial,sans-serif;font-size:14px'>"
        f"<thead><tr>{header_cells}</tr></thead>"
        f"<tbody>{''.join(body_rows)}</tbody>"
        "</table>"
    )

# (Rest of the code below remains unchanged)
...
"""