import fitz  # PyMuPDF

def label_by_top_level_parents_only(
    pdf,
    all_bookmarks,
    prefix="REC",
    corner_offset=10,
    font_size=10
):
    """
    1) Extract top-level bookmarks (level == 1) in the EXACT order from get_toc().
    2) For top-level i, label pages from that bookmark's page up to (next_top_level.page - 1).
       Each page => "REC i:0", "REC i:1", ...
    3) Children do NOT increment. We simply ignore them for the page-range splitting.
    """

    # Get only top-level bookmarks in original order
    # Each item => (page_num)
    top_levels = []
    for (lvl, title, pgnum) in all_bookmarks:
        if lvl == 1:
            top_levels.append(pgnum)

    if not top_levels:
        print("No top-level bookmarks found.")
        return

    # Add a sentinel so the last top-level extends to the final page
    # We'll label up through doc.page_count
    doc_page_count = pdf.page_count
    top_levels.append(doc_page_count + 1)  # sentinel

    # Loop over each pair of consecutive top-level bookmarks
    # i => 0-based index in top_levels (excluding sentinel)
    for i in range(len(top_levels) - 1):
        record_num = i + 1  # "REC 1", "REC 2", etc.

        # This top-level’s starting page (1-based)
        start_page_1b = top_levels[i]
        # Next top-level's start page (1-based)
        next_page_1b  = top_levels[i + 1]

        # We label from start_page_1b..(next_page_1b - 1)
        # Convert to zero-based
        start_idx = start_page_1b - 1
        end_idx   = (next_page_1b - 1) - 1  # because we go UP TO next_top_level - 1

        # If this is the last top-level (before sentinel), we might go
        # all the way to doc_page_count-1
        if next_page_1b == (doc_page_count + 1):
            end_idx = doc_page_count - 1

        # Sanity checks
        if start_idx < 0:
            start_idx = 0
        if end_idx >= doc_page_count:
            end_idx = doc_page_count - 1

        # If we have an invalid (empty) range, skip
        if end_idx < start_idx:
            continue

        offset = 0
        for page_idx in range(start_idx, end_idx + 1):
            label_text = f"{prefix} {record_num}:{offset}"

            page = pdf[page_idx]
            w, h = page.rect.width, page.rect.height

            # Four corners (manual alignment for older PyMuPDF)
            corners = [
                (corner_offset, corner_offset,             "left"),   # top-left
                (w - corner_offset, corner_offset,          "right"),  # top-right
                (corner_offset, h - corner_offset * 2,      "left"),   # bottom-left
                (w - corner_offset, h - corner_offset * 2,  "right")   # bottom-right
            ]

            for (x, y, align_type) in corners:
                text_width = fitz.get_text_length(label_text, fontname="helv", fontsize=font_size)
                if align_type == "right":
                    x -= text_width

                page.insert_text(
                    (x, y),
                    label_text,
                    fontsize=font_size,
                    fontname="helv",
                    color=(0, 0, 0)
                )

            offset += 1


# ─── EXAMPLE USAGE ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    INPUT_PDF  = r"C:\Users\chuck\OneDrive\Desktop\THE BRIEF OF THE CENTURY\MOTIONS\CHARLES D MYERS\MANDAMUSES\FINAL DRAGON\APPELLATE_RECORD_WITH_TOC.pdf"
    OUTPUT_PDF = r"C:\Users\chuck\OneDrive\Desktop\THE BRIEF OF THE CENTURY\MOTIONS\CHARLES D MYERS\MANDAMUSES\FINAL DRAGON\APPELLATE_RECORD_FINAL.pdf"

    doc = fitz.open(INPUT_PDF)
    bookmarks = doc.get_toc()  # => [ [level, title, page_num], ...]

    label_by_top_level_parents_only(
        pdf=doc,
        all_bookmarks=bookmarks,
        prefix="REC",
        corner_offset=10,
        font_size=10
    )

    doc.save(OUTPUT_PDF)
    doc.close()
    print("✅ Done: Each top-level chunk labeled from start_page..(next_top_level - 1). Children remain in same REC.")
