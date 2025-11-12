import fitz  # PyMuPDF

def label_pages_between_all_bookmarks(
    pdf,
    bookmarks,
    prefix="REC",
    corner_offset=10,
    font_size=10
):
    """
    Treats EVERY bookmark—regardless of level—as its own chunk boundary.
    1. Takes the bookmarks in the exact order they appear in doc.get_toc().
    2. For bookmark i, label pages from its page up to (next bookmark’s page - 1).
    3. The last bookmark extends to the end of the PDF.
    4. Each page in that range increments: REC i:0, i:1, etc.

    If two bookmarks share the same or a backward page reference, we "bump" them
    so the ranges don’t collapse or go negative.
    """

    # 1) Extract ALL bookmarks in original order => store (title, page_num)
    #    ignoring the "level" so that every bookmark is a new boundary.
    raw_list = [(bm[1], bm[2]) for bm in bookmarks]  # (title, page_num)

    if not raw_list:
        print("No bookmarks found.")
        return

    # 2) Force strictly ascending page numbers so each subsequent bookmark is
    #    at least 1 page beyond the previous. This avoids empty/negative ranges
    #    if the PDF has out-of-order or duplicate references.
    page_count = pdf.page_count
    fixed = []
    last_p = 0
    for (title, pg) in raw_list:
        if pg <= last_p:
            pg = last_p + 1
        if pg > page_count:
            pg = page_count  # clamp to last page
        fixed.append(pg)
        last_p = pg

    # Remove duplicates if forced bump collides again
    unique_pages = []
    prev = 0
    for pg in fixed:
        if pg > prev:
            unique_pages.append(pg)
            prev = pg

    # Append sentinel "page_count+1" so last chunk extends to the final page
    unique_pages.append(page_count + 1)

    # 3) Label the ranges
    for i in range(len(unique_pages) - 1):
        record_i     = i + 1
        start_1b     = unique_pages[i]
        next_1b      = unique_pages[i+1]
        start_idx    = start_1b - 1
        end_idx      = (next_1b - 1) - 1

        # If next_1b is the sentinel, label up to doc end
        if next_1b == page_count + 1:
            end_idx = page_count - 1

        if end_idx < start_idx:
            # skip empty or negative ranges
            continue

        offset = 0
        for page_idx in range(start_idx, end_idx + 1):
            label_text = f"{prefix} {record_i}:{offset}"
            offset += 1

            page = pdf[page_idx]
            w, h = page.rect.width, page.rect.height

            corners = [
                (corner_offset, corner_offset,             "left"),   # top-left
                (w - corner_offset, corner_offset,          "right"),  # top-right
                (corner_offset, h - corner_offset*2,        "left"),   # bottom-left
                (w - corner_offset, h - corner_offset*2,    "right")   # bottom-right
            ]

            # Manual right-alignment
            for (x, y, align_type) in corners:
                txt_width = fitz.get_text_length(label_text, fontname="helv", fontsize=font_size)
                if align_type == "right":
                    x -= txt_width
                page.insert_text((x, y),
                                 label_text,
                                 fontsize=font_size,
                                 fontname="helv",
                                 color=(0, 0, 0))


# ─── EXAMPLE USAGE ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    INPUT_PDF  = r"C:\Users\chuck\OneDrive\Desktop\THE BRIEF OF THE CENTURY\MOTIONS\CHARLES D MYERS\MANDAMUSES\FINAL DRAGON\APPELLATE_RECORD_FINAL.pdf"
    OUTPUT_PDF = r"C:\Users\chuck\OneDrive\Desktop\THE BRIEF OF THE CENTURY\MOTIONS\CHARLES D MYERS\MANDAMUSES\FINAL DRAGON\APPELLATE_RECORD_FINAL_FINAL.pdf"

    doc = fitz.open(INPUT_PDF)
    bm  = doc.get_toc()  # => [ [level, title, page_num], ... ]

    # Now: every bookmark in order => new chunk
    label_pages_between_all_bookmarks(
        pdf=doc,
        bookmarks=bm,
        prefix="REC",
        corner_offset=10,
        font_size=16
    )

    doc.save(OUTPUT_PDF)
    doc.close()
    print("✅ Done — each bookmark is a new REC block in ascending page order!")
