import fitz  # PyMuPDF

def export_bookmark_page_ranges(doc, output_file):
    """
    Writes a text file listing the page range for EACH bookmark in doc.get_toc().

    For bookmark i at page_num:
      - The range extends from 'page_num' to 'next_bookmark_page - 1'
      - If last bookmark, use up to doc.page_count
      - Skips any zero-length or negative ranges
    """
    toc = doc.get_toc()  # => [ [level, title, page_num], ... ]

    # If no bookmarks, just write a message
    if not toc:
        with open(output_file, "w", encoding="utf-8") as f:
            f.write("No bookmarks found.\n")
        return

    with open(output_file, "w", encoding="utf-8") as f:
        total_bookmarks = len(toc)
        for i in range(total_bookmarks):
            lvl, title, page_num = toc[i]
            # Next bookmark's page
            if i < total_bookmarks - 1:
                _, _, next_page_num = toc[i + 1]
            else:
                # Last bookmark => up to doc.page_count
                next_page_num = doc.page_count + 1

            # current range: [page_num .. next_page_num - 1]
            start_pg = page_num
            end_pg   = next_page_num - 1

            # Validate range
            if end_pg >= start_pg:
                # Write out something like:
                # "Bookmark #1 (Level=1, "Cover Page"): Pages 1-3"
                f.write(
                    f"Bookmark #{i+1} (Level={lvl}, \"{title}\"): "
                    f"Pages {start_pg}-{end_pg}\n"
                )
            else:
                # If it's a zero-length or negative range, skip or log
                f.write(
                    f"Bookmark #{i+1} (Level={lvl}, \"{title}\"): "
                    f"NO VALID RANGE (next bookmark starts on an earlier/same page)\n"
                )

# ─── Example Usage ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    import os

    INPUT_PDF   = r"C:\Users\chuck\OneDrive\Desktop\THE BRIEF OF THE CENTURY\MOTIONS\CHARLES D MYERS\MANDAMUSES\FINAL DRAGON\APPELLATE_RECORD_FINAL.pdf"
    OUTPUT_TXT  = r"C:\Users\chuck\OneDrive\Desktop\THE BRIEF OF THE CENTURY\MOTIONS\CHARLES D MYERS\MANDAMUSES\FINAL DRAGON\APPELLATE_RECORD_WITH_TOC_UPDATED_RANGES.txt"

    doc = fitz.open(INPUT_PDF)
    export_bookmark_page_ranges(doc, OUTPUT_TXT)
    doc.close()

    print(f"✅ Done. Wrote page ranges for each bookmark to {OUTPUT_TXT}")
