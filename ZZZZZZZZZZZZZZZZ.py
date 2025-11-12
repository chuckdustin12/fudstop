import re
import fitz  # PyMuPDF

# ──────────────────────────────────────────────────────────────────────────────
# 1. Paths
# ──────────────────────────────────────────────────────────────────────────────
INPUT_PDF  = r"C:\Users\chuck\OneDrive\Desktop\ALL CASES\APPELLATE_RECORD_FINAL.pdf"
OUTPUT_PDF = r"C:\Users\chuck\OneDrive\Desktop\THE BRIEF OF THE CENTURY\MOTIONS\CHARLES D MYERS\MANDAMUSES\FINAL DRAGON\MANDAMUS_FINALITY.pdf"


# ──────────────────────────────────────────────────────────────────────────────
# 2. Utility: sanitize_title
#    Removes DKT(...) references, trailing dashes, etc.
# ──────────────────────────────────────────────────────────────────────────────
def sanitize_title(title: str) -> str:
    """
    Examples:
      "04.04.24 DKT(96) 02-24-00149-CV - MANDAMUS - " -> "04.04.24 02-24-00149-CV MANDAMUS"
    """
    # remove "DKT(123)" with optional spaces
    title = re.sub(r'\s*DKT\(\d+\)\s*', ' ', title)
    # remove trailing " - " sequences
    title = re.sub(r'(\s*-\s*)+$', '', title)
    # collapse excessive spaces
    title = ' '.join(title.split())
    return title


# ──────────────────────────────────────────────────────────────────────────────
# 3. Utility: wrap_text
#    Splits text into multiple lines if longer than `maxw` points.
# ──────────────────────────────────────────────────────────────────────────────
def wrap_text(txt, font, size, maxw):
    words = txt.split()
    lines = []
    line  = ""

    for w_ in words:
        test = (line + " " + w_).strip()
        if fitz.get_text_length(test, font, size) <= maxw:
            line = test
        else:
            lines.append(line)
            line = w_
    if line:
        lines.append(line)
    return lines


# ──────────────────────────────────────────────────────────────────────────────
# 4. draw_toc
#    Dynamically creates new pages if the TOC overflows.
#    Instead of inserting final links now (when offset is unknown),
#    we store the link data so we can insert them after we append original PDF.
# ──────────────────────────────────────────────────────────────────────────────
def draw_toc(
    final_pdf,
    bookmarks,
    page_start,
    margin_x,
    margin_y,
    w,
    h,
    line_h,
    font_sz
):
    """
    Renders an improved TOC in final_pdf starting at page_start (0-based).
    Dynamically adds pages if needed.
    
    Returns:
      - last_page_used: the final page index used for the TOC
      - link_data: a list of dicts about where to place actual links + page # later
         each dict = {
           "pdf_page_idx": int,    # which page in final_pdf was used
           "x_page_num": float,   # X coordinate for the page number text
           "y_page_num": float,   # Y coordinate for the page number text
           "orig_page_num": int,  # original PDF page number (1-based)
           "line_text": str,      # for reference (optional)
           "text_font": str,      # e.g. "Times-Bold"
           "font_size": float,    # 
           "dot_coords": [ (x1, y1), (x2, y2), ... ] # the dotted line segments
         }
    """
    page_idx = page_start
    page = final_pdf[page_idx]
    y = margin_y

    link_data = []  # we store info about where to place final links, once we know offset

    def draw_header(_page, y_offset):
        heading_text = "TABLE OF CONTENTS"
        heading_font = "Times-Bold"
        heading_size = 16
        heading_width = fitz.get_text_length(heading_text, heading_font, heading_size)
        x_center = (w - heading_width) / 2.0

        _page.insert_text(
            (x_center, y_offset),
            heading_text,
            fontsize=heading_size,
            fontname=heading_font
        )

        # optional line under the header
        line_rect = fitz.Rect(margin_x, y_offset + line_h, w - margin_x, y_offset + line_h + 1)
        _page.draw_line(line_rect.tl, line_rect.tr, width=1, color=(0, 0, 0))

        return y_offset + 1.5 * line_h

    # Draw initial header
    y = draw_header(page, y)

    # Loop over all bookmarks
    for lvl, raw_title, orig_pn in bookmarks:
        title = sanitize_title(raw_title)

        indent = (lvl - 1) * 20
        if lvl == 1:
            text_font = "Times-Bold"
        else:
            text_font = "Times-Roman"

        max_title_w = (w - 2 * margin_x - 60) - indent
        lines = wrap_text(title, text_font, font_sz, max_title_w)

        for i, line_text in enumerate(lines):
            # If we don't have space, go to next page
            if y + line_h > (h - margin_y):
                page_idx += 1
                # Dynamically add a new page if we exceed current count
                if page_idx >= final_pdf.page_count:
                    final_pdf.new_page(width=w, height=h)
                page = final_pdf[page_idx]
                y = margin_y
                y = draw_header(page, y)

            x_txt = margin_x + indent
            x_pg  = w - margin_x - 40

            # Insert the line text
            page.insert_text(
                (x_txt, y),
                line_text,
                fontsize=font_sz,
                fontname=text_font,
                color=(0, 0, 0)
            )

            # On the first line of each bookmark, create dotted line placeholders
            if i == 0:
                text_width = fitz.get_text_length(line_text, text_font, font_sz)
                dot_start = x_txt + text_width + 5
                dot_end   = x_pg - 5
                dot_y     = y + font_sz * 0.65
                dot_step  = font_sz * 0.3
                dot_coords = []
                if dot_end > dot_start:
                    current_x = dot_start
                    while current_x < dot_end:
                        dot_coords.append((current_x, dot_y))
                        current_x += dot_step

                # We'll store the info we need for a final link
                link_info = {
                    "pdf_page_idx": page_idx,    # which page in final_pdf
                    "x_page_num": x_pg,         # where to place final page number
                    "y_page_num": y,
                    "orig_page_num": orig_pn,   # original PDF page number (1-based)
                    "line_text": line_text,
                    "text_font": text_font,
                    "font_size": font_sz,
                    "dot_coords": dot_coords
                }
                link_data.append(link_info)

            y += line_h

    return page_idx, link_data


# ──────────────────────────────────────────────────────────────────────────────
# 5. build_no_overlap_pdf
#    Main function that:
#      - estimates TOC size
#      - allocates pages (with a small buffer)
#      - draws TOC while storing link placeholders
#      - appends the original PDF
#      - finalizes links with correct offset
# ──────────────────────────────────────────────────────────────────────────────
def build_no_overlap_pdf():
    # 1) Read original PDF and bookmarks
    orig = fitz.open(INPUT_PDF)
    bookmarks = orig.get_toc()  # each is [lvl, title, page_num]

    # 2) Basic dimension from first page
    w, h = orig[0].rect.width, orig[0].rect.height

    # 3) Layout
    MARGIN_X = 50
    MARGIN_Y = 50
    FONT_SZ  = 12
    LINE_H   = FONT_SZ * 1.8

    # 4) Estimate how many pages the TOC might require
    cur_y = MARGIN_Y + 2 * LINE_H
    toc_pages_est = 1
    for lvl, raw_title, _ in bookmarks:
        title = sanitize_title(raw_title)
        indent = (lvl - 1)*20
        max_title_w = (w - 2*MARGIN_X - 60) - indent
        lines = wrap_text(title, "Times-Roman", FONT_SZ, max_title_w)
        for _ in lines:
            if cur_y + LINE_H > (h - MARGIN_Y):
                toc_pages_est += 1
                cur_y = MARGIN_Y
            cur_y += LINE_H

    # add a small buffer
    toc_pages_est += 1

    # 5) Create brand-new final PDF
    final_pdf = fitz.open()

    # Add 3 intro pages
    for _ in range(3):
        final_pdf.new_page(width=w, height=h)

    # Add initial TOC pages
    for _ in range(toc_pages_est):
        final_pdf.new_page(width=w, height=h)

    # 6) Draw TOC, possibly adding more pages if we run out
    #    We'll get back the last page used & a list of link placeholders
    toc_start = 3
    last_page_used_for_toc, link_data = draw_toc(
        final_pdf,
        bookmarks,
        page_start=toc_start,
        margin_x=MARGIN_X,
        margin_y=MARGIN_Y,
        w=w,
        h=h,
        line_h=LINE_H,
        font_sz=FONT_SZ
    )

    # 7) The final page used by the TOC is last_page_used_for_toc
    #    The original doc will start on the next page
    original_doc_first_page = last_page_used_for_toc + 1  # zero-based

    # 8) Append the entire original PDF at the end
    final_pdf.insert_pdf(orig)

    # 9) Now finalize the clickable links
    #    We know how many pages we used for TOC & intros, so offset = original_doc_first_page.
    #    Because PyMuPDF uses zero-based page indexing for the link "page",
    #    if the user sees the original doc on page # => offset + 1 in normal sense,
    #    we do disp_pg = offset + orig_page_num, link page => disp_pg - 1.
    for ld in link_data:
        page_idx   = ld["pdf_page_idx"]
        x_pg       = ld["x_page_num"]
        y_pg       = ld["y_page_num"]
        orig_pn    = ld["orig_page_num"]
        dot_coords = ld["dot_coords"]

        # The final page the user should jump to
        disp_pg = original_doc_first_page + (orig_pn - 1)  # zero-based + (orig_pn-1)

        # Draw the dotted fill line if you want, or if not already drawn
        # We stored them as coords, so let's do it here:
        # (We actually drew the line placeholders earlier, but if you want them
        #  AFTER the offset is known, you can do so. For demonstration, let's do it again.)
        page = final_pdf[page_idx]
        for (dx, dy) in dot_coords:
            page.draw_line((dx, dy), (dx + 1, dy), width=0.5, color=(0, 0, 0))

        # Insert the final page number text (overwriting the placeholder if desired)
        # If we inserted a placeholder earlier, you can simply re-insert on top
        # or you can first block it out. For simplicity, let's just re-insert:
        disp_str = str(disp_pg + 1)  # if you want user to see 1-based page in final doc
        page.insert_text(
            (x_pg, y_pg),
            disp_str,
            fontsize=ld["font_size"],
            fontname=ld["text_font"],
            color=(0, 0, 1)
        )

        # Now create a clickable link rectangle around that text
        text_width = fitz.get_text_length(disp_str, ld["text_font"], ld["font_size"])
        link_rect = fitz.Rect(x_pg - 2, y_pg - 2, x_pg + text_width + 6, y_pg + ld["font_size"] + 2)
        page.insert_link({
            "kind": fitz.LINK_GOTO,
            "page": disp_pg,  # zero-based
            "from": link_rect
        })

    # 10) Update the internal PDF bookmarks as well
    #     i.e. the side panel's outline
    new_toc = []
    offset = original_doc_first_page  # zero-based
    for lvl, raw_title, orig_pn in bookmarks:
        final_pg_1based = offset + (orig_pn - 1) + 1
        # e.g. if offset=10 and orig_pn=1 => final_pg_1based=11
        if 1 <= final_pg_1based <= final_pdf.page_count:
            cleaned_title = sanitize_title(raw_title)
            new_toc.append([lvl, cleaned_title, final_pg_1based])
    final_pdf.set_toc(new_toc)

    # 11) Save
    final_pdf.save(OUTPUT_PDF)
    final_pdf.close()
    print("✅ Done! Links should be clickable now. File saved to:", OUTPUT_PDF)


# ──────────────────────────────────────────────────────────────────────────────
# 6. Entry Point
# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    build_no_overlap_pdf()
