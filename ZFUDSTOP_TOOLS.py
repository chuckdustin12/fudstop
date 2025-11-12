# super_pdf_research.py
"""
ENHANCED SUPER MODE - Extended solution with:
 - Fallback to Bing if Google fails or yields insufficient results.
 - Retrying logic when fetching HTML.
 - Summarization fallback (GPT -> HF -> fallback).
 - Optional synonyms-based re-query if results are insufficient.
 - More robust error handling and user notifications.
 - PDF generation with table of contents, references, clickable links.
 - GPT-based relevance check to filter out irrelevant pages.
 - Final fallback: Generate PDF anyway, even with fewer sources.
 - Prints local file path link to PDF.

Requires:
 - pip install fpdf2 googlesearch-python openai transformers torch requests
 - Place DejaVuSans.ttf in the same directory (or adapt path).
 - For Bing fallback, either adapt to official Bing Search API with a key,
   or use some simple approach (shown for demonstration only).
 
You will want to set environment variables for OPENAI_KEY, etc.
"""

import os
import random
import requests
import time
from bs4 import BeautifulSoup

# For Google search
from googlesearch import search

# For using GPT (OpenAI)
from openai import OpenAI

import torch
from transformers import pipeline

from fpdf import FPDF
from fpdf.enums import XPos, YPos


###############################################################################
# FILE: super_pdf_research.py
# PURPOSE: Conduct a multi-step research with fallback searches and summarizations,
#          then output a PDF with a clickable TOC, references, etc.
###############################################################################


###############################################################################
# 1. GLOBAL CONFIG + OPENAI CLIENT
###############################################################################
MIN_REQUIRED_SOURCES = 3       # How many sources we want at minimum
MAX_GOOGLE_RESULTS   = 20
BING_FALLBACK        = True    # Toggle to allow fallback to Bing search
SYNONYM_REQUERY      = True    # Toggle to attempt synonyms-based re-query if needed

client = OpenAI(api_key=os.getenv('OPENAI_KEY'))


###############################################################################
# 2. RANDOM USER-AGENTS + SEARCH
###############################################################################
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 "
    "Mobile/15E148 Safari/604.1"
]

def google_search(query: str, num_results=20):
    """
    FILE: super_pdf_research.py
    FUNCTION: google_search
    PURPOSE:
      Query Google for up to `num_results` results using the 'googlesearch' package.
    RETURNS:
      A list of URLs.
    BEST USE:
      - Provide a clear, concise query.
      - Fallback to Bing or synonyms approach if not enough results.
    """
    try:
        return list(search(query, num_results=num_results))
    except Exception as e:
        print(f"[ERROR Searching Google] {e}")
        return []


def bing_fallback_search(query: str, num_results=20):
    """
    FILE: super_pdf_research.py
    FUNCTION: bing_fallback_search
    PURPOSE:
      Very basic fallback to try to replicate a Bing search with a public endpoint.
      This is not the official Bing Search API. For official usage, you'd do:
        - Sign up for Bing Search API
        - Use their endpoint with your API key
      This function is purely illustrative and may not be reliable long-term.
    RETURNS:
      A list of URLs from a simple parse (if possible).
    BEST USE:
      - Use official endpoints for production settings.
    """
    print("[INFO] Attempting Bing fallback search...")
    fallback_urls = []
    try:
        # A free public endpoint might not exist, but let's pretend:
        # This code just demonstrates the structure.
        pass
    except Exception as e:
        print(f"[ERROR Searching Bing] {e}")
    return fallback_urls


###############################################################################
# 3. FETCH & CLEAN HTML
###############################################################################
def fetch_html_content(url: str, max_len=15000, max_retries=2, sleep_time=2) -> str:
    """
    FILE: super_pdf_research.py
    FUNCTION: fetch_html_content
    PURPOSE:
      - GET the webpage with random user-agent
      - Clean out scripts & styles
      - Return text up to `max_len` chars
      - Retries up to `max_retries` times if there's a connection or timeout error
    RETURNS:
      The cleaned text from the requested URL or "" on failure.
    BEST USE:
      - For summarization or relevance-check.
      - If text is huge, it will be truncated.
    """
    attempts = 0
    while attempts < max_retries:
        attempts += 1
        try:
            headers = {"User-Agent": random.choice(USER_AGENTS)}
            resp = requests.get(url, timeout=10, headers=headers)
            if resp.status_code != 200:
                print(f"[WARN] {url} => Status {resp.status_code}")
                continue
            soup = BeautifulSoup(resp.text, "html.parser")
            for tag in soup(["script", "style"]):
                tag.decompose()
            text = soup.get_text(separator=" ")
            text = " ".join(text.split())
            if len(text) > max_len:
                text = text[:max_len]
            return text
        except Exception as e:
            print(f"[ERROR fetch_html_content] Attempt {attempts} => {url} => {e}")
            time.sleep(sleep_time)
    return ""


###############################################################################
# 4. HUGGINGFACE SUMMARIZER (DEVICE = CUDA:0 IF AVAILABLE)
###############################################################################
device_for_summarizer = 0 if torch.cuda.is_available() else -1
device_name = f"cuda:{device_for_summarizer}" if device_for_summarizer == 0 else "CPU"
print(f"[INFO] HF Summarizer device => {device_name}")

hf_summarizer = pipeline(
    "summarization",
    model="sshleifer/distilbart-cnn-12-6",
    device=device_for_summarizer
)

def hf_summarize_text(text: str, max_length=150) -> str:
    """
    FILE: super_pdf_research.py
    FUNCTION: hf_summarize_text
    PURPOSE:
      Summarize text using HF summarizer on GPU if available.
      Potentially chunk if extremely large (not shown here).
    RETURNS:
      A summarized version of the input text.
    BEST USE:
      - As a fallback when GPT-based summarization fails.
    """
    if not text:
        return ""
    try:
        result = hf_summarizer(
            text,
            max_length=max_length,
            min_length=30,
            do_sample=False
        )
        return result[0]["summary_text"].strip()
    except Exception as e:
        print(f"[ERROR HF Summarize] => {e}")
        return text[:500] + "..."


###############################################################################
# 5. GPT SUMMARIZATION (WITH CHUNKING + FALLBACK)
###############################################################################
def chunk_text(text: str, chunk_size=3000) -> list[str]:
    """
    FILE: super_pdf_research.py
    FUNCTION: chunk_text
    PURPOSE:
      Splits a large text into chunks for GPT or other summarization calls.
    RETURNS:
      A list of text chunks.
    BEST USE:
      - Handling very large text bodies that exceed typical GPT token limits.
    """
    return [text[i:i+chunk_size] for i in range(0, len(text), chunk_size)]


def gpt_summarize_big_text(text: str, model="gpt-4.1") -> str:
    """
    FILE: super_pdf_research.py
    FUNCTION: gpt_summarize_big_text
    PURPOSE:
      Summarize a large text by chunking and calling GPT. 
      If GPT fails for a chunk, fallback to HF summarizer or partial chunk.
    RETURNS:
      A combined summary of the entire text.
    BEST USE:
      - For large articles or multiple paragraphs.
    """
    if not text.strip():
        return ""

    chunks = chunk_text(text, chunk_size=3000)
    partials = []
    for cnum, chunk in enumerate(chunks, start=1):
        messages = [
            {
                "role": "system",
                "content": "You are a brilliant summarizer. Summarize the following text chunk concisely."
            },
            {
                "role": "user",
                "content": chunk
            }
        ]
        try:
            resp = client.chat.completions.create(
                model=model,
                messages=messages,
                temperature=0.7,
                max_completion_tokens=800,
                top_p=1.0,
                store=False
            )
            part = resp.choices[0].message.content.strip()
            partials.append(part)
        except Exception as e:
            print(f"[ERROR GPT Summarizing chunk {cnum}] => {e}")
            # fallback to HF or partial text
            try:
                partials.append(hf_summarize_text(chunk))
            except Exception as e2:
                print(f"[ERROR HF fallback chunk {cnum}] => {e2}")
                partials.append(chunk[:500] + "...")

    # unify partial summaries
    combined = "\n".join(partials)
    return unify_summaries(combined, model=model)


def unify_summaries(text: str, model="gpt-4.1") -> str:
    """
    FILE: super_pdf_research.py
    FUNCTION: unify_summaries
    PURPOSE:
      Merges partial summaries into one coherent final summary 
      by calling GPT again. Fallback to HF on error.
    RETURNS:
      A final combined summary.
    BEST USE:
      - After chunk-based summarization, to unify partial results.
    """
    if not text.strip():
        return ""
    try:
        messages = [
            {
                "role": "system",
                "content": "Combine these partial summaries into one coherent final summary."
            },
            {
                "role": "user",
                "content": text
            }
        ]
        resp = client.chat.completions.create(
            model=model,
            messages=messages,
            temperature=0.7,
            max_completion_tokens=800,
            top_p=1.0,
            store=False
        )
        return resp.choices[0].message.content.strip()
    except Exception as e:
        print(f"[ERROR unify_summaries (GPT)] => {e}")
        # fallback to HF summarizer for combination
        return hf_summarize_text(text, max_length=200)


###############################################################################
# 6. GPT-BASED RELEVANCE CHECK
###############################################################################
def gpt_check_relevance(text: str, query: str, model="gpt-4.1", relevance_threshold=0.5) -> bool:
    """
    FILE: super_pdf_research.py
    FUNCTION: gpt_check_relevance
    PURPOSE:
      Use GPT to decide if the text is relevant to the original query.
      You can customize the prompt to get a numeric score or a yes/no. 
    PARAMS:
      text: The text extracted from a page.
      query: The user's research query.
      model: GPT model name (e.g., "gpt-4.1").
      relevance_threshold: If we produce a 0-1 "relevance" scale, we consider
                          relevant if >= threshold.
    RETURNS:
      True if the page is likely relevant, False otherwise.
    BEST USE:
      - To filter out random or off-topic pages from the search results.
    """
    if not text.strip():
        return False

    # Truncate text for the prompt if it's too large
    truncated_text = text[:2000]
    # Here we ask GPT to produce a single "relevance score" from 0.0 to 1.0
    messages = [
        {
            "role": "system",
            "content": (
                "You are a classifier. Given a search query and some webpage text, "
                "rate how relevant the text is to the query on a scale from 0.0 to 1.0. "
                "Then only output the numeric score, no additional explanation."
            )
        },
        {
            "role": "user",
            "content": f"Query: {query}\n\nWebpage text snippet:\n\n{truncated_text}"
        }
    ]

    try:
        resp = client.chat.completions.create(
            model=model,
            messages=messages,
            temperature=0.0,
            max_completion_tokens=10,
            top_p=1.0,
            store=False
        )
        # We attempt to parse out a float from the response
        relevance_str = resp.choices[0].message.content.strip()
        # Basic parse to float
        try:
            relevance_val = float(relevance_str)
        except:
            # If GPT didn't return a bare number, do a naive parse
            relevance_val = 0.0
        return relevance_val >= relevance_threshold
    except Exception as e:
        print(f"[ERROR gpt_check_relevance] => {e}")
        # If there's an error, assume it's relevant or revert to a safer default
        return False


###############################################################################
# 7. PDF GENERATION (WITH TABLE OF CONTENTS ON PAGE 2 + LINKABLE SECTIONS)
###############################################################################
class PDFReport(FPDF):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.links_for_toc = []
        self.sources_page_links = {}
        self.set_auto_page_break(auto=True, margin=15)

    def header(self):
        self.set_fill_color(30, 144, 255)  # a nice blue bar
        self.rect(0, 0, 9999, 15, 'F')
        self.set_y(5)
        self.set_font("DejaVu", "", 10)
        self.set_text_color(255, 255, 255)
        self.cell(0, 5, "FUDSTOP Research", 0, 1, align="C")
        self.set_text_color(0, 0, 0)
        self.ln(2)

    def footer(self):
        self.set_y(-15)
        self.set_font("DejaVu", "", 10)
        self.set_text_color(128, 128, 128)
        page_str = f"Page {self.page_no()}/{{nb}}"
        self.cell(0, 10, page_str, 0, 0, "C")
        self.set_text_color(0, 0, 0)


def setup_unicode_font(pdf: PDFReport):
    """
    FILE: super_pdf_research.py
    FUNCTION: setup_unicode_font
    PURPOSE:
      Add the DejaVuSans.ttf (or other Unicode-friendly font) for special chars.
    """
    ttf_path = os.path.join(os.path.dirname(__file__), "DejaVuSans.ttf")
    if not os.path.exists(ttf_path):
        raise FileNotFoundError(f"DejaVuSans.ttf not found at: {ttf_path}")
    pdf.add_font("DejaVu", "", ttf_path, uni=True)


def break_long_words(text: str, max_word_len=50) -> str:
    """
    FILE: super_pdf_research.py
    FUNCTION: break_long_words
    PURPOSE:
      Insert hyphens for extremely long words that break the multi_cell layout.
    """
    tokens = text.split()
    for i, token in enumerate(tokens):
        if len(token) > max_word_len:
            chunks = [token[j:j+max_word_len] for j in range(0, len(token), max_word_len)]
            tokens[i] = "- ".join(chunks)
    return ' '.join(tokens)


def safe_multicell(pdf: PDFReport, text: str, w=0, h=8, ln_spacing=3):
    """
    FILE: super_pdf_research.py
    FUNCTION: safe_multicell
    PURPOSE:
      Safely prints multi-line text, inserting line breaks for long words.
    """
    safe_text = break_long_words(text)
    pdf.multi_cell(w, h, safe_text, new_x=XPos.START, new_y=YPos.NEXT)
    pdf.ln(ln_spacing)


def create_pdf_report(
    topic: str,
    source_summaries: list[tuple[str, str]],
    final_summary: str = "",
    filename="research_report.pdf"
):
    """
    FILE: super_pdf_research.py
    FUNCTION: create_pdf_report
    PURPOSE:
      Builds a PDF with a cover page, table of contents, source pages,
      combined final summary, and references.
    PARAMS:
      topic: The user query or research topic.
      source_summaries: A list of (url, summary) pairs.
      final_summary: The unified summary at the end.
      filename: Output PDF filename.
    BEST USE:
      - Called after collecting all relevant sources and summaries.
    """
    pdf = PDFReport()
    setup_unicode_font(pdf)
    pdf.alias_nb_pages()

    # ===== Cover Page =====
    pdf.add_page()
    pdf.set_font("DejaVu", "", 18)
    pdf.set_text_color(30, 144, 255)
    safe_multicell(pdf, "FUDSTOP Research Report", ln_spacing=5)

    pdf.set_font("DejaVu", "", 14)
    pdf.set_text_color(0, 0, 0)
    safe_multicell(pdf, f"Topic: {topic}", ln_spacing=8)

    pdf.set_font("DejaVu", "", 12)
    pdf.set_text_color(80, 80, 80)
    safe_multicell(pdf, "Automatically generated, featuring a Table of Contents and linkable sections!", ln_spacing=5)
    pdf.ln(70)
    pdf.set_text_color(0, 0, 0)
    pdf.set_font("DejaVu", "", 10)
    safe_multicell(pdf, "Cover page done. Next: Table of Contents...", ln_spacing=2)

    # ===== Table of Contents =====
    pdf.add_page()
    pdf.set_font("DejaVu", "", 16)
    pdf.set_text_color(0, 128, 128)
    safe_multicell(pdf, "Table of Contents", ln_spacing=5)

    pdf.set_font("DejaVu", "", 12)
    pdf.set_text_color(0, 0, 0)

    source_links = []
    for i, (url, _) in enumerate(source_summaries, start=1):
        link_id = pdf.add_link()
        source_links.append(link_id)
        label = f"Source {i}"
        line_text = break_long_words(f"{label} : {url}")
        pdf.cell(w=0, h=8, txt=line_text, ln=1, link=link_id)

    final_summary_link = None
    if final_summary.strip():
        final_summary_link = pdf.add_link()
        pdf.cell(w=0, h=8, txt="Combined Final Summary", ln=1, link=final_summary_link)

    refs_link = pdf.add_link()
    pdf.cell(w=0, h=8, txt="References", ln=1, link=refs_link)

    # ===== Source Summaries =====
    for i, (url, summary) in enumerate(source_summaries, start=1):
        pdf.add_page()
        pdf.set_link(source_links[i-1])
        pdf.set_font("DejaVu", "", 14)
        pdf.set_text_color(30, 144, 255)
        safe_multicell(pdf, f"Source {i}", ln_spacing=2)

        pdf.set_font("DejaVu", "", 11)
        pdf.set_text_color(0, 0, 0)
        safe_multicell(pdf, url, ln_spacing=4)
        safe_multicell(pdf, summary, ln_spacing=2)

    # ===== Combined Final Summary =====
    if final_summary.strip():
        pdf.add_page()
        if final_summary_link:
            pdf.set_link(final_summary_link)
        pdf.set_font("DejaVu", "", 14)
        pdf.set_text_color(30, 144, 255)
        safe_multicell(pdf, "Combined Final Summary", ln_spacing=5)
        pdf.set_text_color(0, 0, 0)
        pdf.set_font("DejaVu", "", 11)
        safe_multicell(pdf, final_summary, ln_spacing=2)

    # ===== References =====
    pdf.add_page()
    pdf.set_link(refs_link)
    pdf.set_font("DejaVu", "", 14)
    pdf.set_text_color(30, 144, 255)
    safe_multicell(pdf, "References", ln_spacing=5)
    pdf.set_font("DejaVu", "", 11)
    pdf.set_text_color(0, 0, 0)

    for url, _ in source_summaries:
        safe_multicell(pdf, url, ln_spacing=2)

    # Output
    pdf.output(filename)
    pdf_local_path = os.path.abspath(filename)
    print(f"[INFO] PDF saved as {filename}")
    print(f"[INFO] Local Path: file://{pdf_local_path}")


###############################################################################
# 8. OPTIONAL: GPT-BASED SYNONYM / VARIATION RE-QUERY
###############################################################################
def generate_synonym_queries(query: str, model="gpt-4.1", num_variations=2) -> list[str]:
    """
    FILE: super_pdf_research.py
    FUNCTION: generate_synonym_queries
    PURPOSE:
      Use GPT to produce synonyms or variation queries to re-try a search
      if we can't find enough relevant results from the initial query.
    RETURNS:
      A list of strings representing alternative queries.
    BEST USE:
      - Increase the chance of finding relevant sources when results are minimal.
    """
    try:
        prompt = f"""
        Create {num_variations} alternative queries or synonyms-based variations for the following search:
        '{query}'
        Ensure they remain relevant to the same context.
        """
        messages = [
            {"role": "system", "content": "You generate alternative search queries."},
            {"role": "user", "content": prompt}
        ]
        resp = client.chat.completions.create(
            model=model,
            messages=messages,
            temperature=0.7,
            max_completion_tokens=300,
            top_p=1.0,
            store=False
        )
        answer = resp.choices[0].message.content.strip()
        # Heuristic: parse them line-by-line
        lines = answer.split('\n')
        variations = [l.strip('-• ') for l in lines if l.strip()]
        return variations
    except Exception as e:
        print(f"[ERROR generate_synonym_queries] => {e}")
        return []


# ###############################################################################
# # 9. MAIN PIPELINE
# ###############################################################################
# def run_pdf_research(query: str, model="gpt-4.1"):
#     """
#     FILE: super_pdf_research.py
#     FUNCTION: run_pdf_research
#     PURPOSE:
#       1) Search for sources via Google. If insufficient, try Bing or synonyms re-query.
#       2) Fetch each & summarize if relevant (GPT w/ chunking -> fallback HF).
#       3) Combine partial summaries into a final summary.
#       4) Generate a PDF with a clickable TOC, references, etc.
#       5) Always produce a PDF, even if fewer than MIN_REQUIRED_SOURCES are found.
#     PARAMS:
#       query: The user input or topic to research.
#       model: GPT model name (like "gpt-4" or "gpt-4.1").
#     RETURNS:
#       None. Prints progress, saves a PDF, and outputs a local path link.
#     BEST USE:
#       - High-level function to orchestrate entire research flow.
#     """
#     cleaned_query = query.strip()
#     if not cleaned_query:
#         print("[ERROR] No query provided.")
#         return

#     print(f"[INFO] Running PDF research for query: '{cleaned_query}'")

#     # === STEP 1: Google Search ===
#     results = google_search(cleaned_query, num_results=MAX_GOOGLE_RESULTS)
#     if len(results) < MIN_REQUIRED_SOURCES:
#         print(f"[WARN] Google results less than {MIN_REQUIRED_SOURCES} for: {cleaned_query}")

#         # === Bing fallback ===
#         if BING_FALLBACK:
#             bing_results = bing_fallback_search(cleaned_query, num_results=MAX_GOOGLE_RESULTS)
#             # Combine them, preserving order but removing duplicates
#             combined_results = list(dict.fromkeys(results + bing_results))
#             results = combined_results

#             # === Attempt synonyms re-query if still insufficient ===
#             if len(results) < MIN_REQUIRED_SOURCES and SYNONYM_REQUERY:
#                 variations = generate_synonym_queries(cleaned_query, model=model, num_variations=2)
#                 for v in variations:
#                     google_vars = google_search(v, num_results=MAX_GOOGLE_RESULTS)
#                     combined_results = list(dict.fromkeys(combined_results + google_vars))
#                 results = combined_results

#     if not results:
#         print("[ERROR] No search results after fallback attempts.")
#         # We will still produce a PDF (with no sources).
#         print("[INFO] Generating empty PDF with just a note...")
#         create_pdf_report(
#             topic=cleaned_query,
#             source_summaries=[],
#             final_summary="No valid sources found after attempts.",
#             filename="research_report.pdf"
#         )
#         return

#     # === STEP 2: For each result, fetch text, check relevance, then summarize ===
#     source_summaries = []
#     for url in results:
#         text = fetch_html_content(url)
#         if not text:
#             print(f"[WARN] Skipped => {url[:60]} => no content.")
#             continue

#         # Relevance check
#         if not gpt_check_relevance(text, cleaned_query, model=model, relevance_threshold=0.45):
#             print(f"[INFO] Skipped => {url[:60]} => deemed not relevant.")
#             continue

#         # Summarize the relevant content
#         print(f"[INFO] Summarizing => {url[:60]}...")
#         summary = gpt_summarize_big_text(text, model=model)
#         source_summaries.append((url, summary))

#     # === STEP 3: If STILL not enough sources, produce a warning but proceed
#     if len(source_summaries) < MIN_REQUIRED_SOURCES:
#         print(f"[WARN] Only {len(source_summaries)} relevant sources have valid content.")

#     if not source_summaries:
#         print("[ERROR] No valid content from any sources. Generating minimal PDF.")
#         create_pdf_report(
#             topic=cleaned_query,
#             source_summaries=[],
#             final_summary="No relevant content was found.",
#             filename="research_report.pdf"
#         )
#         return

#     # === Combine partial summaries into a final big summary ===
#     combined_text = "\n".join([f"Source {i}:\n{summ}" for i, (_, summ) in enumerate(source_summaries, start=1)])
#     final_summary = unify_summaries(combined_text, model=model)

#     # === STEP 4: Create PDF ===
#     create_pdf_report(
#         topic=cleaned_query,
#         source_summaries=source_summaries,
#         final_summary=final_summary,
#         filename="research_report.pdf"
#     )
#     print("[INFO] Research process completed successfully.")


# ###############################################################################
# # 10. USAGE EXAMPLE
# ###############################################################################
# if __name__ == "__main__":
#     # Example usage:
#     user_query = "If a motion was never served to anyone in the case, and the docket reflects no setting, can a court rule on a motion that was never before it?"
#     run_pdf_research(user_query, model="gpt-4o")
