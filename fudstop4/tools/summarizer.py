import os
import chardet
import pdfplumber
import docx
import torch
from transformers import pipeline

def read_pdf(file_path: str) -> str:
    """
    Extracts text from a PDF file using pdfplumber.
    Returns a concatenated string of all text in the PDF.
    """
    text_content = []
    with pdfplumber.open(file_path) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text() or ""
            text_content.append(page_text)
    return "\n".join(text_content)

def read_docx(file_path: str) -> str:
    """
    Extracts text from a DOCX file using python-docx.
    Returns a concatenated string of all text in the DOCX file.
    """
    doc = docx.Document(file_path)
    full_text = []
    for para in doc.paragraphs:
        full_text.append(para.text)
    return "\n".join(full_text)

def read_text(file_path: str) -> str:
    """
    Reads and decodes text from a file using chardet to detect encoding.
    Returns the decoded text content.
    """
    with open(file_path, 'rb') as f:
        raw_data = f.read()
    result = chardet.detect(raw_data)
    encoding = result['encoding'] or 'utf-8'
    return raw_data.decode(encoding, errors='replace')

def read_document(file_path: str) -> str:
    """
    Reads the file based on its extension:
      - .pdf  -> pdfplumber
      - .docx -> python-docx
      - else  -> attempts to read as text with chardet
    """
    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    extension = os.path.splitext(file_path)[1].lower()

    if extension == '.pdf':
        return read_pdf(file_path)
    elif extension == '.docx':
        return read_docx(file_path)
    else:
        return read_text(file_path)

def chunk_text(text: str, chunk_size: int = 2000, overlap: int = 300) -> list:
    """
    Splits large text into overlapping chunks so we don't exceed model limits.
    Each chunk is ~chunk_size characters, with an overlap to maintain context continuity.
    
    :param text:      The full text to split.
    :param chunk_size:Approx. size (in characters) of each chunk.
    :param overlap:   Number of overlapping characters between chunks.
    :return:          A list of text chunks.
    """
    chunks = []
    start = 0
    length = len(text)

    while start < length:
        end = min(start + chunk_size, length)
        chunk = text[start:end]
        chunks.append(chunk)

        start = end - overlap
        if start < 0:
            start = 0
        if end >= length:
            break

    return chunks

def summarize_text(text: str) -> str:
    """
    Summarizes the given text using a TWO-PASS approach:
      1. Summarize in chunks (first pass).
      2. Summarize the combined chunk summaries again (second pass).
    
    Utilizes the "facebook/bart-large-cnn" model and leverages GPU if available.
    """
    # 1) Detect device: GPU if available, else CPU
    device = 0 if torch.cuda.is_available() else -1

    # 2) Initialize the summarization pipeline with a BART model
    summarizer = pipeline(
        "summarization",
        model="facebook/bart-large-cnn",
        device=device
    )

    # 3) First pass: break the text into manageable chunks
    text_chunks = chunk_text(text, chunk_size=2000, overlap=300)
    partial_summaries = []

    # Summaries for each chunk
    for chunk in text_chunks:
        # Increase max_length for a richer chunk summary
        summary_output = summarizer(
            chunk,
            max_length=200,  # you can tweak these
            min_length=50,
            do_sample=False
        )
        partial_summaries.append(summary_output[0]['summary_text'])

    # 4) Second pass: combine the partial summaries into a new text
    combined_summary_text = " ".join(partial_summaries)

    # If there's only one chunk or the doc is small, we can skip second summarization
    if len(text_chunks) == 1:
        # Already summarized once, so just return it
        return combined_summary_text

    # Otherwise, second pass summarization for a consolidated final summary
    final_summary_output = summarizer(
        combined_summary_text,
        max_length=1599,  # bigger max to capture entire doc
        min_length=50,
        do_sample=False
    )
    final_summary = final_summary_output[0]['summary_text']

    return final_summary

def main():
    """
    Main function to run the summarizer:
      1. Prompts user for file path
      2. Reads file with robust approach
      3. Summarizes file content (two-pass)
      4. Prints the summary
    """
    print("=== World Class Summarizer (GPU-Enabled) ===")
    file_path = input("Enter the absolute path to the document: ").strip()

    try:
        text_data = read_document(file_path)
        if not text_data.strip():
            print("Could not extract any text from the file.")
            return

        summary = summarize_text(text_data)
        print("\n=== FINAL SUMMARY ===")
        print(summary)
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
