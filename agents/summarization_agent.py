import logging
from openai import OpenAI
import tiktoken  # Tokenizer to manage tokens and chunking

# GPT Summarizer Agent using GPT-4
async def gpt_summarizer_agent(long_text, api_key):
    logging.info("Starting the GPT summarizer agent.")
    
    # Initialize OpenAI client
    client = OpenAI(api_key=api_key)

    # Step 1: Tokenization and splitting the text into manageable chunks
    chunk_size = 4096  # Adjust for GPT model's token limit (allow space for response)
    overlap = 50  # Overlap tokens to maintain context between chunks
    chunks = chunk_text_by_tokens(long_text, max_tokens=chunk_size, overlap=overlap)

    summaries = []

    for i, chunk in enumerate(chunks):
        logging.info(f"Summarizing chunk {i + 1}/{len(chunks)}")

        # Preparing the prompt
        prompt = (
            f"You are an expert in summarizing complex texts professionally. "
            f"Please summarize the following text while considering continuity:\n{chunk}"
        )

        try:
            # Create a completion with GPT-4
            response = client.completions.create(
                model="gpt-4",  # Ensure you're using the correct GPT-4 model
                prompt=prompt,
                max_tokens=1024,  # Limit for the model's response
                temperature=0.5
            )

            # Extract the summary from the response
            chunk_summary = response.choices[0].text.strip()  # Extract the summary
            summaries.append(chunk_summary)

        except Exception as e:
            logging.error(f"Failed to summarize chunk {i + 1}: {e}")

    # Concatenate all chunk summaries into a final summary
    final_summary = " ".join(summaries)
    logging.info("Summarization completed.")
    return final_summary

# Helper function to chunk text by token count (with overlap)
def chunk_text_by_tokens(text, max_tokens=4096, overlap=50):
    """
    Tokenize the text and split it into chunks with the given token limit and overlap.
    """
    tokenizer = tiktoken.get_encoding("cl100k_base")  # GPT-4 compatible tokenizer
    tokens = tokenizer.encode(text)

    chunks = []
    for i in range(0, len(tokens), max_tokens - overlap):
        chunk_tokens = tokens[i:i + max_tokens]
        chunk_text = tokenizer.decode(chunk_tokens)
        chunks.append(chunk_text)

    return chunks
