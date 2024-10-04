from langchain.prompts import ChatPromptTemplate
from langchain_community.llms import OpenAI 
import logging

# 使用 OpenAI 的 API 来进行文本摘要
async def gpt_summarizer_agent(long_text, api_key):
    logging.info("Starting summarization agent.")
    
    # 初始化 OpenAI API 客户端
    llm = OpenAI(api_key=api_key)

    # 对长文本进行分块处理
    chunks = chunk_text(long_text)

    summary_prompt = ChatPromptTemplate.from_template("Please summarize the following text: {text}")
    summaries = []

    for i, chunk in enumerate(chunks):
        logging.info(f"Summarizing chunk {i + 1}/{len(chunks)}")
        prompt = summary_prompt.format(text=chunk)
        try:
            # 使用 OpenAI LLM 来生成摘要
            summary = llm(prompt)
            summaries.append(summary)
        except Exception as e:
            logging.error(f"Failed to summarize chunk {i + 1}: {e}")
    
    return " ".join(summaries)

# Helper function to chunk text
def chunk_text(text, max_length=128000):
    words = text.split()
    return [" ".join(words[i:i + max_length]) for i in range(0, len(words), max_length)]
