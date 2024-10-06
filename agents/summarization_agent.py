from langchain.prompts import ChatPromptTemplate
from langchain_community.llms import OpenAI 
import logging
import tiktoken  # 用来计算文本的 token 数量

# 使用 OpenAI 的 API 来进行文本摘要
async def gpt_summarizer_agent(long_text, api_key):
    logging.info("Starting summarization agent.")
    
    # 初始化 OpenAI API 客户端
    llm = OpenAI(api_key=api_key)

    # 对长文本进行分块处理，按 token 数量而不是单词数量分块
    chunks = chunk_text_by_tokens(long_text, max_tokens=2048)

    summary_prompt = ChatPromptTemplate.from_template("Please summarize the following text: {text}")
    summaries = []

    for i, chunk in enumerate(chunks):
        logging.info(f"Summarizing chunk {i + 1}/{len(chunks)}")
        prompt = summary_prompt.format(text=chunk)
        try:
            # 使用 OpenAI LLM 来生成摘要
            summary = await llm.agenerate(prompt)
            summaries.append(summary['choices'][0]['text'].strip())
        except Exception as e:
            logging.error(f"Failed to summarize chunk {i + 1}: {e}")
    
    # 将所有分块的摘要连接成一个完整的摘要
    return " ".join(summaries)

# Helper function to chunk text by tokens
def chunk_text_by_tokens(text, max_tokens=2048):
    encoding = tiktoken.get_encoding("gpt2")  # 根据使用的模型选择合适的编码方式
    tokens = encoding.encode(text)
    
    # 根据 max_tokens 的长度分割 tokens
    chunks = []
    for i in range(0, len(tokens), max_tokens):
        chunk_tokens = tokens[i:i + max_tokens]
        chunk_text = encoding.decode(chunk_tokens)
        chunks.append(chunk_text)
    
    return chunks
