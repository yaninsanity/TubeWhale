from langchain.prompts import ChatPromptTemplate
from langchain_community.llms import OpenAI  # 更新为正确的导入路径
import logging

async def critic_agent(summary, api_key):
    logging.info("Starting critic agent.")
    
    # 初始化 OpenAI 客户端
    llm = OpenAI(api_key=api_key)

    # 设置评价提示模板
    critique_prompt = ChatPromptTemplate.from_template("Please critique the following summary: {summary}")
    prompt = critique_prompt.format(summary=summary)

    try:
        # 执行 LLM 请求以生成评价
        critique = await llm.agenerate(prompt)
        logging.info("Critique completed successfully.")
        return critique
    except Exception as e:
        logging.error(f"Error in critique generation: {e}")
        return None  # 如果出现错误，返回 None 以确保流程继续
