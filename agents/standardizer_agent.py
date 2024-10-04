from langchain.prompts import ChatPromptTemplate
from langchain_community.llms import OpenAI
import logging

# 通用标准化模板
def generate_general_template():
    return '''
    Please standardize and enrich the following summary. Include structured information based on the following aspects:
    - Main topic of the video
    - Key insights or knowledge shared
    - Important steps or actions mentioned
    - Recommended tools or equipment (if applicable)
    - Best practices or tips shared
    - Any notable challenges or advice given

    Summary: {summary}
    '''

# 元数据提取和标准化总结
def generate_metadata_prompt():
    return '''
    Please analyze the following video metadata and provide a structured analysis. Include information about:
    - Total number of likes and dislikes
    - Number of comments and engagement level
    - Number of views and subscribers
    - Any notable video tags or hashtags
    - Duration and publish date

    Metadata: {metadata}
    '''

# 标准化代理，整合总结和元数据
async def standardizer_agent(summary, metadata, api_key):
    logging.info("Starting standardizer agent.")
    
    # 初始化 OpenAI 客户端
    llm = OpenAI(api_key=api_key)

    # Step 1: 生成通用的标准化总结
    general_prompt = generate_general_template()
    prompt = ChatPromptTemplate.from_template(general_prompt)
    base_summary = prompt.format(summary=summary)

    try:
        logging.info("Generating standardized summary.")
        # 生成基础的标准化总结
        standardized_summary = await llm.agenerate(base_summary)

        # Step 2: 生成元数据分析提示
        metadata_prompt = generate_metadata_prompt()
        detailed_prompt = ChatPromptTemplate.from_template(metadata_prompt)
        enriched_metadata = detailed_prompt.format(metadata=metadata)

        logging.info("Generating metadata analysis.")
        # 生成元数据分析
        enriched_summary = await llm.agenerate(enriched_metadata)

        logging.info("Standardization and metadata analysis completed.")
        return {
            "standardized_summary": standardized_summary,
            "metadata_analysis": enriched_summary
        }

    except Exception as e:
        logging.error(f"Error in standardization: {e}")
        return None  # 保证在错误情况下不会中断流程
