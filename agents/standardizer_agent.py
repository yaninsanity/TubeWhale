from langchain.prompts import ChatPromptTemplate
from langchain_community.llms import OpenAI
import logging

# Template for standardizing and enriching the summary
def generate_general_template():
    return '''
    Please standardize and enrich the following summary. Include structured information based on:
    - Main topic of the video
    - Key insights or knowledge shared
    - Important steps or actions mentioned
    - Recommended tools or equipment (if applicable)
    - Best practices or tips shared
    - Notable challenges or advice given

    Summary: {summary}
    '''

# Metadata analysis and summary enrichment
def generate_metadata_prompt():
    return '''
    Please analyze the following video metadata and provide a structured analysis. Include information about:
    - Total number of likes and dislikes
    - Number of comments and engagement level
    - Number of views and subscribers
    - Notable video tags or hashtags
    - Duration and publish date

    Metadata: {metadata}
    '''

# Standardizer agent to standardize the summary and analyze metadata
def standardizer_agent(summary, metadata, api_key):
    logging.info("Starting standardizer agent.")
    
    if not summary or not metadata:
        logging.error("Summary or metadata is missing. Skipping standardization.")
        return None

    llm = OpenAI(api_key=api_key)

    try:
        # Generate and format the general summary prompt
        general_prompt = generate_general_template()
        prompt = ChatPromptTemplate.from_template(general_prompt)
        base_summary = prompt.format(summary=summary)

        # Call the OpenAI API for summary standardization
        standardized_response = llm.generate(base_summary)
        standardized_summary = standardized_response.choices[0].text.strip()

        # Generate and format the metadata prompt
        metadata_prompt = generate_metadata_prompt()
        detailed_prompt = ChatPromptTemplate.from_template(metadata_prompt)
        enriched_metadata = detailed_prompt.format(metadata=metadata)

        # Call the OpenAI API for metadata analysis
        metadata_response = llm.generate(enriched_metadata)
        metadata_analysis = metadata_response.choices[0].text.strip()

        logging.info("Standardization and metadata analysis completed.")
        return {
            "standardized_summary": standardized_summary,
            "metadata_analysis": metadata_analysis
        }

    except Exception as e:
        logging.error(f"Error in standardization: {e}")
        return None
