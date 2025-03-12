# utils/config.py
import os
import argparse
from dotenv import dotenv_values

class Config:
    def __init__(self, cli_args: dict = None):
        # 先读取 .env 配置
        env_config = dotenv_values(".env")
        # 默认配置（全部转为大写键）
        self.KEYWORD = env_config.get("KEYWORD", "")
        self.YOUTUBE_API_KEYS = env_config.get("YOUTUBE_API_KEYS", "").split(',')
        self.OPENAI_API_KEY = env_config.get("OPENAI_API_KEY", "")
        self.DB_PATH = env_config.get("DB_PATH", "youtube_summaries.db")
        self.PERSIST_AGENT_SUMMARIES = env_config.get("PERSIST_AGENT_SUMMARIES", "true").lower() == "true"
        self.FULL_AUDIO_ANALYSIS = env_config.get("FULL_AUDIO_ANALYSIS", "true").lower() == "true"
        self.DRY_RUN = env_config.get("DRY_RUN", "false").lower() == "true"
        self.MAX_N = int(env_config.get("MAX_N", "5"))
        self.TOP_K = int(env_config.get("TOP_K", "3"))
        self.FILTER_TYPE = env_config.get("FILTER_TYPE", "view_count")
        self.CONCURRENCY = int(env_config.get("CONCURRENCY", "1"))
        self.PURE_YOUTUBE = env_config.get("PURE_YOUTUBE", "false").lower() == "true"
        
        # 覆盖配置：CLI 参数优先
        if cli_args:
            for key, value in cli_args.items():
                setattr(self, key, value)
    
    # @staticmethod
    # def parse_cli_arguments():
    #     """
    #     解析 CLI 参数，并返回一个大写键的字典，CLI 参数会覆盖 .env 中的配置。
    #     """
    #     import argparse
    #     parser = argparse.ArgumentParser(
    #         description="YouTube Summarization Pipeline",
    #         formatter_class=argparse.ArgumentDefaultsHelpFormatter
    #     )
    #     parser.add_argument("--keyword", type=str, help="Base search keyword")
    #     parser.add_argument("--top_k", type=int, help="Number of videos to retrieve per keyword variation")
    #     parser.add_argument("--filter_type", type=str, help="Sorting method for search results")
    #     parser.add_argument("--youtube_api_key", type=str, help="YouTube Data API Key (overrides first key in YOUTUBE_API_KEYS)")
    #     parser.add_argument("--openai_api_key", type=str, help="OpenAI API Key")
    #     parser.add_argument("--db_path", type=str, help="Path to the database")
    #     parser.add_argument("--persist_agent_summaries", action="store_true", help="Enable persisting agent results")
    #     parser.add_argument("--no_persist_agent_summaries", action="store_true", help="Disable persisting agent results")
    #     parser.add_argument("--full_audio_analysis", action="store_true", help="Enable full audio analysis")
    #     parser.add_argument("--no_full_audio_analysis", action="store_true", help="Disable full audio analysis")
    #     parser.add_argument("--dry_run", action="store_true", help="Enable dry run mode")
    #     parser.add_argument("--no_dry_run", action="store_true", help="Disable dry run mode")
    #     parser.add_argument("--max_n", type=int, help="Number of keyword variations to generate")
    #     parser.add_argument("--concurrency", type=int, help="Number of concurrent tasks")
    #     parser.add_argument("--pure_youtube", action="store_true", help="Pure YouTube mode: only use base keyword")
    #     args = parser.parse_args()
    #     # 将参数转成大写 key 的字典
    #     cli_args = {k.upper(): v for k, v in vars(args).items() if v is not None}
    #     # 特殊处理布尔值的开关
    #     if cli_args.get("NO_PERSIST_AGENT_SUMMARIES", False):
    #         cli_args["PERSIST_AGENT_SUMMARIES"] = False
    #     if cli_args.get("NO_FULL_AUDIO_ANALYSIS", False):
    #         cli_args["FULL_AUDIO_ANALYSIS"] = False
    #     if cli_args.get("NO_DRY_RUN", False):
    #         cli_args["DRY_RUN"] = False
    #     return cli_args

    def __str__(self):
        # 仅打印关键配置，避免敏感信息泄露
        return (f"KEYWORD: {self.KEYWORD}, TOP_K: {self.TOP_K}, MAX_N: {self.MAX_N}, "
                f"PURE_YOUTUBE: {self.PURE_YOUTUBE}, CONCURRENCY: {self.CONCURRENCY}")
