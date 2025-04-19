# TubeWhale 🐳✨
``` bash
88888888888       888               888       888 888    888        d8888 888      8888888888 
    888           888               888   o   888 888    888       d88888 888      888        
    888           888               888  d8b  888 888    888      d88P888 888      888        
    888  888  888 88888b.   .d88b.  888 d888b 888 8888888888     d88P 888 888      8888888    
    888  888  888 888 "88b d8P  Y8b 888d88888b888 888    888    d88P  888 888      888        
    888  888  888 888  888 88888888 88888P Y88888 888    888   d88P   888 888      888        
    888  Y88b 888 888 d88P Y8b.     8888P   Y8888 888    888  d8888888888 888      888        
    888   "Y88888 88888P"   "Y8888  888P     Y888 888    888 d88P     888 88888888 8888888888 
```                                                                                  
                                                                                              
                                                                                              
TubeWhale is a fun, open-source, AI-powered multi-agent video processing system designed to search for and analyze YouTube videos efficiently! 🚀 Although the pipeline is currently runnable, there are still a few engineering improvements to be made to ensure its robustness. 🛠️

### Project Status: 🟢


![Logo](logo.png)

![star-history](star-history-2024109.png)

### TubeWhale – An Enhanced AI Product Documentation for Multi-Agent Keyword Brainstorming and Video Analysis

## 1. Introduction:
TubeWhale is an open-source AI-powered multi-agent video processing system designed to search for and analyze YouTube videos efficiently. By leveraging keyword brainstorming, video metadata collection, and multimodal analysis (including audio transcription), the system provides intelligent summaries and insights into video content. It is especially suited for research and use cases where automatic topic generation and summarization are essential.💡

Key Differentiator: TubeWhale employs multiple AI agents to brainstorm topic keywords and searches for YouTube videos based on those keywords. Users have control over the number of videos analyzed, ensuring precision and flexibility tailored to their specific needs.🎯

### Flow Chart
![flow-chart](flow-chart.png)

### Features
- 🔑 Keyword Brainstorming: AI agents expand your base keyword into multiple variations.
- 🎥 YouTube Search & Metadata: Fetches top‑k videos per keyword, deduplicates, scores by view/like/comment.
- 📝 Transcription & Summaries: Uses YouTube captions → Whisper fallback → GPT‑4 summarization.
- 🎙️ Audio Analysis: Optional full‑audio Whisper transcription + GPT summarization + chunked‑fallback.
- 💾 Persistence: Stores metadata, transcripts, summaries, comments, logs in SQLite.
- 📊 Report: Generates a JSON report with token usage and cost.
- ⚙️ Highly Configurable: All parameters via .env or CLI flags.

## 1. Key Concepts and Parameters
When running the system, the user can customize various parameters that control how the pipeline operates:

```bash
python3 main.py 
```
You will receive a database with max `MAX_N` * `TOP_K` videos. This videos list will be deduplicated.

## Key Concepts and Configuration & Parameter Explanations:

### TubeWhale is highly configurable through environment variables. Below are the key parameters and their explanations to help you tailor the system to your requirements.

# 2.Configuration ⚙️
Create a `.env` in project root:
```bash
# YouTube API keys (comma‑separated)
# API keys
YOUTUBE_API_KEYS="AIzaSyXXX,AIzaSyYYY,AIzaSyZZZ"
OPENAI_API_KEY="sk-..."

# Pipeline flags
FULL_AUDIO_ANALYSIS=true
PERSIST_AGENT_SUMMARIES=true
DRY_RUN=false

# Search & analysis
KEYWORD="Arizona homeless during covid19"
MAX_N=2
TOP_K=2
FILTER_TYPE="view_count"

# Storage & concurrency
DB_PATH="AZcovidhomeless.db"
CONCURRENCY=2
```

Variable | Description
KEYWORD (required) | Base search term for keyword brainstorming.
MAX_N (required) | Number of keyword variations to generate.
TOP_K (required) | Number of videos fetched per variation.
FILTER_TYPE (default=view_count) | How to sort/filter videos (view_count, like_count, etc.).
FULL_AUDIO_ANALYSIS (true/false) | Enable Whisper + GPT audio processing.
PERSIST_AGENT_SUMMARIES (true/false) | Store transcript summaries and audio summaries.
DRY_RUN (true/false) | No external API calls or DB writes — for testing.
DB_PATH (default=youtube_summaries.db) | SQLite database file path.
CONCURRENCY (default=3) | Number of parallel video processing tasks.

# 3. Environment Setup
Requirements
Python Version >=3.13.x
```bash
git clone https://github.com/yaninsanity/TubeWhale.git
cd TubeWhale
python3.11 -m venv venv
source venv/bin/activate
# install torch cpu 
pip3 install --pre torch torchvision torchaudio --extra-index-url https://download.pytorch.org/whl/nightly/cpu
pip install pip --upgrade
pip install -r requirements.txt
python3 main.py
```

Additionally, install FFmpeg:
On macOS: `brew install ffmpeg`
On Linux: `sudo apt install ffmpeg`


# 4 How Tubewhale🐳 Works

1. **Keyword Brainstorming**  
   GPT‑4 agents generate `MAX_N` variations of your base `KEYWORD`.

2. **YouTube Search**  
   - Fetch the top `TOP_K` videos for each keyword variation.  
   - Deduplicate results.  
   - Score videos by view count, likes, comments (per `FILTER_TYPE`).

3. **Metadata Storage**  
   Store each video’s metadata in the `videos` table.

4. **Transcription & Summaries**  
   1. **Try YouTube captions** via `TranscriptAgent`.  
   2. **Fallback to Whisper** for full‑audio transcription.  
   3. **GPT Summarization**  
      - Save raw transcripts and generated summaries in the `transcripts` table.  
      - Log prompt/response, tokens and cost in `ai_interactions`.

5. **Audio Analysis** (optional; if `FULL_AUDIO_ANALYSIS=true`)  
   - **Mode A:** Full‑audio → Whisper → GPT summary.  
   - **Mode B (fallback):**  
     1. Slice audio into 60 s chunks.  
     2. Run Whisper + GPT on each chunk.  
     3. Recursively merge chunk summaries into one final summary.

6. **Comments**  
   Fetch all comments via YouTube API and save them in the `comments` table.

7. **Standardization**  
   Normalize each summary to the JSON schema using `StandardizerAgent`.

8. **Final Report**  
   After all videos are processed, generate `logs/report_<timestamp>.json` containing:  
   - `processed_videos` count  
   - List of `video_ids`  
   - `total_prompt_tokens` and `total_completion_tokens`  
   - `total_cost`  
   - Run `timestamp`


# 5 Usage Example 🎉
To run the system with your desired parameters, simply execute:
```bash
python3 main.py 
```


## 6. Testing
We have integrated `pytest` for unit testing. To ensure the test, what you can do is in project root run following
```bash
pytest --cov
```

## 7. Contributing
We welcome contributions from the open-source community. Here’s how you can contribute:

### Reporting Bugs[🪲]:
If you encounter any issues while using TubeWhale, please open an issue on GitHub with following:
- a clear description of the bug and steps to reproduce it.
- The way you think which module goes wrong. Any traceback?

### Pull Requests:
Fork the repository and create a new branch for your feature [🚩] or bugfix [🪲🔫] .

Commit your changes with clear and descriptive messages.
Push your branch to your forked repository.
Open a pull request describing the changes made. I will review when if I have the time 👀

## 8. Donation Polygon & Support 💖☕️

###  😊 I will apprecatie if you show your love or just buy me a cup of coffee ☕️.  
![Polygon](image.png)


# 9. License 📜
This project is licensed under the [MIT](https://mit-license.org/) License.

# 10. Contact 📧
For any inquiries or support, please contact: admin@jl-blog.com
Please include the header: [TubeWhale] Support/Question: ... in your email.

# 11. Citing TubeWhale🔖
If you use TubeWhale in your research or data collection, please consider citing our project to acknowledge our efforts. Proper citation supports the ongoing development of open-source tools.


