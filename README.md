# Gravix AI Content Agent (yt-automation)

An advanced content optimization and automation agent designed for short-form video reposting. This system integrates Gemini AI for intelligent metadata generation, Google Sheets for workflow management, and FFmpeg for automated video processing.

## 🚀 Key Features

- **AI Optimization**: Leverages Google Gemini to generate SEO-friendly titles, descriptions, and hashtags.
- **Fingerprint Mitigation**: Automated FFmpeg transformation instructions to avoid platform duplicate detection.
- **Workflow Management**: Bidirectional integration with Google Sheets for tracking pending and processed videos.
- **Robust Orchestration**: Built-in rate limiting, retries, and detailed logging for reliable long-running operations.
- **Content Safety**: Automated risk assessment for copyright and community guidelines.

## 🛠️ Tech Stack

- **Core**: Python 3.10+
- **AI**: Google Generative AI (Gemini Pro)
- **Data**: Google Sheets API, SQLite (Queue Management)
- **Video**: FFmpeg

## 📦 Installation & Setup

1. **Clone the repository**:
   ```bash
   git clone https://github.com/gravixrdp/yt-automation.git
   cd yt-automation
   ```

2. **Setup virtual environment**:
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

3. **Configure Environment**:
   - Copy `.env.example` to `.env`.
   - Add your `GEMINI_API_KEY`.
   - Place your Google Cloud `service_account.json` in the root directory.

4. **Initialize Sheets**:
   ```bash
   python3 main.py --setup
   ```

## 🎮 Usage

### Process All Pending Rows
```bash
python3 main.py --all
```

### Dry Run (Visualizing AI Output)
```bash
python3 main.py --row-id 1 --dry-run
```

## 🏗️ Project Structure

- `main.py`: CLI Entry point and orchestrator.
- `ai_agent.py`: Gemini AI integration and validation logic.
- `sheets_client.py`: Google Sheets API wrapper.
- `ffmpeg_worker.py`: Automated video processing logic.
- `scheduler.py`: Background task orchestration.

---
*Built with ❤️ for Content Creators.*
