# 🎬 Metadata & Asset Generator for Plex & Kometa

A robust, multi-threaded Python tool to automate the extraction, enrichment, and management of metadata and artwork for your Plex libraries. This script fetches high-quality data from TMDb, generates [Kometa](https://kometa.wiki/) compatible YAML files, manages poster/season assets, and keeps your library clean by removing orphans—all with smart update logic and flexible configuration.

---

## 🚀 What Does This Script Do?

- **Connects to Plex:** Reads your Plex libraries directly.
- **Fetches TMDb Metadata:** Pulls rich, up-to-date info for movies and TV shows.
- **Smart Metadata Updates:** Only updates YAML if something has changed, minimizing unnecessary writes.
- **Asset Management:** Downloads, upgrades, and manages posters and season artwork.
- **Orphan Cleanup:** Removes unused metadata and asset files for a tidy library.
- **Kometa-Compatible:** Outputs YAML ready for [Kometa](https://kometa.wiki/) and similar tools.
- **Multi-threaded:** Fast, parallel processing for large libraries.
- **Dry-Run Mode:** Test everything safely—no files are written or deleted.
- **Highly Configurable:** Choose which libraries, asset types, and metadata to process—all via `config.yml`.

---

## 🛠️ How It Works

1. **Connects to Plex** using your server URL and token.
2. **Scans your selected libraries** for movies and TV shows.
3. **Fetches metadata from TMDb** for each item, using smart caching and update logic.
4. **Downloads and manages posters/season artwork** based on your preferences.
5. **Writes YAML files** compatible with Kometa, one per library.
6. **Optionally cleans up orphaned metadata and assets** not linked to any current Plex item.
7. **Logs a detailed summary** of all actions and changes.

---

## 📦 Requirements

- **Python:** 3.8+
- **Dependencies:**
  - `requests`
  - `plexapi`
  - `PyYAML`
  - `pycountry`
  - `Pillow` (for image handling)

Install all dependencies with:

```bash
pip install -r requirements.txt
```

---

## ⚙️ Configuration Guide

### 1. Download and Prepare Your Config

- Download the provided `config_template.yml` from the repo.
- **Rename it to `config.yml`** (the script will only use `config.yml`).

### 2. Fill in Your Details

Open `config.yml` and fill in the following:

```yaml
#Plex server configuration
plex:
  url: "http://localhost:32400"
  token: "YOUR_PLEX_TOKEN"

# TMDb API configuration
tmdb:
  api_key: "YOUR_TMDB_API_KEY"
  language: "en"
  region: "US"
  fallback:
    - zh
    - ja
    - fr

# Plex libraries
preferred_libraries:
  - Movies
  - TV Shows
cleanup_orphans: true

# Logging
log_level: "INFO"

# Threading
threads:
  max_workers: 10
  timeout: 300

# Metadata output
metadata_path: "/config/metadata/"

# Asset management
assets_path: "/path/to/assets"

# Poster selection preferences
poster_selection:
  preferred_width: 2000
  preferred_height: 3000
  min_width: 1000
  min_height: 1500
  preferred_vote: 7.0
  vote_relaxed: 5.0
  vote_average_threshold: 5.0

# Network and retry settings
network:
  backoff_factor: 1
  max_retries: 3
  timeout: 10
  pool_connections: 100
  pool_maxsize: 100

# Dry run
dry_run: false
```

#### 🔑 **How to Get Your Plex Token**
- Follow this guide: [How to find your Plex Token](https://support.plex.tv/articles/204059436-finding-an-authentication-token-x-plex-token/)
- Paste your token in the `token` field.

#### 🎬 **How to Get a TMDb API Key**
- Sign up at [TMDb](https://www.themoviedb.org/) and request an API key: [TMDb API Key Guide](https://developers.themoviedb.org/3/getting-started/introduction)
- Paste your API key in the `api_key` field.

---

## 🏃 Usage

Run the script from your terminal:

```bash
python metadata_gen.py
```

> **Note:** All options and behaviors are controlled via your `config.yml`. There are no CLI flags—simply edit your config and run the script.

---

## 📝 How to Read the Output

- **YAML files** are generated in your `metadata_path`, one per library.
- **Assets** (posters, season images) are saved in your configured `assets_path`.
- **Logs** are written to `metadata_generator.log` for troubleshooting and audit.

---

## 🧹 Orphan Cleanup

When enabled, the script will:
- Remove TMDb cache entries not present in your current Plex libraries.
- Remove metadata entries from YAML files that no longer match any Plex item.
- Delete poster/season asset files not referenced by any current item (with safety checks to avoid accidental deletion).

---

## 🛡️ Safety Features

- **Dry-Run Mode:** No files are written or deleted—perfect for testing.
- **Smart Update:** Only writes YAML or downloads assets if something has changed.
- **Asset Tracking:** Prevents accidental deletion of assets still in use.

---

## 🛠️ Roadmap & Upcoming Enhancements

Here’s what’s coming next (and how you can help!):

1. **Background Poster Download**  
   - 🎨 Download TMDb backgrounds for movies and TV shows. *Done
   - User-configurable width, height, vote average, and language preferences. *Done

2. **Configurable Asset Types**  
   - 🖼️ Turn season posters and background downloads on/off via config options. *Done

3. **Enhanced Episode Metadata**  
   - 🎭 Improved fallbacks to fetch more detailed crew and cast info for episodes.

4. **User-Configurable Metadata Limits**  
   - 🔧 Set how many cast/crew members to include in metadata via config.

5. **Franchise/Collection Extraction**  
   - 📚 Extract franchise/collection info from TMDb and generate Kometa-compatible collection YAML files.
   - Include poster URLs for collections and franchises.

6. **Speed Optimizations**  
   - ⚡ Further parallelization and smarter caching for even faster runs. *Done at the best i could

---

## 💡 Suggestions for a More Visual & Engaging Experience

- **Add progress bars** (e.g., with [tqdm](https://tqdm.github.io/)) for real-time feedback.
- **Generate HTML reports** with summary tables and asset previews.
- **Use emojis and colorized logs** for easier reading in the terminal.
- **Add a web dashboard** for monitoring and controlling runs (future idea!).

> **Want to make it even more visual?**  
> Consider adding screenshots, flowcharts, or even short demo videos to this README.  
> You can also use badges (e.g., build status, Python version) at the top for a more professional look.

---

## 📚 Resources

- [Kometa Metadata Wiki](https://kometa.wiki/)
- [Plex API Docs](https://python-plexapi.readthedocs.io/en/latest/)
- [TMDb API Docs](https://developers.themoviedb.org/3/getting-started/introduction)

---

## 📝 License

MIT License

---

Enjoy your perfectly organized Plex library! 🍿