#!/bin/bash
# run_scraper_loop.sh — Run scraper periodically (every 1 hour)

# Ensure log dir exists
mkdir -p /home/ubuntu/gravix-agent/logs

# Source environment
cd /home/ubuntu/gravix-agent
# source .env  # (Scraper loads .env via python-dotenv, but sourcing here helps too)

while true; do
  echo "[$(date)] Starting Scraper Run..."
  # Check for per-source trigger flags (from Telegram)
  shopt -s nullglob
  TRIGGERED=0
  for flag in /tmp/shorts_upload/*/trigger_scrape_*.flag /tmp/shorts_ingest/*/trigger_scrape_*.flag; do
    TRIGGERED=1
    tab_name=$(basename "$flag")
    tab_name="${tab_name#trigger_scrape_}"
    tab_name="${tab_name%.flag}"
    echo "[$(date)] Triggered scrape for $tab_name"
    /home/ubuntu/gravix-agent/venv/bin/python3 /home/ubuntu/gravix-agent/scraper.py --source "$tab_name" >> /home/ubuntu/gravix-agent/logs/scraper_loop.log 2>&1
    rm -f "$flag"
  done
  shopt -u nullglob

  # If no trigger, run batch scrape
  if [ "$TRIGGERED" -eq 0 ]; then
    /home/ubuntu/gravix-agent/venv/bin/python3 /home/ubuntu/gravix-agent/scraper.py --batch >> /home/ubuntu/gravix-agent/logs/scraper_loop.log 2>&1
  fi
  
  EXIT_CODE=$?
  echo "[$(date)] Scraper finished with exit code $EXIT_CODE."
  
  if [ $EXIT_CODE -ne 0 ]; then
    echo "Scraper failed! Sleeping 5m before retry..."
    sleep 300
  else
    # Read interval from file (default: 3600)
    INTERVAL_FILE="/home/ubuntu/gravix-agent/scraper_interval.txt"
    INTERVAL=3600
    if [ -f "$INTERVAL_FILE" ]; then
      READ_VAL=$(cat "$INTERVAL_FILE")
      if [[ "$READ_VAL" =~ ^[0-9]+$ ]]; then
        INTERVAL=$READ_VAL
      fi
    fi
    
    echo "Sleeping $INTERVAL seconds..."
    SLEPT=0
    while [ "$SLEPT" -lt "$INTERVAL" ]; do
      # check for new trigger flags every 30s
      for flag in /tmp/shorts_upload/*/trigger_scrape_*.flag /tmp/shorts_ingest/*/trigger_scrape_*.flag; do
        [ -e "$flag" ] || continue
        tab_name=$(basename "$flag")
        tab_name="${tab_name#trigger_scrape_}"
        tab_name="${tab_name%.flag}"
        echo "[$(date)] Triggered scrape for $tab_name"
        /home/ubuntu/gravix-agent/venv/bin/python3 /home/ubuntu/gravix-agent/scraper.py --source "$tab_name" >> /home/ubuntu/gravix-agent/logs/scraper_loop.log 2>&1
        rm -f "$flag"
      done
      sleep 30
      SLEPT=$((SLEPT + 30))
    done
  fi
done
