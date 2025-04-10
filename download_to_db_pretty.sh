# This script was built entirely by Claude AI after being given the 'download_to_db.sh' script and prompted for a status bar
#!/bin/bash
mkdir -p gharchive_new
cd gharchive_new

# Max number of simultaneous background processes
MAX_PROCESSES=12
# Array to track process PIDs
declare -a PIDS
# Array to track day labels for each process
declare -a DAYS

# Function to display a progress bar
display_progress() {
  local day=$1
  local current=$2
  local total=$3
  local status=$4
  local width=50
  local percent=$((current * 100 / total))
  local completed=$((width * current / total))
  
  # Build the progress bar
  local bar="["
  for ((i=0; i<completed; i++)); do
    bar+="#"
  done
  for ((i=completed; i<width; i++)); do
    bar+="-"
  done
  bar+="]"
  
  # Print the progress bar with day and percentage
  printf "\r%-10s %-10s %s %3d%% (%d/%d)" "$day" "$status" "$bar" "$percent" "$current" "$total"
}

# Function to update progress in the correct line
update_progress() {
  local day=$1
  local current=$2
  local total=$3
  local status=$4
  local line=$5
  
  # Move cursor to the specific line
  tput cup "$line" 0
  display_progress "$day" "$current" "$total" "$status"
}

# Clear the terminal and set up the display area
clear
echo "GHArchive Downloader and Processor"
echo "-----------------------------------"
echo ""
for ((i=0; i<MAX_PROCESSES; i++)); do
  echo ""  # Reserve lines for progress bars
done
echo ""
echo "Press Ctrl+C to exit gracefully"

# Function to process a file and update progress
process_file() {
  local FILE=$1
  local DAY=$2
  local LINE=$3
  local FILE_NUM=$4
  
  update_progress "$DAY" "$FILE_NUM" 24 "Processing" "$LINE"
  
  gzip -cd "$FILE" | jq -c 'select(.type | IN("WatchEvent", "ForkEvent", "IssuesEvent", "PullRequestEvent", "PushEvent", "CreateEvent")) |
  [
    ("'"$FILE"'" | scan("[0-9]+-[0-9]+-[0-9]+-[0-9]+")),
    .type,
    .actor.login? // .actor_attributes.login? // (.actor | strings) // null,
    .repo.id? // null,
    .repo.name? // (.repository.owner? + "/" + .repository.name?) // null,
    .created_at,
    .payload.updated_at? // .payload.comment?.updated_at? // .payload.issue?.updated_at? // .payload.pull_request?.updated_at? // null,
    .payload.action,
    .payload.comment.id,
    .payload.review.body // .payload.comment.body // .payload.issue.body? // .payload.pull_request.body? // .payload.release.body? // null,
    .payload.comment?.path? // null,
    .payload.comment?.position? // null,
    .payload.comment?.line? // null,
    .payload.ref? // null,
    .payload.ref_type? // null,
    .payload.comment.user?.login? // .payload.issue.user?.login? // .payload.pull_request.user?.login? // null,
    .payload.issue.number? // .payload.pull_request.number? // .payload.number? // null,
    .payload.issue.title? // .payload.pull_request.title? // null,
    [.payload.issue.labels?[]?.name // .payload.pull_request.labels?[]?.name],
    .payload.issue.state? // .payload.pull_request.state? // null,
    .payload.issue.locked? // .payload.pull_request.locked? // null,
    .payload.issue.assignee?.login? // .payload.pull_request.assignee?.login? // null,
    [.payload.issue.assignees?[]?.login? // .payload.pull_request.assignees?[]?.login?],
    .payload.issue.comments? // .payload.pull_request.comments? // null,
    .payload.review.author_association // .payload.issue.author_association? // .payload.pull_request.author_association? // null,
    .payload.issue.closed_at? // .payload.pull_request.closed_at? // null,
    .payload.pull_request.merged_at? // null,
    .payload.pull_request.merge_commit_sha? // null,
    [.payload.pull_request.requested_reviewers?[]?.login],
    [.payload.pull_request.requested_teams?[]?.name],
    .payload.pull_request.head?.ref? // .payload.ref? // null,
    .payload.pull_request.head?.sha? // .payload.head? // null,
    .payload.pull_request.base?.ref? // null,
    .payload.pull_request.base?.sha? // null,
    .payload.pull_request.merged? // null,
    .payload.pull_request.mergeable? // null,
    .payload.pull_request.rebaseable? // null,
    .payload.pull_request.mergeable_state? // null,
    .payload.pull_request.merged_by?.login? // null,
    .payload.pull_request.review_comments? // null,
    .payload.pull_request.maintainer_can_modify? // null,
    .payload.pull_request.commits? // null,
    .payload.pull_request.additions? // null,
    .payload.pull_request.deletions? // null,
    .payload.pull_request.changed_files? // null,
    .payload.comment.diff_hunk? // null,
    .payload.comment.original_position? // null,
    .payload.comment.commit_id? // null,
    .payload.comment.original_commit_id? // null,
    .payload.size? // null,
    .payload.distinct_size? // null,
    .payload.member.login? // .payload.member? // null,
    .payload.release?.tag_name? // null,
    .payload.release?.name? // null,
    .payload.review?.state? // null
  ]' | clickhouse-client --input_format_null_as_default 1 --date_time_input_format best_effort --query 'INSERT INTO github_events FORMAT JSONCompactEachRow' && rm "$FILE" || echo "Error processing $FILE" >&2
}

# Handle pending files first
if [ "$(find . -name "*.json.gz" | wc -l)" -gt 0 ]; then
  tput cup 3 0
  echo "Processing pre-existing files..."
  find . -name "*.json.gz" | while read FILE; do
    process_file "$FILE" "Existing" 4 1
  done
  tput cup 3 0
  echo "Pre-existing files processed.        "
fi

# Get the latest date from the database
LATEST=$(clickhouse-client --query "SELECT max(created_at) FROM github_events FORMAT TabSeparatedRaw")
[ -z "$LATEST" ] && LATEST="2011-02-12"  # Default if empty

tput cup 3 0
echo "Starting from: $LATEST"

START=$(date -u -d "$LATEST" +%s)
NOW=$(date -u +%s)
START=$((START + 3600 - START % 3600))

# Function to handle downloading and processing for a single day
process_day() {
  local day_start=$1
  local slot=$2
  local line=$((4 + slot))
  local day=$(date -u -d "@$day_start" "+%Y-%m-%d")
  
  update_progress "$day" 0 24 "Download" "$line"
  
  # Download all 24 files for the day
  for ((h=0; h<24; h++)); do
    local file=$(date -u -d "@$((day_start + h*3600))" "+%Y-%m-%d-%-H.json.gz")
    if [ ! -f "$file" ]; then
      wget -q --continue "https://data.gharchive.org/$file" 
      # Wait briefly to avoid hammering the server
      sleep 0.2
    fi
    update_progress "$day" $((h+1)) 24 "Download" "$line"
  done
  
  # Process files sequentially
  for ((h=0; h<24; h++)); do
    local file=$(date -u -d "@$((day_start + h*3600))" "+%Y-%m-%d-%-H.json.gz")
    if [ -f "$file" ]; then
      process_file "$file" "$day" "$line" $((h+1))
    fi
  done
  
  update_progress "$day" 24 24 "Complete" "$line"
  sleep 1
  # Clear the line when done
  tput cup "$line" 0
  printf "%-80s" ""
}

# Track available slots
declare -a SLOTS
for ((i=0; i<MAX_PROCESSES; i++)); do
  SLOTS[$i]=1
done

# Process days
for ((d=START; d<=NOW; d+=86400)); do
  # Find an available slot
  while true; do
    for ((i=0; i<MAX_PROCESSES; i++)); do
      if [ "${SLOTS[$i]}" -eq 1 ]; then
        # Slot is available
        SLOTS[$i]=0
        process_day "$d" "$i" &
        PIDS[$i]=$!
        DAYS[$i]=$(date -u -d "@$d" "+%Y-%m-%d")
        break 2
      fi
    done
    # No slots available, wait for one to complete
    for ((i=0; i<MAX_PROCESSES; i++)); do
      if [ "${SLOTS[$i]}" -eq 0 ]; then
        if ! kill -0 ${PIDS[$i]} 2>/dev/null; then
          # Process has completed
          SLOTS[$i]=1
          break
        fi
      fi
    done
    sleep 0.5
  done
done

# Wait for all remaining processes to complete
for ((i=0; i<MAX_PROCESSES; i++)); do
  if [ "${SLOTS[$i]}" -eq 0 ]; then
    wait ${PIDS[$i]}
  fi
done

tput cup $((4 + MAX_PROCESSES + 1)) 0
echo "All data processing completed."
