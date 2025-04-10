#!/bin/bash
mkdir -p gharchive_new
cd gharchive_new

# Max number of simultaneous background processes
# ClickHouse default is number of available threads
MAX_PROCESSES=12
PROCESS_COUNT=0

# Added repo_id column to github_events table.
# Filter data for only the events that I need
process_file() {
  local FILE=$1
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
  ]' | clickhouse-client --input_format_null_as_default 1 --date_time_input_format best_effort --query 'INSERT INTO github_events FORMAT JSONCompactEachRow' && rm "$FILE" || echo 'File $FILE has issues'
}

if [ $(ls | wc -l) -gt 0 ]; then
  echo "Processing pre-existing files..."

  find . -name "*.json.gz" | while read FILE; do
    process_file "$FILE"
  done
  echo "Done"
fi

# LATEST="2011-02-12"
LATEST=$(clickhouse-client --query "SELECT max(created_at) FROM github_events FORMAT TabSeparatedRaw")

echo "Starting at $LATEST"

START=$(date -u -d "$LATEST" +%s)
NOW=$(date -u +%s)
START=$((START + 3600 - START % 3600))

for ((d=START; d<=NOW; d+=86400)); do  # Loop through each day
  DAY=$(date -u -d "@$d" "+%Y-%m-%d")

  # Use find and process the files in a single background job
  (
    echo "Downloading files for $DAY"

    # Download all 24 files for the day
    for ((h=0; h<24; h++)); do
      FILE=$(date -u -d "@$((d + h*3600))" "+%Y-%m-%d-%-H.json.gz")
      while [ $(ls | wc -l) -gt 336 ]; do
        echo "Too many files, waiting..."
        sleep 2
      done

      if [ ! -f "$FILE" ]; then
        wget -q --continue "https://data.gharchive.org/$FILE"
      fi
    done

    echo "Processing files for $DAY"

    find . -type f -name "$DAY-*.json.gz" | while read FILE; do
      if [ -f "$FILE" ]; then
        # echo "Processing $FILE"
        process_file "$FILE"
      fi
    done
  ) &

  # Increment process counter and wait if it exceeds MAX_PROCESSES
  ((PROCESS_COUNT++))
  if [ $PROCESS_COUNT -ge $MAX_PROCESSES ]; then
    wait -n  # Wait for at least one process to finish
    ((PROCESS_COUNT--))
  fi
done
