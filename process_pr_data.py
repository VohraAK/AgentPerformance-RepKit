import os
import ijson
import pandas as pd
from tqdm import tqdm

# Base directory where agent folders are located
BASE_DIR = 'AIDev/aidev-pop'
OUTPUT_CSV = 'agent_pr_allcomments_with_loc.csv'

def _extract_comments_with_details(file_path, comment_category):
    """
    Extracts comment body, user login, and user type from a JSON file.
    """
    extracted_entries = []

    if os.path.exists(file_path):
        with open(file_path, 'rb') as f:
            # Use ijson to stream key-value pairs from the root object
            try:
                data_iter = ijson.kvitems(f, '')
                for pr_filename, entries_list in data_iter:
                    pr_id = pr_filename.replace('.json', '')
                    for entry in entries_list:
                        body = entry.get('body', '') # Works for comments and review comments
                        # Reviews have a 'body' and also a 'state'
                        if comment_category == 'Review_Summary':
                            # If it's a review summary, capture the state as well, or just use the body
                            # For simplicity, let's just use the body if available, otherwise consider state.
                            # GitHub review bodies are often empty if it's just an approval/request changes
                            if not body and entry.get('state'):
                                body = f"Review State: {entry['state']}"
                        
                        user = entry.get('user') or {}
                        user_login = user.get('login', 'N/A')
                        user_type = user.get('type', 'N/A')
                        
                        extracted_entries.append({
                            'PR_ID': pr_id,
                            'Comment_Body': body,
                            'User_Login': user_login,
                            'User_Type': user_type,
                            'Comment_Category': comment_category
                        })
            except ijson.common.IncompleteJSONError as e:
                print(f"Warning: Incomplete JSON in {file_path} for category {comment_category}: {e}")
            except Exception as e:
                print(f"Error processing {file_path} for category {comment_category}: {e}")

    return extracted_entries

def process_agent_data(agent_name, agent_path):
    """
    Processes various PR-related JSON files for a single agent
     and returns a list of dictionaries with merged data.
    """
    pr_loc_data = {}

    print(f"Processing commit details for {agent_name}...")

    # --- 0. Load Task Types ---
    print(f"Loading task types for {agent_name}...")
    task_types_path = os.path.join(agent_path, 'gpt_conventional_commits.csv')
    pr_task_map = {}
    if os.path.exists(task_types_path):
        try:
            task_df = pd.read_csv(task_types_path)
            # Ensure ID is string for matching
            task_df['id'] = task_df['id'].astype(str)
            # Create dictionary mapping ID to Type
            pr_task_map = task_df.set_index('id')['type'].to_dict()
            print(f"Loaded {len(pr_task_map)} task classifications for {agent_name}.")
        except Exception as e:
            print(f"Error loading task types from {task_types_path}: {e}")
    else:
        print(f"No task type file found for {agent_name} at '{task_types_path}'.")

    # --- 1. Calculate LOC per PR ---
    commit_details_path = os.path.join(agent_path, 'pr_commit_details.json')

    if os.path.exists(commit_details_path):
        print(f"Streaming commit details for {agent_name}...")
        with open(commit_details_path, 'rb') as f:
            # Use ijson to stream key-value pairs from the root object
            commit_details_iter = ijson.kvitems(f, '')
            
            count = 0
            for pr_filename, commits in tqdm(commit_details_iter, desc=f"Calculating LOC for {agent_name}"):
                count += 1
                pr_id = pr_filename.replace('.json', '')
                total_additions = 0
                total_deletions = 0
                for commit in commits:
                    if 'stats' in commit and commit['stats'] is not None:
                        total_additions += commit['stats'].get('additions', 0)
                        total_deletions += commit['stats'].get('deletions', 0)
                pr_loc_data[pr_id] = {
                    'Total_LOC_Change': total_additions + total_deletions,
                    'Additions': total_additions,
                    'Deletions': total_deletions
                }
        print(f"Calculated LOC for {count} PRs for {agent_name}.")
    else:
        print(f"No commit details found for {agent_name} at '{commit_details_path}'.")
    
    all_comments_and_reviews = []

    # --- 2. Extract PR Comments ---
    print(f"Extracting PR comments for {agent_name}...")
    pr_comments_path = os.path.join(agent_path, 'pr_comments.json')
    all_comments_and_reviews.extend(_extract_comments_with_details(pr_comments_path, 'PR_Comment'))
    
    # --- 3. Extract PR Review Comments ---
    print(f"Extracting PR review comments for {agent_name}...")
    pr_review_comments_path = os.path.join(agent_path, 'pr_review_comments.json')
    all_comments_and_reviews.extend(_extract_comments_with_details(pr_review_comments_path, 'Review_Comment'))

    # --- 4. Extract PR Reviews ---
    print(f"Extracting PR reviews for {agent_name}...")
    pr_reviews_path = os.path.join(agent_path, 'pr_reviews.json')
    all_comments_and_reviews.extend(_extract_comments_with_details(pr_reviews_path, 'Review_Summary'))

    print(f"Merging data for {agent_name}...")
    # --- 5. Merge Data ---
    merged_output = []
    # Using tqdm for the merging loop as well for better progress indication
    for entry in tqdm(all_comments_and_reviews, desc=f"Merging all interactions for {agent_name}"):
        pr_id = entry['PR_ID']
        loc_info = pr_loc_data.get(pr_id, {'Total_LOC_Change': 0, 'Additions': 0, 'Deletions': 0})
        
        merged_output.append({
            'Agent': agent_name,
            'PR_ID': pr_id,
            'Task_Type': pr_task_map.get(pr_id, 'N/A'),
            'Total_LOC_Change': loc_info['Total_LOC_Change'],
            'Additions': loc_info['Additions'],
            'Deletions': loc_info['Deletions'],
            'Comment_Category': entry['Comment_Category'],
            'Comment_Body': entry['Comment_Body'],
            'User_Login': entry['User_Login'],
            'User_Type': entry['User_Type']
        })
    print(f"Merged {len(merged_output)} interaction entries with LOC data for {agent_name}.")
    return merged_output

def main():
    print("Starting data processing for PR comments and LOC changes.")
    all_data = []
    
    # --- 4. Iterate through ALL agent directories ---
    if not os.path.exists(BASE_DIR):
        print(f"Error: Base directory '{BASE_DIR}' not found. Please ensure the path is correct.")
        return

    agent_dirs = [d for d in os.listdir(BASE_DIR) if os.path.isdir(os.path.join(BASE_DIR, d))]
    
    if not agent_dirs:
        print(f"No agent directories found in '{BASE_DIR}'. Exiting.")
        return

    print(f"Found {len(agent_dirs)} agent directories in '{BASE_DIR}'.")
    for agent_name in agent_dirs:
        agent_path = os.path.join(BASE_DIR, agent_name)
        print(f"Processing data for agent: {agent_name} from '{agent_path}'")
        agent_processed_data = process_agent_data(agent_name, agent_path)
        all_data.extend(agent_processed_data)
            
    # --- 5. Save the final processed data ---
    if all_data:
        df = pd.DataFrame(all_data)
        df.to_csv(OUTPUT_CSV, index=False)
        print(f"Successfully processed data for {len(agent_dirs)} agents.")
        print(f"Output saved to '{OUTPUT_CSV}'. Total rows: {len(df)}")
    else:
        print("No data processed. Output CSV not created.")
    print("Data processing complete.")

if __name__ == '__main__':
    main()