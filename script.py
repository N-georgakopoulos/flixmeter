import pandas as pd
import numpy as np
import re
from datetime import datetime

def load_data():
    """Load all necessary data files"""
    print("Loading data files...")
    
    try:
        # Load movie data
        moviedata_df = pd.read_csv('moviedata.csv')
        print(f"✓ Loaded {len(moviedata_df)} movie entries")
        
        # Load alternative titles
        alttitles_df = pd.read_csv('alternatetitles.csv')
        print(f"✓ Loaded {len(alttitles_df)} alternative titles")
        
        # Load watch history - this is what we want to sum
        watchhistory_df = pd.read_csv('watchhistory.csv')
        print(f"✓ Loaded {len(watchhistory_df)} watch history entries")
        
        return moviedata_df, alttitles_df, watchhistory_df
        
    except Exception as e:
        print(f"Error loading data: {e}")
        return None, None, None

def clean_title_for_matching(title):
    """Clean title for better matching"""
    if pd.isna(title) or not isinstance(title, str):
        return ""
    
    # Remove common prefixes/suffixes that might interfere
    cleaned = title.strip()
    
    # Remove year patterns like (2020) or [2020]
    cleaned = re.sub(r'\s*[\(\[]\d{4}[\)\]]\s*', '', cleaned)
    
    # Remove "Season X:" patterns for TV shows
    cleaned = re.sub(r':\s*Season\s+\d+\s*:', ':', cleaned)
    
    return cleaned.strip()

def extract_variations(title):
    """Extract different title variations for matching"""
    if pd.isna(title) or not isinstance(title, str):
        return [""]
    
    variations = []
    cleaned = clean_title_for_matching(title)
    
    # Original cleaned title
    variations.append(cleaned)
    
    # Episode title (last part after colon)
    if ':' in cleaned:
        episode_title = cleaned.split(':')[-1].strip()
        if episode_title:
            variations.append(episode_title)
    
    # Series title (first part before colon)
    if ':' in cleaned:
        series_title = cleaned.split(':')[0].strip()
        if series_title:
            variations.append(series_title)
    
    # Remove "The" from beginning
    for var in variations.copy():
        if var.lower().startswith('the '):
            variations.append(var[4:].strip())
    
    # Remove duplicates while preserving order
    seen = set()
    unique_variations = []
    for var in variations:
        if var and var not in seen:
            seen.add(var)
            unique_variations.append(var)
    
    return unique_variations

def find_runtime_enhanced(title_variations, moviedata_df, alttitles_df):
    """Enhanced runtime finding with multiple strategies"""
    
    for variation in title_variations:
        if not variation:
            continue
            
        # Strategy 1: Direct match in moviedata (primary title)
        match = moviedata_df[moviedata_df['primaryTitle'].str.lower() == variation.lower()]
        if not match.empty:
            runtime = match.iloc[0]['runtimeMinutes']
            if pd.notna(runtime) and str(runtime).strip() not in ['', '\\N', 'N/A']:
                return int(float(runtime)), match.iloc[0]['tconst'], f"Direct match (primary): '{variation}'"
        
        # Strategy 2: Direct match in moviedata (original title)
        match = moviedata_df[moviedata_df['originalTitle'].str.lower() == variation.lower()]
        if not match.empty:
            runtime = match.iloc[0]['runtimeMinutes']
            if pd.notna(runtime) and str(runtime).strip() not in ['', '\\N', 'N/A']:
                return int(float(runtime)), match.iloc[0]['tconst'], f"Direct match (original): '{variation}'"
        
        # Strategy 3: Partial match in moviedata (contains)
        match = moviedata_df[moviedata_df['primaryTitle'].str.contains(re.escape(variation), case=False, na=False)]
        if not match.empty:
            # Get the shortest match (most likely to be exact)
            match = match.loc[match['primaryTitle'].str.len().idxmin()]
            runtime = match['runtimeMinutes']
            if pd.notna(runtime) and str(runtime).strip() not in ['', '\\N', 'N/A']:
                return int(float(runtime)), match['tconst'], f"Partial match (primary): '{variation}' → '{match['primaryTitle']}'"
        
        # Strategy 4: Alternative titles exact match
        alt_match = alttitles_df[alttitles_df['title'].str.lower() == variation.lower()]
        if not alt_match.empty:
            tconst = alt_match.iloc[0]['titleId']
            movie_match = moviedata_df[moviedata_df['tconst'] == tconst]
            if not movie_match.empty:
                runtime = movie_match.iloc[0]['runtimeMinutes']
                if pd.notna(runtime) and str(runtime).strip() not in ['', '\\N', 'N/A']:
                    return int(float(runtime)), tconst, f"Alt title exact: '{variation}'"
        
        # Strategy 5: Alternative titles partial match
        alt_match = alttitles_df[alttitles_df['title'].str.contains(re.escape(variation), case=False, na=False)]
        if not alt_match.empty:
            # Try first few matches
            for _, alt_row in alt_match.head(3).iterrows():
                tconst = alt_row['titleId']
                movie_match = moviedata_df[moviedata_df['tconst'] == tconst]
                if not movie_match.empty:
                    runtime = movie_match.iloc[0]['runtimeMinutes']
                    if pd.notna(runtime) and str(runtime).strip() not in ['', '\\N', 'N/A']:
                        return int(float(runtime)), tconst, f"Alt title partial: '{variation}' → '{alt_row['title']}'"
    
    return None, None, "Not found"

def analyze_watch_history(watchhistory_df, moviedata_df, alttitles_df):
    """Analyze watch history and sum runtimes with enhanced matching"""
    
    print("\n" + "="*70)
    print("ANALYZING WATCH HISTORY - ENHANCED MATCHING")
    print("="*70)
    
    total_runtime = 0
    found_entries = []
    not_found_entries = []
    
    print(f"\nProcessing {len(watchhistory_df)} titles...")
    
    for idx, row in watchhistory_df.head(20).iterrows():
        title = row['Title']
        date = row['Date']
        
        # Convert to string and handle NaN
        title_str = str(title) if not pd.isna(title) else 'nan'
        
        if title_str in ['nan', 'None', ''] or title_str.strip() == '':
            print(f"[{idx+1}/{len(watchhistory_df)}] Skipping empty: {title_str}")
            not_found_entries.append({
                'original_title': title_str,
                'variations_tried': '',
                'reason': 'Empty/Invalid title',
                'date': date
            })
            continue
        
        print(f"\n[{idx+1}/{len(watchhistory_df)}] Processing: '{title_str}'")
        
        # Get all variations of the title
        variations = extract_variations(title_str)
        print(f"  Trying variations: {variations}")
        
        # Try to find runtime with enhanced matching
        runtime, tconst, match_info = find_runtime_enhanced(variations, moviedata_df, alttitles_df)
        
        if runtime:
            total_runtime += runtime
            found_entries.append({
                'original_title': title_str,
                'matched_via': match_info,
                'runtime': runtime,
                'tconst': tconst,
                'date': date
            })
            print(f"  ✓ FOUND: {match_info} - Runtime: {runtime} mins")
        else:
            not_found_entries.append({
                'original_title': title_str,
                'variations_tried': str(variations),
                'reason': 'No matches found in any strategy',
                'date': date
            })
            print(f"  ✗ NOT FOUND: Tried {len(variations)} variations")
    
    return {
        'total_runtime': total_runtime,
        'found_count': len(found_entries),
        'not_found_count': len(not_found_entries),
        'found_entries': found_entries,
        'not_found_entries': not_found_entries
    }

def print_summary(results):
    """Print comprehensive summary"""
    
    total_time = results['total_runtime']
    found_count = results['found_count']
    not_found_count = results['not_found_count']
    total_count = found_count + not_found_count
    
    print("\n" + "="*70)
    print("RUNTIME SUMMARY")
    print("="*70)
    
    print(f"\nMATCHING RESULTS:")
    print(f"  ✓ Found: {found_count} titles ({found_count/total_count*100:.1f}%)")
    print(f"  ✗ Not Found: {not_found_count} titles ({not_found_count/total_count*100:.1f}%)")
    print(f"  📊 Success Rate: {found_count/total_count*100:.1f}%")
    
    print(f"\nRUNTIME TOTALS:")
    print(f"  🎬 Total Runtime: {total_time:,} minutes")
    print(f"  🕐 Total Hours: {total_time/60:.1f} hours")
    print(f"  📅 Total Days: {total_time/(60*24):.1f} days")
    print(f"  📈 Average per title: {total_time/found_count:.1f} minutes" if found_count > 0 else "  📈 Average: N/A")
    
    # Breakdown by matching strategy
    strategy_counts = {}
    for entry in results['found_entries']:
        strategy = entry['matched_via'].split(':')[0]
        strategy_counts[strategy] = strategy_counts.get(strategy, 0) + 1
    
    if strategy_counts:
        print(f"\nMATCHING STRATEGY BREAKDOWN:")
        for strategy, count in sorted(strategy_counts.items(), key=lambda x: x[1], reverse=True):
            print(f"  {strategy}: {count} matches")

def save_results(results):
    """Save only runtime summary to single CSV file"""
    
    print(f"\n" + "="*70)
    print("SAVING RUNTIME SUMMARY")
    print("="*70)
    
    # Create comprehensive summary data
    total_time = results['total_runtime']
    found_count = results['found_count']
    not_found_count = results['not_found_count']
    total_count = found_count + not_found_count
    
    
    # Strategy breakdown
    strategy_counts = {}
    strategy_runtimes = {}
    for entry in results['found_entries']:
        strategy = entry['matched_via'].split(':')[0]
        strategy_counts[strategy] = strategy_counts.get(strategy, 0) + 1
        strategy_runtimes[strategy] = strategy_runtimes.get(strategy, 0) + entry['runtime']
    
    # Build summary table
    summary_data = []
    
    # Basic metrics
    summary_data.extend([

        ['Total Titles Processed', total_count],
        ['Found Count', found_count],
        ['Not Found Count', not_found_count],
        ['Success Rate (%)', f'{found_count/total_count*100:.1f}'],
        ['Total Runtime (minutes)', f'{total_time:,}'],
        ['Total Runtime (hours)', f'{total_time/60:.1f}'],
        ['Total Runtime (days)', f'{total_time/(60*24):.1f}'],
        ['Average Runtime per Found Title (min)', f'{total_time/found_count:.1f}' if found_count > 0 else 'N/A'],
        ['', ''],  # Separator
        ['MATCHING STRATEGY BREAKDOWN', '']
    ])
    
    # Strategy breakdown
    for strategy, count in sorted(strategy_counts.items(), key=lambda x: x[1], reverse=True):
        runtime_for_strategy = strategy_runtimes[strategy]
        summary_data.append([f'{strategy} - Count', count])
        summary_data.append([f'{strategy} - Runtime (min)', f'{runtime_for_strategy:,}'])
    
    # Add separator and detailed data
    summary_data.extend([
        ['', ''],  # Separator
        ['FOUND ENTRIES SAMPLE (First 10)', '']
    ])
    
    # Add sample of found entries
    for i, entry in enumerate(results['found_entries'][:10]):
        summary_data.append([f'Found {i+1} - Title', entry['original_title']])
        summary_data.append([f'Found {i+1} - Runtime', entry['runtime']])
        summary_data.append([f'Found {i+1} - Strategy', entry['matched_via']])
    
    # Add sample of not found entries
    if results['not_found_entries']:
        summary_data.extend([
            ['', ''],  # Separator
            ['NOT FOUND ENTRIES SAMPLE (First 10)', '']
        ])
        
        for i, entry in enumerate(results['not_found_entries'][:10]):
            summary_data.append([f'Not Found {i+1} - Title', entry['original_title']])
            summary_data.append([f'Not Found {i+1} - Reason', entry['reason']])
    
    # Create DataFrame and save
    summary_df = pd.DataFrame(summary_data, columns=['Metric', 'Value'])
    summary_df.to_csv('runtime_summary.csv', index=False)
    
    print(f"✓ Saved comprehensive summary to 'runtime_summary.csv'")
    print(f"  Contains: Basic stats, strategy breakdown, and sample entries")
    print(f"  File includes all data in a single convenient table")

def main():
    """Main execution"""
    print("RUNTIME SUMMER - Enhanced Movie Runtime Calculator")
    print("="*70)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Load data
    moviedata_df, alttitles_df, watchhistory_df = load_data()
    
    if moviedata_df is None:
        print("Failed to load data. Exiting.")
        return
    
    # Analyze and sum runtimes
    results = analyze_watch_history(watchhistory_df, moviedata_df, alttitles_df)
    
    # Print summary
    print_summary(results)
    
    # Save results
    save_results(results)
    
    print(f"\n{'='*70}")
    print(f"Analysis completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)

if __name__ == "__main__":
    main()