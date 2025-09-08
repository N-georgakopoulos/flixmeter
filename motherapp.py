from flask import Flask, request, jsonify
from flask_cors import CORS
import pandas as pd
import numpy as np
import re
from datetime import datetime
import os
import traceback
import io
import gc

app = Flask(__name__)
CORS(app)

# Enable debug mode for better error messages
app.config['DEBUG'] = True

class ChunkedRuntimeCalculator:
    def __init__(self):
        self.chunk_size = 10000  # Process 10k rows at a time
        self.loaded = False
        self.moviedata_file = 'moviedata.csv'  # Local file
        self.alttitles_file = 'alternatetitles.csv'  # Local file
        
    def load_data(self):
        """Check if local CSV files exist and are readable"""
        try:
            # Check if files exist
            files_to_check = [self.moviedata_file, self.alttitles_file]
            missing_files = []
            
            for file in files_to_check:
                if not os.path.exists(file):
                    missing_files.append(file)
            
            if missing_files:
                return False, f"Missing local CSV files: {missing_files}. Please upload: {', '.join(missing_files)} to your deployment."
            
            # Test that we can read the headers
            try:
                moviedata_sample = pd.read_csv(self.moviedata_file, nrows=1)
                alttitles_sample = pd.read_csv(self.alttitles_file, nrows=1)
                print(f"Moviedata columns: {list(moviedata_sample.columns)}")
                print(f"Alt titles columns: {list(alttitles_sample.columns)}")
            except Exception as e:
                return False, f"Error reading CSV headers: {str(e)}"
            
            self.loaded = True
            print("✅ Local CSV files ready for chunked processing")
            return True, "Local CSV files loaded and ready"
            
        except Exception as e:
            print(f"Error in load_data: {str(e)}")
            print(traceback.format_exc())
            return False, f"Error preparing data: {str(e)}"

    def clean_title_for_matching(self, title):
        """Clean title for better matching"""
        if pd.isna(title) or not isinstance(title, str):
            return ""
        
        cleaned = title.strip()
        cleaned = re.sub(r'\s*[\(\[]\d{4}[\)\]]\s*', '', cleaned)
        cleaned = re.sub(r':\s*Season\s+\d+\s*:', ':', cleaned)
        return cleaned.strip()

    def extract_variations(self, title):
        """Extract different title variations for matching"""
        if pd.isna(title) or not isinstance(title, str):
            return [""]
        
        variations = []
        cleaned = self.clean_title_for_matching(title)
        variations.append(cleaned)
        
        if ':' in cleaned:
            episode_title = cleaned.split(':')[-1].strip()
            if episode_title:
                variations.append(episode_title)
            
            series_title = cleaned.split(':')[0].strip()
            if series_title:
                variations.append(series_title)
        
        for var in variations.copy():
            if var.lower().startswith('the '):
                variations.append(var[4:].strip())
        
        seen = set()
        unique_variations = []
        for var in variations:
            if var and var not in seen:
                seen.add(var)
                unique_variations.append(var)
        
        return unique_variations

    def find_runtime_chunked(self, title_variations):
        """Find runtime using chunked reading to minimize memory usage"""
        
        if not self.loaded:
            return None, None, "Database not loaded"
            
        # Convert variations to lowercase for faster comparison
        variations_lower = [v.lower() for v in title_variations if v]
        if not variations_lower:
            return None, None, "No valid variations"
            
        try:
            # Strategy 1: Search in moviedata chunks
            moviedata_reader = pd.read_csv(self.moviedata_file, chunksize=self.chunk_size, low_memory=False)
            
            for chunk_idx, chunk in enumerate(moviedata_reader):
                # Clean up memory
                if chunk_idx > 0 and chunk_idx % 10 == 0:
                    gc.collect()
                
                # Check primary title
                if 'primaryTitle' in chunk.columns:
                    chunk['primaryTitle_lower'] = chunk['primaryTitle'].astype(str).str.lower()
                    for idx, var in enumerate(variations_lower):
                        match = chunk[chunk['primaryTitle_lower'] == var]
                        if not match.empty:
                            runtime = match.iloc[0]['runtimeMinutes']
                            if pd.notna(runtime) and str(runtime).strip() not in ['', '\\N', 'N/A']:
                                try:
                                    runtime_int = int(float(runtime))
                                    return runtime_int, match.iloc[0]['tconst'], f"Primary title match: '{title_variations[idx]}'"
                                except:
                                    continue
                
                # Check original title
                if 'originalTitle' in chunk.columns:
                    chunk['originalTitle_lower'] = chunk['originalTitle'].astype(str).str.lower()
                    for idx, var in enumerate(variations_lower):
                        match = chunk[chunk['originalTitle_lower'] == var]
                        if not match.empty:
                            runtime = match.iloc[0]['runtimeMinutes']
                            if pd.notna(runtime) and str(runtime).strip() not in ['', '\\N', 'N/A']:
                                try:
                                    runtime_int = int(float(runtime))
                                    return runtime_int, match.iloc[0]['tconst'], f"Original title match: '{title_variations[idx]}'"
                                except:
                                    continue
            
            # Strategy 2: Search in alternate titles chunks
            alttitles_reader = pd.read_csv(self.alttitles_file, chunksize=self.chunk_size, low_memory=False)
            
            for chunk_idx, alt_chunk in enumerate(alttitles_reader):
                if chunk_idx > 0 and chunk_idx % 10 == 0:
                    gc.collect()
                
                if 'title' in alt_chunk.columns and 'titleId' in alt_chunk.columns:
                    alt_chunk['title_lower'] = alt_chunk['title'].astype(str).str.lower()
                    
                    for idx, var in enumerate(variations_lower):
                        alt_match = alt_chunk[alt_chunk['title_lower'] == var]
                        if not alt_match.empty:
                            tconst = alt_match.iloc[0]['titleId']
                            
                            # Now find this tconst in moviedata
                            moviedata_reader2 = pd.read_csv(self.moviedata_file, chunksize=self.chunk_size, low_memory=False)
                            
                            for movie_chunk in moviedata_reader2:
                                movie_match = movie_chunk[movie_chunk['tconst'] == tconst]
                                if not movie_match.empty:
                                    runtime = movie_match.iloc[0]['runtimeMinutes']
                                    if pd.notna(runtime) and str(runtime).strip() not in ['', '\\N', 'N/A']:
                                        try:
                                            runtime_int = int(float(runtime))
                                            return runtime_int, tconst, f"Alt title match: '{title_variations[idx]}'"
                                        except:
                                            continue
                                    break  # Found the tconst, no need to continue
            
            return None, None, "Not found after chunked search"
            
        except Exception as e:
            print(f"Error in chunked search: {str(e)}")
            return None, None, f"Search error: {str(e)}"

    def analyze_watch_history(self, watchhistory_data, limit=None):
        """Analyze watch history using chunked processing"""
        
        total_runtime = 0
        found_entries = []
        not_found_entries = []
        
        # Convert to DataFrame if it's not already
        if isinstance(watchhistory_data, list):
            watchhistory_df = pd.DataFrame(watchhistory_data)
        else:
            watchhistory_df = watchhistory_data
        
        # Apply limit if specified
        if limit:
            watchhistory_df = watchhistory_df.head(limit)
        
        # Process in smaller batches to manage memory
        batch_size = 50  # Process 50 titles at a time
        total_rows = len(watchhistory_df)
        
        for batch_start in range(0, total_rows, batch_size):
            batch_end = min(batch_start + batch_size, total_rows)
            batch_df = watchhistory_df.iloc[batch_start:batch_end]
            
            print(f"Processing batch {batch_start//batch_size + 1}/{(total_rows-1)//batch_size + 1} ({batch_start+1}-{batch_end}/{total_rows})")
            
            for idx, row in batch_df.iterrows():
                title = row.get('Title', row.get('title', ''))
                date = row.get('Date', row.get('date', ''))
                
                title_str = str(title) if not pd.isna(title) else 'nan'
                
                if title_str in ['nan', 'None', ''] or title_str.strip() == '':
                    not_found_entries.append({
                        'original_title': title_str,
                        'variations_tried': '',
                        'reason': 'Empty/Invalid title',
                        'date': str(date) if not pd.isna(date) else ''
                    })
                    continue
                
                variations = self.extract_variations(title_str)
                runtime, tconst, match_info = self.find_runtime_chunked(variations)
                
                if runtime:
                    total_runtime += runtime
                    found_entries.append({
                        'original_title': title_str,
                        'matched_via': match_info,
                        'runtime': runtime,
                        'tconst': tconst,
                        'date': str(date) if not pd.isna(date) else ''
                    })
                else:
                    not_found_entries.append({
                        'original_title': title_str,
                        'variations_tried': str(variations),
                        'reason': 'No matches found in chunked search',
                        'date': str(date) if not pd.isna(date) else ''
                    })
            
            # Force garbage collection after each batch
            gc.collect()
        
        return {
            'total_runtime': total_runtime,
            'found_count': len(found_entries),
            'not_found_count': len(not_found_entries),
            'found_entries': found_entries,
            'not_found_entries': not_found_entries
        }

@app.route('/')
def index():
    """API status endpoint"""
    return jsonify({
        "status": "Local Files Movie Runtime Calculator API",
        "version": "2.1",
        "features": ["Chunked CSV processing", "Local file access", "Memory-efficient"],
        "endpoints": {
            "POST /api/calculate": "Calculate total runtime from watch history CSV",
            "GET /api/status": "Check if local CSV files are loaded"
        }
    })

@app.route('/api/status')
def api_status():
    """Check API and local file status"""
    if not calculator.loaded:
        success, message = calculator.load_data()
        if not success:
            return jsonify({
                "success": False,
                "loaded": False,
                "error": message
            }), 500
    
    return jsonify({
        "success": True,
        "loaded": True,
        "message": "Local CSV files ready for chunked processing",
        "chunk_size": calculator.chunk_size,
        "files": {
            "moviedata": os.path.exists(calculator.moviedata_file),
            "alttitles": os.path.exists(calculator.alttitles_file)
        }
    })

@app.route('/api/calculate', methods=['POST'])
def calculate_runtime():
    """Main API endpoint to calculate runtime from uploaded CSV"""
    
    try:
        # Check if database is loaded
        if not calculator.loaded:
            success, message = calculator.load_data()
            if not success:
                return jsonify({
                    "success": False,
                    "error": f"Database loading failed: {message}"
                }), 500
        
        # Check if file was uploaded
        if 'watchhistory' not in request.files:
            return jsonify({
                "success": False,
                "error": "No file uploaded. Please upload a CSV file with 'watchhistory' key."
            }), 400
        
        file = request.files['watchhistory']
        
        if file.filename == '':
            return jsonify({
                "success": False,
                "error": "No file selected"
            }), 400
        
        if not file.filename.lower().endswith('.csv'):
            return jsonify({
                "success": False,
                "error": "File must be a CSV file"
            }), 400
        
        # Read CSV from uploaded file
        try:
            # Read the file content
            file_content = file.read()
            
            # Try different encodings
            for encoding in ['utf-8', 'latin-1', 'cp1252']:
                try:
                    file_string = file_content.decode(encoding)
                    file_io = io.StringIO(file_string)
                    watchhistory_df = pd.read_csv(file_io)
                    break
                except UnicodeDecodeError:
                    continue
            else:
                return jsonify({
                    "success": False,
                    "error": "Could not decode CSV file. Please ensure it's saved as UTF-8."
                }), 400
                
        except Exception as e:
            return jsonify({
                "success": False,
                "error": f"Error reading CSV file: {str(e)}"
            }), 400
        
        # Check if CSV has required columns
        available_columns = watchhistory_df.columns.tolist()
        title_column = None
        
        # Look for title column (case insensitive)
        for col in available_columns:
            if col.lower() in ['title', 'name', 'movie', 'show']:
                title_column = col
                break
        
        if not title_column:
            return jsonify({
                "success": False,
                "error": f"CSV must contain a title column. Found columns: {available_columns}"
            }), 400
        
        # Rename the title column to standardize
        if title_column != 'Title':
            watchhistory_df = watchhistory_df.rename(columns={title_column: 'Title'})
        
        # Get optional limit parameter
        limit = request.form.get('limit')
        if limit:
            try:
                limit = int(limit)
                if limit <= 0:
                    limit = None
            except:
                limit = None
        
        # Process the watch history
        print(f"Starting analysis of {len(watchhistory_df)} titles...")
        results = calculator.analyze_watch_history(watchhistory_df, limit=limit)
        
        # Add some calculated fields for convenience
        total_count = results['found_count'] + results['not_found_count']
        results['total_count'] = total_count
        results['success_rate'] = (results['found_count'] / total_count * 100) if total_count > 0 else 0
        results['total_hours'] = results['total_runtime'] / 60
        results['total_days'] = results['total_runtime'] / (60 * 24)
        results['avg_runtime'] = results['total_runtime'] / results['found_count'] if results['found_count'] > 0 else 0
        
        # Force garbage collection
        gc.collect()
        
        return jsonify({
            "success": True,
            "results": results,
            "message": f"Processed {total_count} titles, found {results['found_count']} matches using local CSV files"
        })
        
    except Exception as e:
        return jsonify({
            "success": False,
            "error": f"Internal server error: {str(e)}",
            "traceback": traceback.format_exc()
        }), 500

# Initialize calculator
calculator = ChunkedRuntimeCalculator()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)