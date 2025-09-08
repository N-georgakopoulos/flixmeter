from flask import Flask, request, jsonify
from flask_cors import CORS
import pandas as pd
import numpy as np
import re
from datetime import datetime
import os
import traceback
import io

app = Flask(__name__)
CORS(app)

# Enable debug mode for better error messages
app.config['DEBUG'] = True

class RuntimeCalculator:
    def __init__(self):
        self.moviedata_df = None
        self.alttitles_df = None
        self.loaded = False
        
    def load_data(self):
        """Load all necessary data files"""
        try:
            # Check if files exist
            files_to_check = ['moviedata.csv', 'alternatetitles.csv']
            missing_files = []
            
            for file in files_to_check:
                if not os.path.exists(file):
                    missing_files.append(file)
            
            if missing_files:
                return False, f"Missing files: {missing_files}"
                
            # Load the data
            print("Loading moviedata.csv...")
            self.moviedata_df = pd.read_csv('moviedata.csv', low_memory=False)
            
            print("Loading alternatetitles.csv...")
            self.alttitles_df = pd.read_csv('alternatetitles.csv', low_memory=False)
            
            self.loaded = True
            
            print(f"Successfully loaded {len(self.moviedata_df)} movies, {len(self.alttitles_df)} alt titles")
            return True, f"Loaded {len(self.moviedata_df)} movies, {len(self.alttitles_df)} alt titles"
            
        except Exception as e:
            print(f"Error loading data: {str(e)}")
            print(traceback.format_exc())
            return False, f"Error loading data: {str(e)}"

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

    def find_runtime_enhanced(self, title_variations):
        """Enhanced runtime finding with multiple strategies"""
        
        if not self.loaded:
            return None, None, "Database not loaded"
            
        for variation in title_variations:
            if not variation:
                continue
                
            try:
                # Strategy 1: Direct match in moviedata (primary title)
                if 'primaryTitle' in self.moviedata_df.columns:
                    match = self.moviedata_df[self.moviedata_df['primaryTitle'].str.lower() == variation.lower()]
                    if not match.empty:
                        runtime = match.iloc[0]['runtimeMinutes']
                        if pd.notna(runtime) and str(runtime).strip() not in ['', '\\N', 'N/A']:
                            try:
                                runtime_int = int(float(runtime))
                                return runtime_int, match.iloc[0]['tconst'], f"Direct match (primary): '{variation}'"
                            except:
                                continue
                
                # Strategy 2: Direct match in moviedata (original title)  
                if 'originalTitle' in self.moviedata_df.columns:
                    match = self.moviedata_df[self.moviedata_df['originalTitle'].str.lower() == variation.lower()]
                    if not match.empty:
                        runtime = match.iloc[0]['runtimeMinutes']
                        if pd.notna(runtime) and str(runtime).strip() not in ['', '\\N', 'N/A']:
                            try:
                                runtime_int = int(float(runtime))
                                return runtime_int, match.iloc[0]['tconst'], f"Direct match (original): '{variation}'"
                            except:
                                continue
                
                # Strategy 3: Alternative titles exact match
                if 'title' in self.alttitles_df.columns and 'titleId' in self.alttitles_df.columns:
                    alt_match = self.alttitles_df[self.alttitles_df['title'].str.lower() == variation.lower()]
                    if not alt_match.empty:
                        tconst = alt_match.iloc[0]['titleId']
                        movie_match = self.moviedata_df[self.moviedata_df['tconst'] == tconst]
                        if not movie_match.empty:
                            runtime = movie_match.iloc[0]['runtimeMinutes']
                            if pd.notna(runtime) and str(runtime).strip() not in ['', '\\N', 'N/A']:
                                try:
                                    runtime_int = int(float(runtime))
                                    return runtime_int, tconst, f"Alt title exact: '{variation}'"
                                except:
                                    continue
                
            except Exception as e:
                print(f"Error processing variation '{variation}': {str(e)}")
                continue
        
        return None, None, "Not found"

    def analyze_watch_history(self, watchhistory_data, limit=None):
        """Analyze watch history and sum runtimes with enhanced matching"""
        
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
        
        for idx, row in watchhistory_df.iterrows():
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
            runtime, tconst, match_info = self.find_runtime_enhanced(variations)
            
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
                    'reason': 'No matches found in any strategy',
                    'date': str(date) if not pd.isna(date) else ''
                })
        
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
        "status": "Movie Runtime Calculator API",
        "version": "1.0",
        "endpoints": {
            "POST /api/calculate": "Calculate total runtime from watch history CSV",
            "GET /api/status": "Check if database is loaded"
        }
    })

@app.route('/api/status')
def api_status():
    """Check API and database status"""
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
        "message": f"Database loaded: {len(calculator.moviedata_df)} movies, {len(calculator.alttitles_df)} alt titles"
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
        results = calculator.analyze_watch_history(watchhistory_df, limit=limit)
        
        # Add some calculated fields for convenience
        total_count = results['found_count'] + results['not_found_count']
        results['total_count'] = total_count
        results['success_rate'] = (results['found_count'] / total_count * 100) if total_count > 0 else 0
        results['total_hours'] = results['total_runtime'] / 60
        results['total_days'] = results['total_runtime'] / (60 * 24)
        results['avg_runtime'] = results['total_runtime'] / results['found_count'] if results['found_count'] > 0 else 0
        
        return jsonify({
            "success": True,
            "results": results,
            "message": f"Processed {total_count} titles, found {results['found_count']} matches"
        })
        
    except Exception as e:
        return jsonify({
            "success": False,
            "error": f"Internal server error: {str(e)}",
            "traceback": traceback.format_exc()
        }), 500

# Initialize calculator and try to load data on startup
calculator = RuntimeCalculator()

# Try to load data on startup
try:
    success, message = calculator.load_data()
    if success:
        print(f"✅ Startup: {message}")
    else:
        print(f"⚠️ Startup warning: {message}")
except Exception as e:
    print(f"❌ Startup error: {str(e)}")

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)