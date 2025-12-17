import os
import pandas as pd
from fastapi import UploadFile
import shutil
from pathlib import Path

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

class DataManager:
    def __init__(self):
        self.current_file_path: Path | None = None
        self.parquet_path: Path | None = None
        self.df: pd.DataFrame | None = None

    async def save_upload(self, file: UploadFile) -> Path:
        """Saves uploaded file to disk."""
        file_path = DATA_DIR / file.filename
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        self.current_file_path = file_path
        return file_path

    def convert_to_parquet(self, file_path: Path) -> Path:
        """Converts Excel to Parquet and returns the path. If already Parquet, just reads it."""
        
        if file_path.suffix == '.parquet':
            df = pd.read_parquet(file_path)
            # Ensure columns are normalized even for parquet inputs
            df.columns = [str(col).strip().upper() for col in df.columns]
            self.parquet_path = file_path
            self.df = df
            return file_path

        # Read Excel - optimize with engine='openpyxl'
        df = pd.read_excel(file_path, engine='openpyxl')
        
        # Normalize columns: uppercase and strip whitespace
        df.columns = [str(col).strip().upper() for col in df.columns]
        
        # Save as Parquet
        parquet_path = file_path.with_suffix('.parquet')
        df.to_parquet(parquet_path, index=False)
        
        self.parquet_path = parquet_path
        self.df = df
        return parquet_path
    
    def get_summary(self):
        if self.df is None:
            return None
        
        # Calculate distributions
        print("DEBUG: Columns in DF:", self.df.columns.tolist())
        
        # 1. Subject Status (Unique ID count + Grouping)
        subj_status_counts = {}
        if 'SUBJ_STATUS' in self.df.columns and 'SUBJ_ID' in self.df.columns:
            # Clean string data
            self.df['SUBJ_STATUS'] = self.df['SUBJ_STATUS'].astype(str).str.strip()
            
            # Drop duplicates to get unique subjects.
            unique_subjs = self.df.drop_duplicates(subset=['SUBJ_ID'])
            
            print("DEBUG: Unique Subject Statuses:", unique_subjs['SUBJ_STATUS'].unique())

            # Grouping Logic
            status_map = {
                'Screening': 'Enrolled',
                'Enrolled': 'Enrolled',
                'Drop Out': 'Study Off',
                'Study Off + Lock': 'Study Off',
                'Study Off': 'Study Off',
                'Death': 'Death',
                'Screening Failure': 'Screening Failure'
            }
            
            # Apply mapping
            mapped_status = unique_subjs['SUBJ_STATUS'].map(status_map).fillna(unique_subjs['SUBJ_STATUS'])
            
            # Define specific order
            target_order = ['Enrolled', 'Study Off', 'Death', 'Screening Failure']
            
            # Count and reindex to ensure order/zeros
            counts = mapped_status.value_counts()
            subj_status_counts = {k: int(counts.get(k, 0)) for k in target_order if k in target_order or k in counts.index}
            
            # Fill missing target keys with 0
            for k in target_order:
                if k not in subj_status_counts:
                    subj_status_counts[k] = 0

        # 2. Enrollment Type (Mapping)
        enroll_counts = {}
        if 'ENROLL_COPDNOTCOPD' in self.df.columns:
            print("DEBUG: Raw Enrollment Values:", self.df['ENROLL_COPDNOTCOPD'].unique())
            
            # Normalize to string '1', '2', '3', '4' to handle 1.0, 1, "1.0", "1"
            def normalize_enroll(val):
                try:
                    # Convert to float then int then str to handle "1.0" -> 1.0 -> 1 -> "1"
                    return str(int(float(val)))
                except:
                    return str(val)

            temp_series = self.df['ENROLL_COPDNOTCOPD'].apply(normalize_enroll)
            
            enroll_map = {
                '1': 'COPD',
                '2': 'Non-COPD',
                '3': 'COPD in young age',
                '4': 'PRISm'
            }
            
            mapped_enroll = temp_series.map(enroll_map)
            
            # Ensure order
            target_enroll_order = ['COPD', 'Non-COPD', 'COPD in young age', 'PRISm']
            counts = mapped_enroll.value_counts()
            enroll_counts = {k: int(counts.get(k, 0)) for k in target_enroll_order}

        # 3. Visit Distribution
        visit_counts = {}
        if 'VISIT_NM' in self.df.columns:
            # Clean string data
            self.df['VISIT_NM'] = self.df['VISIT_NM'].astype(str).str.strip()
            visit_counts = self.df['VISIT_NM'].value_counts().to_dict()

        return {
            "record_count": len(self.df),
            "columns": len(self.df.columns),
            "institutions": self.df['LOCATION_NAME'].nunique() if 'LOCATION_NAME' in self.df.columns else 0,
            "subjects": self.df['SUBJ_ID'].nunique() if 'SUBJ_ID' in self.df.columns else 0,
            "distributions": {
                "subj_status": subj_status_counts,
                "enroll_copd": enroll_counts,
                "visit_nm": visit_counts
            }
        }

    
    def get_data(self, page: int = 1, limit: int = 50, filters: dict = None):
        """Returns paginated data with optional filtering."""
        if self.df is None:
            return {"data": [], "total": 0, "page": page, "limit": limit}

        filtered_df = self.df.copy()

        # Apply filters
        if filters:
            for key, value in filters.items():
                if value and key in filtered_df.columns:
                    # Simple case-insensitive exact match for string columns
                    if filtered_df[key].dtype == 'object':
                        filtered_df = filtered_df[filtered_df[key].astype(str).str.contains(value, case=False, na=False)]
                    else:
                        filtered_df = filtered_df[filtered_df[key] == value]

        # Pagination
        total = len(filtered_df)
        start = (page - 1) * limit
        end = start + limit

        data = filtered_df.iloc[start:end].fillna("").to_dict(orient="records")

        return {
            "data": data,
            "total": total,
            "page": page,
            "limit": limit
        }

    def get_column_stats(self):
        """Returns statistics for all columns."""
        if self.df is None:
            return []
        
        stats = []
        for col in self.df.columns:
            total = len(self.df)
            missing = int(self.df[col].isna().sum())
            unique = int(self.df[col].nunique())
            dtype = str(self.df[col].dtype)
            
            # Top 3 values
            try:
                top_values = self.df[col].value_counts().head(3).to_dict()
                top_values_str = ", ".join([f"{k} ({v})" for k, v in top_values.items()])
            except:
                top_values_str = "-"

            stats.append({
                "column": col,
                "dtype": dtype,
                "missing": missing,
                "missing_pct": round((missing / total) * 100, 1),
                "unique": unique,
                "top_values": top_values_str
            })
            
        return stats

    def get_variable_stats(self, variable: str, filters: dict = None):
        """
        Returns detailed statistics and visualization data for a single variable.
        Supports filtering by Institution, Visit, etc.
        """
        if self.df is None or variable not in self.df.columns:
            return None

        # Apply filters
        df_filtered = self.df.copy()
        if filters:
            for key, value in filters.items():
                if value and key in df_filtered.columns and value != '전체':
                    if isinstance(value, list):
                        df_filtered = df_filtered[df_filtered[key].isin(value)]
                    else:
                        df_filtered = df_filtered[df_filtered[key] == value]

        # Basic Stats
        series = df_filtered[variable]
        total = len(series)
        missing = int(series.isna().sum())
        non_null = total - missing
        
        is_numeric = pd.api.types.is_numeric_dtype(series)
        
        stats = {
            "n": int(non_null),
            "missing": missing,
            "missing_pct": round((missing / total) * 100, 1) if total > 0 else 0,
        }

        # Distribution Data for Charts
        distribution = []
        
        if is_numeric:
            # Numeric Stats
            desc = series.describe()
            stats.update({
                "mean": round(desc['mean'], 2) if not pd.isna(desc['mean']) else None,
                "std": round(desc['std'], 2) if not pd.isna(desc['std']) else None,
                "min": desc['min'],
                "max": desc['max'],
                "q1": desc['25%'],
                "median": desc['50%'],
                "q3": desc['75%']
            })
            
            # Histogram (10 bins)
            if non_null > 0:
                try:
                    # Drop NA for histogram
                    valid_data = series.dropna()
                    # Calculate simple histogram
                    hist_values, bin_edges = pd.cut(valid_data, bins=10, retbins=True)
                    counts = hist_values.value_counts().sort_index()
                    
                    for i, (interval, count) in enumerate(counts.items()):
                        label = f"{interval.left:.1f} - {interval.right:.1f}"
                        distribution.append({"name": label, "value": int(count)})
                except:
                    pass

        else:
            # Categorical Stats
            value_counts = series.value_counts().head(20) # Limit to top 20
            for name, count in value_counts.items():
                distribution.append({"name": str(name), "value": int(count)})
                
            stats.update({
                "unique_count": series.nunique()
            })

        # Institution Comparison (Boxplot data equivalents)
        institution_stats = []
        if 'LOCATION_NAME' in df_filtered.columns:
            # Group by institution
            grouped = df_filtered.groupby('LOCATION_NAME')[variable]
            
            if is_numeric:
                for name, group in grouped:
                    valid_g = group.dropna()
                    if len(valid_g) > 0:
                        g_desc = valid_g.describe()
                        institution_stats.append({
                            "name": name,
                            "min": g_desc['min'],
                            "q1": g_desc['25%'],
                            "median": g_desc['50%'],
                            "q3": g_desc['75%'],
                            "max": g_desc['max'],
                            "count": int(g_desc['count'])
                        })
            else:
                # For categorical: maybe input rate per institution?
                pass
                
        return {
            "variable": variable,
            "is_numeric": is_numeric,
            "stats": stats,
            "distribution": distribution,
            "institution_stats": institution_stats
        }

    def get_filter_options(self):
        """Returns unique values for filters."""
        if self.df is None:
            return {}
        
        return {
            "institutions": sorted(self.df['LOCATION_NAME'].dropna().unique().tolist()) if 'LOCATION_NAME' in self.df.columns else [],
            "visits": sorted(self.df['VISIT_NM'].dropna().unique().tolist()) if 'VISIT_NM' in self.df.columns else [],
            # Domains could be inferred from column naming (e.g., if prefix based) or static list
            "domains": ["대상자관리", "임상정보", "설문", "검사항목", "약물"], 
            # Variable list - for now return all, usually filtered by domain in frontend if we had mapping
            "variables": sorted(self.df.columns.tolist())
        }

data_manager = DataManager()
