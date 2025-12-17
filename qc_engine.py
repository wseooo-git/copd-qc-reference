import pandas as pd
from typing import List, Dict, Any

class QCEngine:
    def __init__(self):
        self.rules: List[Dict] = [
            # Sample Rules based on spec
            {
                "id": "SUBJ-MISS-001",
                "domain": "subject",
                "type": "MISS",
                "severity": "Critical",
                "variable": "SUBJ_ID",
                "description": "Subject ID must be present",
                "check": lambda df: df['SUBJ_ID'].isna()
            },
            {
                "id": "SUBJ-MISS-002",
                "domain": "subject",
                "type": "MISS",
                "severity": "Critical",
                "variable": "LOCATION_NAME",
                "description": "Location Name must be present",
                "check": lambda df: df['LOCATION_NAME'].isna()
            },
            # Add more rules as needed (dynamic or static)
            {
                "id": "AGE-RANGE-001",
                "domain": "subject",
                "type": "RANGE",
                "severity": "Medium",
                "variable": "AGE",
                "description": "Age should be between 40 and 100",
                "check": lambda df: (df['AGE'] < 40) | (df['AGE'] > 100) if 'AGE' in df.columns else pd.Series(False, index=df.index)
            },
            {
                "id": "ENROLL-VAL-001",
                "domain": "function",
                "type": "VALUE",
                "severity": "High",
                "variable": "ENROLL_COPDNOTCOPD",
                "description": "Enrollment status must be 1, 2, or 3",
                "check": lambda df: (~df['ENROLL_COPDNOTCOPD'].isin([1, 2, 3])) & (df['ENROLL_COPDNOTCOPD'].notna()) if 'ENROLL_COPDNOTCOPD' in df.columns else pd.Series(False, index=df.index)
            }
        ]

    def run_qc(self, df: pd.DataFrame) -> pd.DataFrame:
        results = []
        
        for rule in self.rules:
            # Execute check
            try:
                # The check function returns a boolean Series (True = Error)
                error_mask = rule["check"](df)
                
                if error_mask.any():
                    error_rows = df[error_mask].copy()
                    
                    for idx, row in error_rows.iterrows():
                        results.append({
                            "LOCATION_NAME": row.get('LOCATION_NAME', 'Unknown'),
                            "SUBJ_ID": row.get('SUBJ_ID', 'Unknown'),
                            "VISIT_NM": row.get('VISIT_NM', 'Unknown'),
                            "rule_id": rule["id"],
                            "domain": rule["domain"],
                            "rule_type": rule["type"],
                            "severity": rule["severity"],
                            "variable": rule["variable"],
                            "current_value": str(row.get(rule["variable"], 'Missing')),
                            "rule_description": rule["description"]
                        })
            except Exception as e:
                print(f"Error executing rule {rule['id']}: {e}")

        return pd.DataFrame(results)

    def get_stats(self, qc_df: pd.DataFrame):
        if qc_df.empty:
            return {
                "total_errors": 0,
                "error_rate": 0,
                "severity_counts": {},
                "domain_counts": {}
            }
        
        return {
            "total_errors": len(qc_df),
            "severity_counts": qc_df['severity'].value_counts().to_dict(),
            "domain_counts": qc_df['domain'].value_counts().to_dict()
        }

qc_engine = QCEngine()
