from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from data_manager import data_manager

app = FastAPI(title="COPD QC System API")

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],  # Vite default port
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def read_root():
    return {"message": "Welcome to COPD QC System API"}

@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    if not file.filename.endswith(('.xlsx', '.xls', '.parquet')):
        raise HTTPException(status_code=400, detail="Invalid file format. Please upload Excel (.xlsx) or Parquet (.parquet) file.")
    
    try:
        # Save file
        file_path = await data_manager.save_upload(file)
        
        # Convert processing
        parquet_path = data_manager.convert_to_parquet(file_path)
        
        # Run QC
        from qc_engine import qc_engine
        if data_manager.df is not None:
             qc_results = qc_engine.run_qc(data_manager.df)
             qc_stats = qc_engine.get_stats(qc_results)
        else:
             qc_stats = {}

        summary = data_manager.get_summary()
        summary.update(qc_stats)
        
        return {
            "filename": file.filename,
            "status": "success",
            "summary": summary
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/data")
def get_data(page: int = 1, limit: int = 50, search: str = None):
    try:
        filters = {}
        # Simple search implementation: check basic columns if search query exists
        # For now, we'll just pass search as a 'SUBJ_ID' filter if provided, or could handle generic search later
        if search:
            # Check if search looks like an institution or subject?
            # For simplicity let's just use it as global search across key columns or specific SUBJ_ID filter
            filters['SUBJ_ID'] = search 
            
        return data_manager.get_data(page=page, limit=limit, filters=filters)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/data/filters")
def get_filters():
    try:
        return data_manager.get_filter_options()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/data/variable")
def get_variable_stats(variable: str, institution: str = None, visit: str = None):
    try:
        filters = {}
        if institution and institution != '전체':
            filters['LOCATION_NAME'] = institution
        if visit and visit != '전체':
            filters['VISIT_NM'] = visit
            
        return data_manager.get_variable_stats(variable, filters)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
def health_check():
    return {"status": "healthy"}
