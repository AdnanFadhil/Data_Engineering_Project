from src.insert import run_insert_all
from src.analysis import AnalysisExporter

if __name__ == "__main__":
    # Jalankan insert data
    run_insert_all()
    
    # Jalankan export SQL → CSV + table
    exporter = AnalysisExporter()
    exporter.run_all()
