from src.insert import run_insert_all
from src.analysis import AnalysisExporter

if __name__ == "__main__":
    run_insert_all()

    exporter = AnalysisExporter()
    exporter.run_all()
