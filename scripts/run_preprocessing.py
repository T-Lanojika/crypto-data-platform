import os
import sys

if __package__ in (None, ""):
	project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
	if project_root not in sys.path:
		sys.path.insert(0, project_root)

from app.pipeline.preprocessing_pipeline import PreprocessingPipeline


def main() -> None:
	pipeline = PreprocessingPipeline()
	results = pipeline.run_time_series_audit()

	issue_results = [item for item in results if item.status == "ISSUES"]
	empty_results = [item for item in results if item.status in {"EMPTY", "MISSING_TABLE"}]

	print("\n[PREPROCESS] Final Result", flush=True)
	if not issue_results and not empty_results:
		print("[PREPROCESS] Data is fully correct time-series with no detected missing intervals.", flush=True)
	else:
		print(
			"[PREPROCESS] Data has time-series issues or missing tables. "
			"Review statuses above and run collector sync if needed.",
			flush=True,
		)


if __name__ == "__main__":
	main()
