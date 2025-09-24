PY ?= python3

.PHONY: help install run run-all validate all clean

help:
	@echo "Targets:"
	@echo "  install    Install requirements via pip"
	@echo "  run        Run full pipeline"
	@echo "  run-all    Same as run"
	@echo "  clean      Remove generated CSV outputs"

install:
	$(PY) -m pip install -r requirements.txt

run:
	$(PY) run_pipeline.py

run-all: run

validate:
	$(PY) validation_report.py --out reports

all:
	$(PY) main.py all --out reports

clean:
	rm -rf converted_data combined_data cleaned_data dimensions facts
	mkdir -p converted_data/Occupancy converted_data/Deskcount \
		combined_data cleaned_data dimensions facts
