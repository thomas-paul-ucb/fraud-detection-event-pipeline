# Real-Time Fraud Detection Pipeline

A high-throughput distributed system designed to process financial transactions and identify fraudulent activity in real-time.

## Current Architecture (Phase 1)
- **Ingestion Tier**: FastAPI (Asynchronous)
- **Data Validation**: Pydantic
- **Traffic Simulation**: Custom Python Load Generator

## Setup
1. `pip install -r requirements.txt`
2. `uvicorn main:app --reload`
3. In a new terminal: `python stress_test.py`