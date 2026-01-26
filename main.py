import os
import requests
import tempfile

from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import text

from database import engine, get_db
from models import Base, Person
from crud import create_or_update_person, get_by_aadhaar
from schemas import (
    PersonCreate,
    PersonResponse,
    AadhaarOCRRequest,
    AadhaarOCRResponse
)

from paddleocr import PaddleOCR
import numpy as np
import cv2

ocr = None  # Global OCR variable

# -------------------------------------------------
# App init
# -------------------------------------------------

app = FastAPI(title="Janasena Backend API")

# PaddleOCR initialization on startup
@app.on_event("startup")
def startup_event():
    global ocr
    print("Initializing PaddleOCR...")
    ocr = PaddleOCR(lang="en")#, use_angle_cls=True, use_textline_orientation=True
    print("PaddleOCR ready!")

# Ensure schema exists before creating tables
SCHEMA_NAME = "janasena"
with engine.connect() as connection:
    connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}"))
    connection.commit()

# Create tables
Base.metadata.create_all(bind=engine)

# -------------------------------------------------
# CORS
# -------------------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -------------------------------------------------
# PERSON APIs
# -------------------------------------------------
@app.post("/person/submit", response_model=PersonResponse)
def submit_person(
    data: PersonCreate,
    db: Session = Depends(get_db)
):
    """
    Receives JSON payload from frontend.
    Images must already be uploaded to Cloudinary.
    Stores text fields + image URLs in PostgreSQL.
    """
    try:
        person = create_or_update_person(db, data)
        return person

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/person/by-aadhaar/{aadhaar_number}", response_model=PersonResponse)
def get_person_by_aadhaar(
    aadhaar_number: str,
    db: Session = Depends(get_db)
):
    person = db.query(Person).filter(
        Person.aadhaar_number == aadhaar_number
    ).first()

    if not person:
        raise HTTPException(status_code=404, detail="Person not found")

    return person



# New OCR endpoint using global ocr object
@app.post("/ocr/aadhaar")
async def ocr_aadhaar(payload: AadhaarOCRRequest):
    try:
        resp = requests.get(payload.image_url)
        img_array = np.frombuffer(resp.content, np.uint8)
        img = cv2.imdecode(img_array, cv2.IMREAD_COLOR)
        result = ocr.ocr(img)
        text_output = [line[1][0] for line in sum(result, [])]
        return {"text": text_output}
    except Exception as e:
        return {"error": str(e)}
