import os
from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import text

from database import engine, get_db
from models import Base, Person
from crud import create_or_update_person
from schemas import PersonCreate, PersonResponse

# 🔹 OCR router (HF API based)
from ocr.router import router as ocr_router


# -------------------------------------------------
# App init
# -------------------------------------------------
app = FastAPI(title="Janasena Backend API")

# ✅ NOW app exists → safe to include router
app.include_router(ocr_router)


# -------------------------------------------------
# DB init
# -------------------------------------------------
SCHEMA_NAME = "janasena"
with engine.connect() as connection:
    connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}"))
    connection.commit()

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
# PERSON APIs (UNCHANGED)
# -------------------------------------------------
@app.post("/person/submit", response_model=PersonResponse)
def submit_person(
    data: PersonCreate,
    db: Session = Depends(get_db)
):
    try:
        return create_or_update_person(db, data)
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
