# ocr/router.py
from fastapi import APIRouter, UploadFile, File, HTTPException, Query, Depends
from sqlalchemy.orm import Session
from typing import Optional
from services.ocr_client import call_aadhaar_ocr
from crud import create_or_update_person
from database import get_db
from schemas import PersonCreate  # your existing pydantic schema

router = APIRouter(prefix="/ocr", tags=["OCR"])


@router.post("/aadhaar")
async def aadhaar_ocr(
    file: Optional[UploadFile] = File(None),
    image_url: Optional[str] = Query(None),
    save: bool = Query(False, description="If true, save OCR result into DB"),
    db: Session = Depends(get_db)
):
    """
    Accepts either:
      - multipart file (form field "file")
      - image_url (query param)
    Optionally: save=true to persist into DB using your existing create_or_update_person.

    Returns the OCR JSON from the HF OCR service. If save=true, returns DB person after save.
    """
    try:
        # 1) get bytes (prefer file if provided)
        if file is not None:
            file_bytes = await file.read()
            ocr_json = call_aadhaar_ocr(image_bytes=file_bytes, filename=file.filename)
        elif image_url:
            ocr_json = call_aadhaar_ocr(image_url=image_url)
        else:
            raise HTTPException(status_code=400, detail="Provide file or image_url")

        # ocr_json expected to contain keys like:
        # { "aadhaar_number": "...", "full_name": "...", "gender": "...", "dob": "...", "mobile_number": "...", "pincode": "..." }

        if not save:
            return ocr_json

        # 2) Save to DB (optional). Build PersonCreate from ocr_json.
        # Map HF keys -> PersonCreate fields. Adjust keys if your schema differs.
        payload = {
            "aadhaar_number": ocr_json.get("aadhaar_number") or "",
            "full_name": ocr_json.get("full_name") or "",
            "gender": ocr_json.get("gender") or "",
            "dob": ocr_json.get("dob") or None,            # schema may accept ISO date string or None
            "mobile_number": ocr_json.get("mobile_number") or "",
            "pincode": ocr_json.get("pincode") or "",
            # optional fields you may want to set:
            "aadhaar_image_url": image_url or (getattr(file, "filename", None) or ""),
            # photo_url, constituency, etc. can be left out or added if available
        }

        # Validate and create PersonCreate pydantic object
        try:
            person_data = PersonCreate(**payload)
        except Exception as e:
            # Validation failed: return OCR result but report that saving failed
            return {
                "ocr_result": ocr_json,
                "save_error": f"Failed to validate PersonCreate: {str(e)}"
            }

        # create_or_update_person will create or update existing record
        person = create_or_update_person(db, person_data)
        return {"ocr_result": ocr_json, "person": person}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
