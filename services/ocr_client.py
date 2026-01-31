# services/ocr_client.py
import requests
from typing import Optional

HF_OCR_URL = "https://Vazeed970-aadhaar-ocr-api.hf.space/aadhaar-ocr"
# Increase timeout if needed
REQUEST_TIMEOUT = 60


def call_aadhaar_ocr_from_bytes(file_bytes: bytes, filename: str) -> dict:
    files = {"file": (filename or "upload.jpg", file_bytes, "image/jpeg")}
    resp = requests.post(HF_OCR_URL, files=files, timeout=REQUEST_TIMEOUT)
    if resp.status_code != 200:
        raise RuntimeError(f"OCR service failed ({resp.status_code}): {resp.text}")
    return resp.json()


def download_image_to_bytes(url: str) -> bytes:
    resp = requests.get(url, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.content


def call_aadhaar_ocr(image_bytes: Optional[bytes] = None, filename: Optional[str] = None, image_url: Optional[str] = None) -> dict:
    """
    Convenience wrapper:
    - if image_bytes provided => use it
    - else if image_url provided => download and use it
    - else raise
    """
    if image_bytes:
        return call_aadhaar_ocr_from_bytes(image_bytes, filename or "upload.jpg")
    if image_url:
        b = download_image_to_bytes(image_url)
        return call_aadhaar_ocr_from_bytes(b, filename or "upload.jpg")
    raise ValueError("Provide image_bytes or image_url")
