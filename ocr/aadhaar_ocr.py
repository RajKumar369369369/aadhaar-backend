import cv2
import re
from datetime import datetime
from paddleocr import PaddleOCR

# ---------------- OCR INITIALIZATION ----------------
ocr = PaddleOCR(
    lang="en",
    # keep minimal for backend stability
)

# ---------------- PREPROCESS IMAGE ----------------
def preprocess_image(img_path):
    img = cv2.imread(img_path)
    img = cv2.resize(img, None, fx=2.5, fy=2.5, interpolation=cv2.INTER_CUBIC)
    return img

# ---------------- TEXT EXTRACTION ----------------
def extract_text(img_path):
    img = preprocess_image(img_path)
    result = ocr.ocr(img, cls=True)

    lines = []
    for block in result:
        for line in block:
            lines.append(line[1][0])

    return "\n".join(lines)

# ---------------- NAME EXTRACTION ----------------
def extract_name(text):
    lines = [line.strip() for line in text.splitlines() if line.strip()]

    for i, line in enumerate(lines):
        if line.lower() == "to":
            if i + 2 < len(lines):
                return lines[i + 2]

    return ""

# ---------------- DATE REGEX ----------------
DATE_REGEX = re.compile(
    r'(0?[1-9]|[12][0-9]|3[01])\s*[/-]\s*'
    r'(0?[1-9]|1[0-2])\s*[/-]\s*'
    r'(19\d{2}|20\d{2})'
)

# ---------------- NORMALIZE OCR TEXT ----------------
def normalize_text(text):
    text = text.replace('\n', ' ')
    text = re.sub(r'\s+', ' ', text)
    return text

# ---------------- DOB EXTRACTION ----------------
def extract_dob(text):
    text = normalize_text(text)
    dates = DATE_REGEX.findall(text)

    if not dates:
        return ""

    # Aadhaar usually has DOB second
    return "/".join(dates[-1])

# ---------------- GENDER ----------------
def extract_gender(text):
    text = text.upper()
    if "FEMALE" in text:
        return "FEMALE"
    if "MALE" in text:
        return "MALE"
    return ""

# ---------------- IMPROVED AADHAAR EXTRACTION ----------------
def extract_aadhaar_number(text):
    clean_text = normalize_text(text)

    # 1️⃣ Aadhaar near keyword (highest confidence)
    keyword_pattern = re.compile(
        r'(aadhaar|your aadhaar)[^\d]{0,20}((?:[2-9]\d{3}\s?\d{4}\s?\d{4}))',
        re.IGNORECASE
    )

    match = keyword_pattern.search(clean_text)
    if match:
        num = re.sub(r'\s', '', match.group(2))
        return f"{num[:4]} {num[4:8]} {num[8:]}"

    # 2️⃣ General 12-digit Aadhaar (exclude VID)
    candidates = re.findall(r'\b[2-9]\d{11}\b', clean_text)
    for num in candidates:
        context = clean_text[max(0, clean_text.find(num)-5): clean_text.find(num)+len(num)+5]
        if re.search(r'\d{16}', context):
            continue
        return f"{num[:4]} {num[4:8]} {num[8:]}"

    # 3️⃣ Grouped fallback
    grouped = re.search(r'\b[2-9]\d{3}\s\d{4}\s\d{4}\b', clean_text)
    if grouped:
        return grouped.group()

    return ""

# ---------------- IMPROVED MOBILE EXTRACTION ----------------
def extract_mobile_number(text):
    clean_text = normalize_text(text)

    keyword_match = re.search(
        r'(mobile|moblle|moblie)[^\d]{0,10}([6-9]\d{9})',
        clean_text,
        re.IGNORECASE
    )

    if keyword_match:
        return keyword_match.group(2)

    match = re.search(r'\b[6-9]\d{9}\b', clean_text)
    if match:
        return match.group()

    return ""

# ---------------- FIELD EXTRACTION ----------------
def extract_fields(text):
    data = {
        "Name": "",
        "Adhaar_Number": "",
        "GENDER": "",
        "DOB": "",
        "Mobile": "",
        "Pincode": ""
    }

    data["Name"] = extract_name(text)
    data["Adhaar_Number"] = extract_aadhaar_number(text)
    data["GENDER"] = extract_gender(text)
    data["DOB"] = extract_dob(text)
    data["Mobile"] = extract_mobile_number(text)

    pin_match = re.search(r'\b\d{6}\b', text)
    if pin_match:
        data["Pincode"] = pin_match.group()

    return data

# ---------------- NORMALIZE AADHAAR ----------------
def normalize_aadhaar(aadhaar_str: str) -> str:
    if not aadhaar_str:
        return ""
    return re.sub(r"\D", "", aadhaar_str)

# ---------------- BACKEND SAFE ENTRY FUNCTION ----------------
def run_aadhaar_ocr(image_path: str) -> dict:
    text = extract_text(image_path)
    fields = extract_fields(text)

    return {
        "aadhaar_number": normalize_aadhaar(fields.get("Adhaar_Number", "")),
        "full_name": fields.get("Name", "") or "",
        "gender": (fields.get("GENDER", "") or "").capitalize(),
        "dob": fields.get("DOB", "") or "",
        "mobile_number": fields.get("Mobile", "") or "",
        "pincode": fields.get("Pincode", "") or ""
    }

# ---------------- LOCAL TEST ----------------
if __name__ == "__main__":
    path = r"D:\PycharmProjects\pythonProject\Narendra Mama\Adhaar\Adhaar\Adhaar_001.jpg"

    text = extract_text(path)
    print("\n===== OCR TEXT =====\n", text)

    data = run_aadhaar_ocr(path)
    print("\n===== FINAL DATA =====\n")
    for k, v in data.items():
        print(f"{k}: {v}")
