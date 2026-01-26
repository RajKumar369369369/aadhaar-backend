from paddleocr import PaddleOCR

print("Downloading PaddleOCR model...")
ocr = PaddleOCR(lang="en", use_angle_cls=True, use_textline_orientation=True)
print("Model downloaded and ready!")
