"""
muavin_api.py — n8n Cloud için HTTP API Wrapper
================================================
Flask ile REST API. ngrok ile internete açılır, n8n cloud buraya çağırır.

Başlatma:
  python muavin_api.py

Varsayılan port: 5050

Endpoint'ler:
  GET  /health          — Sağlık kontrolü
  POST /convert-file    — n8n cloud: binary dosya yükle, binary çıktı al  ← ANA ENDPOINT
  POST /convert         — Lokal test: JSON ile dosya yolu gönder
"""

import sys
import uuid
import traceback
from pathlib import Path

from flask import Flask, request, jsonify, send_file

# muavin_pipeline aynı klasörde olmalı
sys.path.insert(0, str(Path(__file__).parent))
from muavin_pipeline import pipeline

# ─────────────────────────────────────────────
# AYARLAR
# ─────────────────────────────────────────────
import os
HOST       = "0.0.0.0"
PORT       = int(os.environ.get("PORT", 5050))   # Render PORT env'i otomatik atar

# Şablon: önce env'den bak, yoksa script yanındaki dosyayı kullan
_BASE      = Path(__file__).parent
SABLON_DEFAULT = os.environ.get(
    "SABLON_PATH",
    str(_BASE / "Örnek Sablonlar.xlsx")
)

TMP_KLASOR = Path(os.environ.get("TMP_DIR", str(_BASE / "tmp_api")))
TMP_KLASOR.mkdir(parents=True, exist_ok=True)

app = Flask(__name__)


# ─────────────────────────────────────────────
# ENDPOINT: GET /health
# ─────────────────────────────────────────────
@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "service": "muavin-pipeline"}), 200


# ─────────────────────────────────────────────
# ENDPOINT: POST /convert-file          ← n8n CLOUD ANA ENDPOINT
#
# n8n bu endpoint'e:
#   - multipart/form-data olarak "file" alanında xlsx binary'sini gönderir
#   - Opsiyonel "filename" form alanı (Drive'dan gelen orijinal isim)
#
# Yanıt: işlenmiş .xlsx dosyasını binary olarak döner
#   → n8n bunu doğrudan Google Drive'a upload eder
#
# Header olarak da özet meta gelir:
#   X-Muavin-Format, X-Muavin-RowsOut, X-Muavin-Accounts
# ─────────────────────────────────────────────
@app.route("/convert-file", methods=["POST"])
def convert_file():
    if "file" not in request.files:
        return jsonify({"status": "error", "error": "Form'da 'file' alanı eksik"}), 400

    f         = request.files["file"]
    orig_name = request.form.get("filename") or f.filename or "muavin.xlsx"
    stem      = Path(orig_name).stem
    run_id    = uuid.uuid4().hex[:8]

    tmp_in  = TMP_KLASOR / f"{run_id}_{orig_name}"
    tmp_out = TMP_KLASOR / f"{run_id}_{stem}_Standart.xlsx"

    try:
        f.save(tmp_in)
        sonuc = pipeline(str(tmp_in), str(tmp_out), SABLON_DEFAULT)

        if sonuc["status"] != "success":
            return jsonify(sonuc), 500

        response = send_file(
            tmp_out,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            as_attachment=True,
            download_name=f"{stem}_Standart.xlsx",
        )
        # Meta bilgileri header'a ekle — n8n downstream node'larında kullanılabilir
        response.headers["X-Muavin-Format"]   = sonuc.get("format", "")
        response.headers["X-Muavin-RowsOut"]  = str(sonuc.get("rows_out", ""))
        response.headers["X-Muavin-Accounts"] = str(sonuc.get("account_count", ""))
        response.headers["X-Muavin-Borc"]     = str(sonuc.get("toplam_borc", ""))
        response.headers["X-Muavin-Alacak"]   = str(sonuc.get("toplam_alacak", ""))
        return response

    except Exception as e:
        return jsonify({"status": "error", "error": str(e),
                        "traceback": traceback.format_exc()}), 500
    finally:
        tmp_in.unlink(missing_ok=True)
        # tmp_out send_file sonrası temizle (after_request ile)


# ─────────────────────────────────────────────
# ENDPOINT: POST /convert          ← LOKAL TEST
# JSON body: { "input_path", "output_path"(opt), "sablon_path"(opt) }
# ─────────────────────────────────────────────
@app.route("/convert", methods=["POST"])
def convert():
    body        = request.get_json(force=True, silent=True) or {}
    input_path  = body.get("input_path", "")
    if not input_path:
        return jsonify({"status": "error", "error": "input_path zorunlu"}), 400

    inp         = Path(input_path)
    output_path = body.get("output_path") or str(TMP_KLASOR / (inp.stem + "_Standart.xlsx"))
    sablon_path = body.get("sablon_path") or SABLON_DEFAULT

    try:
        return jsonify(pipeline(input_path, output_path, sablon_path)), 200
    except Exception as e:
        return jsonify({"status": "error", "error": str(e),
                        "traceback": traceback.format_exc()}), 500


# ─────────────────────────────────────────────
# BAŞLATMA
# ─────────────────────────────────────────────
if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    print(f"Muavin API baslatiliyor: http://{HOST}:{PORT}")
    print(f"  /health        GET")
    print(f"  /convert-file  POST  multipart (n8n cloud)")
    print(f"  /convert       POST  JSON       (lokal test)")
app.run(host=HOST, port=int(os.environ.get("PORT", PORT)), debug=False)
